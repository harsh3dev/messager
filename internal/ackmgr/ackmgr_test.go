package ackmgr_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/harsh3dev/messager/internal/ackmgr"
	"github.com/harsh3dev/messager/internal/connmgr"
	"github.com/harsh3dev/messager/internal/core"
	"github.com/harsh3dev/messager/internal/queue"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func newMsg(id string) core.Message {
	return core.Message{ID: core.MessageID(id), Queue: "orders", Payload: []byte(id), EnqueueTime: time.Now()}
}

func TestAck_RemovesFromWALAndDecrementsInFlight(t *testing.T) {
	walDir := t.TempDir()

	manager, err := queue.NewManager(walDir, discard())
	if err != nil {
		t.Fatal(err)
	}
	registry := connmgr.NewRegistry()
	consumer, _ := connmgr.NewConsumer("orders", 5)
	registry.Register(consumer)
	registry.IncrementInFlight(consumer.ID) // simulate dispatcher increment
	ackMgr := ackmgr.NewAckManager(manager, registry, 5, discard())

	msg := newMsg("msg-1")
	if err := manager.Enqueue(msg); err != nil {
		t.Fatal(err)
	}
	ackMgr.Register(msg, consumer.ID)

	if err := ackMgr.Ack(msg.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if consumer.InFlight() != 0 {
		t.Fatalf("want inFlight=0 after Ack, got %d", consumer.InFlight())
	}

	// Close and reopen with the same WAL dir — tombstone must prevent replay.
	manager.Close()
	manager2, err := queue.NewManager(walDir, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer manager2.Close()

	got := make(chan core.Message, 1)
	go func() {
		msg, ok := manager2.Dequeue("orders")
		if ok {
			got <- msg
		}
	}()

	select {
	case <-got:
		t.Fatal("ACKed message must not reappear after WAL replay")
	case <-time.After(50 * time.Millisecond):
		// correct: tombstone prevents replay
	}
}

func TestNack_RequeuesWithIncrementedRetryCount(t *testing.T) {
	manager, err := queue.NewManager(t.TempDir(), discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.Close() })

	registry := connmgr.NewRegistry()
	consumer, _ := connmgr.NewConsumer("orders", 5)
	registry.Register(consumer)
	registry.IncrementInFlight(consumer.ID)
	ackMgr := ackmgr.NewAckManager(manager, registry, 5, discard())

	msg := newMsg("msg-1")
	if err := manager.Enqueue(msg); err != nil {
		t.Fatal(err)
	}

	// Dequeue to simulate the dispatcher having removed the message from the queue.
	dequeued := make(chan core.Message, 1)
	go func() {
		m, ok := manager.Dequeue("orders")
		if ok {
			dequeued <- m
		}
	}()

	var dispatched core.Message
	select {
	case dispatched = <-dequeued:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dequeue")
	}

	ackMgr.Register(dispatched, consumer.ID)

	if err := ackMgr.Nack(dispatched.ID); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	if consumer.InFlight() != 0 {
		t.Fatalf("want inFlight=0 after Nack, got %d", consumer.InFlight())
	}

	// Message must reappear in the queue with RetryCount incremented to 1.
	requeued := make(chan core.Message, 1)
	go func() {
		m, ok := manager.Dequeue("orders")
		if ok {
			requeued <- m
		}
	}()

	select {
	case m := <-requeued:
		if m.RetryCount != 1 {
			t.Fatalf("want RetryCount=1, got %d", m.RetryCount)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for requeued message")
	}
}

func TestAckNack_IdempotentForUnknownID(t *testing.T) {
	manager, _ := queue.NewManager(t.TempDir(), discard())
	t.Cleanup(func() { manager.Close() })
	registry := connmgr.NewRegistry()
	ackMgr := ackmgr.NewAckManager(manager, registry, 5, discard())

	if err := ackMgr.Ack("nonexistent"); err != nil {
		t.Fatalf("Ack with unknown ID returned error: %v", err)
	}
	if err := ackMgr.Nack("nonexistent"); err != nil {
		t.Fatalf("Nack with unknown ID returned error: %v", err)
	}
}
