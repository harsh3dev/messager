package queue

import (
	"testing"
	"time"

	"github.com/harsh3dev/messager/internal/core"
)

func newMsg(id, queue string) core.Message {
	return core.Message{
		ID:          core.MessageID(id),
		Queue:       queue,
		Payload:     []byte(id),
		EnqueueTime: time.Now(),
	}
}

func TestManager_EnqueueDequeue(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if err := m.Enqueue(newMsg("1", "orders")); err != nil {
		t.Fatal(err)
	}

	got, ok := m.Dequeue("orders")
	if !ok {
		t.Fatal("unexpected close")
	}
	if string(got.ID) != "1" {
		t.Fatalf("want id=1, got %s", got.ID)
	}
}

func TestManager_RestoreOnRestart(t *testing.T) {
	dir := t.TempDir()

	// first run: enqueue two messages, ack one
	func() {
		m, err := NewManager(dir)
		if err != nil {
			t.Fatal(err)
		}
		m.Enqueue(newMsg("a", "orders"))
		m.Enqueue(newMsg("b", "orders"))
		m.WriteTombstone("b", "orders")
		m.Close()
	}()

	// second run: only "a" should survive
	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	got, ok := m2.Dequeue("orders")
	if !ok || string(got.ID) != "a" {
		t.Fatalf("want id=a, got %v (ok=%v)", got.ID, ok)
	}
	if m2.topics["orders"].Len() != 0 {
		t.Fatal("expected only one message to survive replay")
	}
}

func TestManager_WALFailureDoesNotEnqueue(t *testing.T) {
	// Use a non-existent directory that can't be created (file in the way)
	// to trigger a WAL writer init failure.
	// Easiest: init manager with a good dir, then corrupt the writer by
	// replacing the WAL dir with a file.
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// prime the queue so initQueue has been called
	m.Enqueue(newMsg("1", "q"))

	// now point walDir at a file so any future writer open for a new queue fails
	m.walDir = dir + "/not-a-dir.wal" // initQueue for a new queue will fail MkdirAll

	err = m.Enqueue(newMsg("x", "newqueue"))
	if err == nil {
		t.Fatal("expected error when WAL dir is invalid")
	}
	// "newqueue" topic must not exist
	m.mu.Lock()
	_, exists := m.topics["newqueue"]
	m.mu.Unlock()
	if exists {
		t.Fatal("topic must not be created when WAL init fails")
	}
}

func TestManager_EnqueueTimestampPreserved(t *testing.T) {
	dir := t.TempDir()
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	func() {
		m, _ := NewManager(dir)
		msg := newMsg("1", "q")
		msg.EnqueueTime = ts
		m.Enqueue(msg)
		m.Close()
	}()

	m2, _ := NewManager(dir)
	defer m2.Close()
	got, _ := m2.Dequeue("q")

	if !got.EnqueueTime.Equal(ts) {
		t.Fatalf("want %v, got %v", ts, got.EnqueueTime)
	}
}
