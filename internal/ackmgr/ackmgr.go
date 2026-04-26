package ackmgr

import (
	"sync"
	"time"

	"github.com/harsh3dev/messager/internal/connmgr"
	"github.com/harsh3dev/messager/internal/core"
	"github.com/harsh3dev/messager/internal/queue"
)

type inFlightEntry struct {
	message    core.Message
	consumerID string
}

// AckManager tracks in-flight messages and handles ACK/NACK outcomes.
type AckManager struct {
	mu         sync.Mutex
	inFlight   map[core.MessageID]inFlightEntry
	manager    *queue.Manager
	registry   *connmgr.Registry
	maxRetries int32
}

func NewAckManager(manager *queue.Manager, registry *connmgr.Registry, maxRetries int32) *AckManager {
	return &AckManager{
		inFlight:   make(map[core.MessageID]inFlightEntry),
		manager:    manager,
		registry:   registry,
		maxRetries: maxRetries,
	}
}

// Register records a dispatched message as in-flight.
// Called by the Dispatcher immediately after pushing the message to consumer.Send.
func (a *AckManager) Register(msg core.Message, consumerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight[msg.ID] = inFlightEntry{message: msg, consumerID: consumerID}
}

// Ack removes the message from the in-flight map, decrements the consumer's in-flight
// counter, and writes an ACK tombstone to the WAL so the message is not redelivered on restart.
func (a *AckManager) Ack(id core.MessageID) error {
	a.mu.Lock()
	entry, ok := a.inFlight[id]
	if !ok {
		a.mu.Unlock()
		return nil
	}
	delete(a.inFlight, id)
	a.mu.Unlock()

	a.registry.DecrementInFlight(entry.consumerID)
	return a.manager.WriteTombstone(id, entry.message.Queue)
}

// Nack removes the message from the in-flight map, decrements the consumer's in-flight
// counter, and either re-enqueues the message (retries remaining) or moves it to the DLQ
// (retries exhausted).
func (a *AckManager) Nack(id core.MessageID) error {
	a.mu.Lock()
	entry, ok := a.inFlight[id]
	if !ok {
		a.mu.Unlock()
		return nil
	}
	delete(a.inFlight, id)
	a.mu.Unlock()

	return a.nackEntry(entry)
}

// ScanAndTimeout iterates the in-flight map, removes entries whose DispatchedAt is older
// than dispatchTimeout, and treats each as an implicit NACK (requeue or DLQ).
// Called periodically by the retry.Scanner.
func (a *AckManager) ScanAndTimeout(dispatchTimeout time.Duration) error {
	threshold := time.Now().Add(-dispatchTimeout)

	a.mu.Lock()
	var timedOut []inFlightEntry
	for id, entry := range a.inFlight {
		if entry.message.DispatchedAt.Before(threshold) {
			timedOut = append(timedOut, entry)
			delete(a.inFlight, id)
		}
	}
	a.mu.Unlock()

	var firstErr error
	for _, entry := range timedOut {
		if err := a.nackEntry(entry); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// nackEntry decrements the consumer's in-flight counter, then either re-enqueues
// the message with an incremented retry count or sends it to the DLQ if retries
// are exhausted.
func (a *AckManager) nackEntry(entry inFlightEntry) error {
	a.registry.DecrementInFlight(entry.consumerID)

	if entry.message.RetryCount >= a.maxRetries {
		dlqMsg := entry.message
		dlqMsg.Queue = entry.message.Queue + ".dlq"
		dlqMsg.Status = core.StatusDead
		return a.manager.Enqueue(dlqMsg)
	}
	return a.manager.Enqueue(entry.message.WithRetry())
}
