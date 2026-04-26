package ackmgr

import (
	"sync"

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
	mu       sync.Mutex
	inFlight map[core.MessageID]inFlightEntry
	manager  *queue.Manager
	registry *connmgr.Registry
}

func NewAckManager(manager *queue.Manager, registry *connmgr.Registry) *AckManager {
	return &AckManager{
		inFlight: make(map[core.MessageID]inFlightEntry),
		manager:  manager,
		registry: registry,
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
// counter, and re-enqueues the message with an incremented retry count.
func (a *AckManager) Nack(id core.MessageID) error {
	a.mu.Lock()
	entry, ok := a.inFlight[id]
	if !ok {
		a.mu.Unlock()
		return nil
	}
	delete(a.inFlight, id)
	a.mu.Unlock()

	a.registry.DecrementInFlight(entry.consumerID)
	return a.manager.Enqueue(entry.message.WithRetry())
}
