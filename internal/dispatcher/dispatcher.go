package dispatcher

import (
	"context"
	"sort"
	"time"

	"github.com/harsh3dev/messager/internal/ackmgr"
	"github.com/harsh3dev/messager/internal/connmgr"
	"github.com/harsh3dev/messager/internal/core"
	"github.com/harsh3dev/messager/internal/queue"
)

const retryInterval = 5 * time.Millisecond

type Dispatcher struct {
	queueManager *queue.Manager
	registry     *connmgr.Registry
	ackManager   *ackmgr.AckManager
}

func NewDispatcher(queueManager *queue.Manager, registry *connmgr.Registry, ackManager *ackmgr.AckManager) *Dispatcher {
	return &Dispatcher{
		queueManager: queueManager,
		registry:     registry,
		ackManager:   ackManager,
	}
}

// Run starts the dispatch loop for a single queue. Blocks until ctx is cancelled or the queue manager closes.
func (d *Dispatcher) Run(ctx context.Context, queueName string) {
	for {
		msg, ok := d.queueManager.Dequeue(queueName)
		if !ok {
			return
		}

		// Hold the message and retry until an eligible consumer is available.
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			eligible := d.registry.EligibleConsumers(queueName)
			if len(eligible) == 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(retryInterval):
				}
				continue
			}

			consumer := leastInFlight(eligible)
			d.registry.IncrementInFlight(consumer.ID)

			msg.DispatchedAt = time.Now()
			msg.Status = core.StatusInFlight

			// Register before sending so the AckManager always has the entry
			// when the consumer sends back an ACK — even if the consumer is very
			// fast and the ACK arrives before the channel write returns.
			d.ackManager.Register(msg, consumer.ID)

			select {
			case consumer.Send <- msg:
				// successfully queued for delivery
			case <-ctx.Done():
				// Channel write was cancelled before the consumer could receive.
				// Undo everything so the message isn't lost and in-flight is correct.
				d.ackManager.Unregister(msg.ID)
				return
			}
			break
		}
	}
}

// leastInFlight picks the consumer with the fewest unacknowledged messages,
// spreading load naturally toward faster or less busy consumers.
func leastInFlight(consumers []*connmgr.Consumer) *connmgr.Consumer {
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].InFlight() < consumers[j].InFlight()
	})
	return consumers[0]
}
