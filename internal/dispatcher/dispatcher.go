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

			select {
			case consumer.Send <- msg:
				d.ackManager.Register(msg, consumer.ID)
			case <-ctx.Done():
				// Undo the increment — message was never received.
				d.registry.DecrementInFlight(consumer.ID)
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
