package retry

import (
	"context"
	"time"

	"github.com/harsh3dev/messager/internal/ackmgr"
)

// Scanner runs periodic timeout scans against the AckManager.
// Any in-flight message whose DispatchedAt is older than DispatchTimeout
// is treated as an implicit NACK: requeued (retries remaining) or sent to the DLQ.
type Scanner struct {
	ackManager      *ackmgr.AckManager
	interval        time.Duration
	dispatchTimeout time.Duration
}

func NewScanner(ackManager *ackmgr.AckManager, interval time.Duration, dispatchTimeout time.Duration) *Scanner {
	return &Scanner{
		ackManager:      ackManager,
		interval:        interval,
		dispatchTimeout: dispatchTimeout,
	}
}

// Run starts the scan loop. Blocks until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.ackManager.ScanAndTimeout(s.dispatchTimeout)
		}
	}
}
