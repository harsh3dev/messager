package queue

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/harsh3dev/messager/internal/core"
	"github.com/harsh3dev/messager/internal/wal"
)

// Manager owns all topics and their WAL writers.
// One WAL file and one Topic per queue name.
type Manager struct {
	walDir  string
	mu      sync.Mutex
	topics  map[string]*Topic
	writers map[string]*wal.Writer
}

// NewManager creates a Manager and replays any existing WAL files in walDir.
// Messages surviving replay (not tombstoned) are pre-loaded into their topics.
func NewManager(walDir string) (*Manager, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}

	m := &Manager{
		walDir:  walDir,
		topics:  make(map[string]*Topic),
		writers: make(map[string]*wal.Writer),
	}

	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		queue := strings.TrimSuffix(e.Name(), ".wal")
		if err := m.initQueue(queue); err != nil {
			return nil, fmt.Errorf("replay %s: %w", queue, err)
		}
	}

	return m, nil
}

// initQueue replays the WAL for a queue and sets up its topic and writer.
// Must be called with m.mu held or during single-threaded init.
func (m *Manager) initQueue(queue string) error {
	if _, ok := m.topics[queue]; ok {
		return nil
	}

	msgs, err := wal.NewReader(m.walDir, queue).Replay()
	if err != nil {
		return err
	}

	w, err := wal.NewWriter(m.walDir, queue)
	if err != nil {
		return err
	}

	t := NewTopic()
	for _, msg := range msgs {
		t.Enqueue(msg)
	}

	m.topics[queue] = t
	m.writers[queue] = w
	return nil
}

// Enqueue writes the message to the WAL then adds it to the in-memory topic.
// The in-memory enqueue only happens if the WAL write succeeds.
func (m *Manager) Enqueue(msg core.Message) error {
	m.mu.Lock()
	if err := m.initQueue(msg.Queue); err != nil {
		m.mu.Unlock()
		return err
	}
	w := m.writers[msg.Queue]
	t := m.topics[msg.Queue]
	m.mu.Unlock()

	// WAL write outside the lock — Sync is the slow path
	if err := w.WriteMessage(msg); err != nil {
		return err
	}

	t.Enqueue(msg)
	return nil
}

// Dequeue blocks until a message is available for the given queue.
// Returns false only when the topic is closed and empty (shutdown).
func (m *Manager) Dequeue(queue string) (core.Message, bool) {
	m.mu.Lock()
	if err := m.initQueue(queue); err != nil {
		m.mu.Unlock()
		return core.Message{}, false
	}
	t := m.topics[queue]
	m.mu.Unlock()

	return t.Dequeue() // blocks outside the lock
}

// WriteTombstone writes an ACK tombstone to the queue's WAL.
// Called by the Ack Manager (Phase 6) when a message is acknowledged.
func (m *Manager) WriteTombstone(id core.MessageID, queue string) error {
	m.mu.Lock()
	w := m.writers[queue]
	m.mu.Unlock()

	if w == nil {
		return fmt.Errorf("queue %q not found", queue)
	}
	return w.WriteTombstone(id)
}

// Close shuts down all topics (unblocking any waiting dispatchers) and
// flushes and closes all WAL writers.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, t := range m.topics {
		t.Close()
	}

	var firstErr error
	for _, w := range m.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
