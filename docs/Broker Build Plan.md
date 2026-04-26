# Push-Based Message Broker — Build Plan

---

## Phase 0 — Foundation

- [x] Define `.proto` file
  - [x] `Publish` RPC (unary)
  - [x] `Subscribe` RPC (bidirectional stream)
  - [x] `AckRequest` message shape (message ID, ACK / NACK signal)
  - [x] `Message` shape (ID, payload, headers, retry count, enqueue timestamp)
- [x] Define core Go types
  - [x] `MessageID` type
  - [x] `Message` struct (ID, payload, metadata, retry count, DLQ flag)
  - [x] Delivery status constants (pending, in-flight, acked, nacked, dead)
- [x] Scaffold project structure
  - [x] `cmd/` — entry point
  - [x] `internal/wal/` — WAL package
  - [x] `internal/queue/` — queue manager package
  - [x] `internal/dispatcher/` — dispatcher package
  - [x] `internal/connmgr/` — connection manager package
  - [x] `internal/ackmgr/` — ack manager package
  - [x] `internal/retry/` — retry + DLQ package
  - [x] `proto/` — generated gRPC code
  - [x] `data/` — WAL files at runtime

**Done when:** proto compiles, all packages exist, core types are importable across packages.

---

## Phase 1 — WAL (Durability Layer)

- [x] Define binary record format
  - [x] Magic header bytes (for record boundary detection)
  - [x] Length-prefix field
  - [x] Serialised message payload
  - [x] CRC32 checksum (for torn-write detection)
- [x] Implement append-only writer
  - [x] Sequential write only — no random access
  - [x] `fsync` after each write
  - [x] One WAL file per queue
- [x] Implement ACK tombstone records
  - [x] Tombstone format (same envelope, tombstone flag set)
  - [x] Write tombstone to WAL on ACK
- [x] Implement WAL reader / replay
  - [x] Read records sequentially from start
  - [x] Stop replay at first CRC mismatch (torn write boundary)
  - [x] Apply tombstones — skip tombstoned messages during replay
  - [x] Return only un-tombstoned messages to caller
- [x] Unit tests
  - [x] Replay after clean shutdown restores all messages
  - [x] Replay after mid-write crash does not corrupt state
  - [x] Tombstoned messages do not reappear on replay

**Done when:** WAL is the source of truth; restart correctly restores only unacknowledged messages.

---

## Phase 2 — Queue Manager

- [x] Implement in-memory FIFO queue
  - [x] Thread-safe enqueue / dequeue
  - [x] Blocking dequeue (dispatcher blocks until message available)
- [x] Implement enqueue pipeline
  - [x] Write to WAL first
  - [x] Add to in-memory queue only after WAL write succeeds
- [x] Implement startup replay
  - [x] Read WAL via Phase 1 reader
  - [x] Re-enqueue all returned (un-tombstoned) messages
  - [x] Preserve original enqueue timestamp (not reset to now)
- [x] Unit tests
  - [x] Publish → message appears in queue
  - [x] Restart → queue is restored from WAL
  - [x] Enqueue failure (WAL error) does not add message to memory

**Done when:** queue is consistent across restarts; in-memory state always derived from WAL.

---

## Phase 3 — gRPC API Layer

- [x] Implement `Publish` endpoint
  - [x] Receive message from producer
  - [x] Hand off to Queue Manager enqueue pipeline
  - [x] Return acknowledgement to producer
- [x] Implement `Subscribe` endpoint (bidirectional stream)
  - [x] Accept consumer connection with queue name and prefetch limit
  - [x] Spawn send goroutine (server → consumer: push `Message`)
  - [x] Spawn receive goroutine (consumer → server: read `AckRequest`)
  - [x] Route incoming ACK/NACK to Ack Manager (Phase 6 stub for now)
  - [x] Handle stream close / consumer disconnect cleanly
- [x] Unit tests
  - [x] Producer can publish a message
  - [x] Consumer can establish a persistent stream connection
  - [x] Stream closes without panic on consumer disconnect

**Done when:** basic producer/consumer communication works over gRPC streams.

---

## Phase 4 — Connection Manager

- [x] Define `Consumer` struct
  - [x] Consumer ID
  - [x] Queue name
  - [x] Send channel (to dispatcher)
  - [x] Prefetch limit
  - [x] In-flight counter (atomic)
  - [x] Connected-at timestamp
- [x] Implement consumer registry (per queue)
  - [x] Register consumer on subscribe
  - [x ] Deregister consumer on disconnect
  - [x] Thread-safe access (multiple goroutines read the registry)
- [x] Expose query interface for dispatcher
  - [x] `EligibleConsumers(queue)` — returns consumers where in-flight < prefetch
  - [x] `IncrementInFlight(consumerID)`
  - [x] `DecrementInFlight(consumerID)`
- [x] Unit tests
  - [x] Consumer disconnect → removed from registry cleanly
  - [x] Multiple consumers → all tracked correctly
  - [x] `EligibleConsumers` respects prefetch limits

**Done when:** system knows who can receive messages and can enforce prefetch limits.

---

## Phase 5 — Dispatcher + Flow Control

- [x] Implement dispatcher loop (one goroutine per queue)
  - [x] Block on queue dequeue until message available
  - [x] Query Connection Manager for eligible consumers
  - [x] If no eligible consumers — wait and retry (backoff)
- [x] Implement consumer selection (least in-flight)
  - [x] Sort eligible consumers by in-flight count ascending
  - [x] Select the first (lowest in-flight)
- [x] Implement prefetch enforcement (flow control)
  - [x] Only select consumers where in-flight < prefetch
  - [x] Naturally throttles slow consumers; fast consumers get more load
- [x] Implement message push
  - [x] Call `IncrementInFlight` on Connection Manager *before* sending
  - [x] Send message to consumer via send channel
  - [x] Record dispatch timestamp (for timeout scanner in Phase 7)
- [x] Unit tests
  - [x] Multiple consumers → load distributed by in-flight count
  - [x] Consumer at prefetch limit → receives no further messages
  - [x] No consumer available → dispatcher waits without blocking the queue

**Done when:** messages flow to consumers; slow consumers are naturally throttled.

---

## Phase 6 — Ack Manager

- [x] Define in-flight map
  - [x] Key: `MessageID`
  - [x] Value: `InFlightEntry` (message, consumer ID, dispatch timestamp, retry count)
  - [x] Thread-safe access
- [x] Implement `Register` (called by Dispatcher on push)
  - [x] Add entry to in-flight map
- [x] Implement `Ack` handler
  - [x] Remove from in-flight map
  - [x] Call `DecrementInFlight` on Connection Manager
  - [x] Write ACK tombstone to WAL
- [x] Implement `Nack` handler
  - [x] Remove from in-flight map
  - [x] Call `DecrementInFlight` on Connection Manager
  - [x] Re-enqueue message (increment retry count)
- [x] Wire ACK/NACK from gRPC receive goroutine (Phase 3) into Ack Manager
- [x] Unit tests
  - [x] ACK → message removed permanently; tombstone written to WAL
  - [x] NACK → message reappears in queue with incremented retry count
  - [x] In-flight count decrements correctly on both ACK and NACK

**Done when:** system guarantees at-least-once delivery with correct state transitions.

---

## Phase 7 — Timeout · Retry · DLQ

- [x] Implement timeout scanner
  - [x] Background goroutine, configurable scan interval
  - [x] Iterate in-flight map, flag entries exceeding timeout threshold
  - [x] Treat timed-out entry as implicit NACK (requeue via Ack Manager)
- [x] Implement retry counter
  - [x] Retry count stored on `Message` and persisted in WAL record
  - [x] Increment on each NACK or timeout requeue
- [x] Implement DLQ
  - [x] Configure max retry threshold
  - [x] On retry count > threshold → move message to DLQ instead of requeueing
  - [x] DLQ is a WAL-backed queue (separate file, same record format)
  - [x] DLQ contents survive broker restart
- [x] Unit tests
  - [x] Kill consumer mid-delivery → message reassigned after timeout
  - [x] Message exceeding max retries → lands in DLQ, not re-delivered
  - [x] DLQ persists across restart
  - [x] No message stuck in-flight forever

**Done when:** system self-recovers from consumer failures; poison messages are isolated.

---

## Phase 8 — Hardening

- [x] Implement WAL compaction
  - [x] Trigger: message count threshold or time interval (configurable)
  - [x] Read current WAL segment
  - [x] Drop all tombstoned records
  - [x] Write clean segment atomically (write temp file, rename)
  - [x] Verify compacted WAL before replacing original
- [x] Implement graceful shutdown
  - [x] Stop accepting new Publish requests
  - [x] Drain dispatcher loop (no new dispatches)
  - [x] Wait for in-flight map to drain (or timeout)
  - [x] Flush and close WAL
- [x] Implement in-flight recovery on restart
  - [x] Messages replayed from WAL with retry count > 0 → treated as NACK candidates
  - [x] Re-enqueued with existing retry count preserved (not reset)
- [x] Harden error paths
  - [x] WAL write failure → do not enqueue, return error to producer
  - [x] Consumer send failure → treat as NACK immediately
  - [x] Registry access on nil consumer → safe no-op
- [x] Unit tests
  - [x] Compaction produces a WAL that replays identically to original
  - [x] Graceful shutdown with in-flight messages → messages requeued on next start
  - [x] Hard kill mid-write → clean recovery on restart

**Done when:** system handles operational edge cases safely; WAL does not grow unbounded.

---

## Phase 9 — Integration Testing

- [ ] Load balancing scenario
  - [ ] Start 3 consumers with different processing speeds
  - [ ] Verify fast consumer receives proportionally more messages
  - [ ] Verify slow consumer is not overwhelmed (prefetch cap respected)
- [ ] Consumer crash scenario
  - [ ] Start consumer, dispatch messages, kill process
  - [ ] Verify timed-out messages are reassigned to surviving consumers
  - [ ] Verify no message is lost
- [ ] Durability scenario
  - [ ] Publish N messages, kill broker before all are ACKed
  - [ ] Restart broker, verify unacknowledged messages are re-delivered
  - [ ] Verify ACKed messages are not re-delivered
- [ ] Retry / DLQ scenario
  - [ ] Configure a consumer that always NACKs
  - [ ] Verify message retries up to max threshold
  - [ ] Verify message moves to DLQ and is not re-delivered to main queue
- [ ] Backpressure scenario
  - [ ] Set low prefetch limit on all consumers
  - [ ] Flood broker with messages
  - [ ] Verify dispatcher blocks cleanly without memory growth or crash

**Done when:** system behaves correctly under real-world failure conditions end-to-end.

---

## Key invariants to keep in mind throughout

- WAL write always precedes in-memory state change — never the other way around
- In-flight count increments before send, decrements on ACK, NACK, or timeout
- Retry count is part of the message record and persists through WAL replay
- DLQ is a first-class WAL-backed queue, not an in-memory list
- Compaction is always atomic — partial compaction must not corrupt the WAL
