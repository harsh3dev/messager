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

- [ ] Define binary record format
  - [ ] Magic header bytes (for record boundary detection)
  - [ ] Length-prefix field
  - [ ] Serialised message payload
  - [ ] CRC32 checksum (for torn-write detection)
- [ ] Implement append-only writer
  - [ ] Sequential write only — no random access
  - [ ] `fsync` after each write
  - [ ] One WAL file per queue
- [ ] Implement ACK tombstone records
  - [ ] Tombstone format (same envelope, tombstone flag set)
  - [ ] Write tombstone to WAL on ACK
- [ ] Implement WAL reader / replay
  - [ ] Read records sequentially from start
  - [ ] Stop replay at first CRC mismatch (torn write boundary)
  - [ ] Apply tombstones — skip tombstoned messages during replay
  - [ ] Return only un-tombstoned messages to caller
- [ ] Unit tests
  - [ ] Replay after clean shutdown restores all messages
  - [ ] Replay after mid-write crash does not corrupt state
  - [ ] Tombstoned messages do not reappear on replay

**Done when:** WAL is the source of truth; restart correctly restores only unacknowledged messages.

---

## Phase 2 — Queue Manager

- [ ] Implement in-memory FIFO queue
  - [ ] Thread-safe enqueue / dequeue
  - [ ] Blocking dequeue (dispatcher blocks until message available)
- [ ] Implement enqueue pipeline
  - [ ] Write to WAL first
  - [ ] Add to in-memory queue only after WAL write succeeds
- [ ] Implement startup replay
  - [ ] Read WAL via Phase 1 reader
  - [ ] Re-enqueue all returned (un-tombstoned) messages
  - [ ] Preserve original enqueue timestamp (not reset to now)
- [ ] Unit tests
  - [ ] Publish → message appears in queue
  - [ ] Restart → queue is restored from WAL
  - [ ] Enqueue failure (WAL error) does not add message to memory

**Done when:** queue is consistent across restarts; in-memory state always derived from WAL.

---

## Phase 3 — gRPC API Layer

- [ ] Implement `Publish` endpoint
  - [ ] Receive message from producer
  - [ ] Hand off to Queue Manager enqueue pipeline
  - [ ] Return acknowledgement to producer
- [ ] Implement `Subscribe` endpoint (bidirectional stream)
  - [ ] Accept consumer connection with queue name and prefetch limit
  - [ ] Spawn send goroutine (server → consumer: push `Message`)
  - [ ] Spawn receive goroutine (consumer → server: read `AckRequest`)
  - [ ] Route incoming ACK/NACK to Ack Manager (Phase 6 stub for now)
  - [ ] Handle stream close / consumer disconnect cleanly
- [ ] Unit tests
  - [ ] Producer can publish a message
  - [ ] Consumer can establish a persistent stream connection
  - [ ] Stream closes without panic on consumer disconnect

**Done when:** basic producer/consumer communication works over gRPC streams.

---

## Phase 4 — Connection Manager

- [ ] Define `Consumer` struct
  - [ ] Consumer ID
  - [ ] Queue name
  - [ ] Send channel (to dispatcher)
  - [ ] Prefetch limit
  - [ ] In-flight counter (atomic)
  - [ ] Connected-at timestamp
- [ ] Implement consumer registry (per queue)
  - [ ] Register consumer on subscribe
  - [ ] Deregister consumer on disconnect
  - [ ] Thread-safe access (multiple goroutines read the registry)
- [ ] Expose query interface for dispatcher
  - [ ] `EligibleConsumers(queue)` — returns consumers where in-flight < prefetch
  - [ ] `IncrementInFlight(consumerID)`
  - [ ] `DecrementInFlight(consumerID)`
- [ ] Unit tests
  - [ ] Consumer disconnect → removed from registry cleanly
  - [ ] Multiple consumers → all tracked correctly
  - [ ] `EligibleConsumers` respects prefetch limits

**Done when:** system knows who can receive messages and can enforce prefetch limits.

---

## Phase 5 — Dispatcher + Flow Control

- [ ] Implement dispatcher loop (one goroutine per queue)
  - [ ] Block on queue dequeue until message available
  - [ ] Query Connection Manager for eligible consumers
  - [ ] If no eligible consumers — wait and retry (backoff)
- [ ] Implement consumer selection (least in-flight)
  - [ ] Sort eligible consumers by in-flight count ascending
  - [ ] Select the first (lowest in-flight)
- [ ] Implement prefetch enforcement (flow control)
  - [ ] Only select consumers where in-flight < prefetch
  - [ ] Naturally throttles slow consumers; fast consumers get more load
- [ ] Implement message push
  - [ ] Call `IncrementInFlight` on Connection Manager *before* sending
  - [ ] Send message to consumer via send channel
  - [ ] Record dispatch timestamp (for timeout scanner in Phase 7)
- [ ] Unit tests
  - [ ] Multiple consumers → load distributed by in-flight count
  - [ ] Consumer at prefetch limit → receives no further messages
  - [ ] No consumer available → dispatcher waits without blocking the queue

**Done when:** messages flow to consumers; slow consumers are naturally throttled.

---

## Phase 6 — Ack Manager

- [ ] Define in-flight map
  - [ ] Key: `MessageID`
  - [ ] Value: `InFlightEntry` (message, consumer ID, dispatch timestamp, retry count)
  - [ ] Thread-safe access
- [ ] Implement `Register` (called by Dispatcher on push)
  - [ ] Add entry to in-flight map
- [ ] Implement `Ack` handler
  - [ ] Remove from in-flight map
  - [ ] Call `DecrementInFlight` on Connection Manager
  - [ ] Write ACK tombstone to WAL
- [ ] Implement `Nack` handler
  - [ ] Remove from in-flight map
  - [ ] Call `DecrementInFlight` on Connection Manager
  - [ ] Re-enqueue message (increment retry count)
- [ ] Wire ACK/NACK from gRPC receive goroutine (Phase 3) into Ack Manager
- [ ] Unit tests
  - [ ] ACK → message removed permanently; tombstone written to WAL
  - [ ] NACK → message reappears in queue with incremented retry count
  - [ ] In-flight count decrements correctly on both ACK and NACK

**Done when:** system guarantees at-least-once delivery with correct state transitions.

---

## Phase 7 — Timeout · Retry · DLQ

- [ ] Implement timeout scanner
  - [ ] Background goroutine, configurable scan interval
  - [ ] Iterate in-flight map, flag entries exceeding timeout threshold
  - [ ] Treat timed-out entry as implicit NACK (requeue via Ack Manager)
- [ ] Implement retry counter
  - [ ] Retry count stored on `Message` and persisted in WAL record
  - [ ] Increment on each NACK or timeout requeue
- [ ] Implement DLQ
  - [ ] Configure max retry threshold
  - [ ] On retry count > threshold → move message to DLQ instead of requeueing
  - [ ] DLQ is a WAL-backed queue (separate file, same record format)
  - [ ] DLQ contents survive broker restart
- [ ] Unit tests
  - [ ] Kill consumer mid-delivery → message reassigned after timeout
  - [ ] Message exceeding max retries → lands in DLQ, not re-delivered
  - [ ] DLQ persists across restart
  - [ ] No message stuck in-flight forever

**Done when:** system self-recovers from consumer failures; poison messages are isolated.

---

## Phase 8 — Hardening

- [ ] Implement WAL compaction
  - [ ] Trigger: message count threshold or time interval (configurable)
  - [ ] Read current WAL segment
  - [ ] Drop all tombstoned records
  - [ ] Write clean segment atomically (write temp file, rename)
  - [ ] Verify compacted WAL before replacing original
- [ ] Implement graceful shutdown
  - [ ] Stop accepting new Publish requests
  - [ ] Drain dispatcher loop (no new dispatches)
  - [ ] Wait for in-flight map to drain (or timeout)
  - [ ] Flush and close WAL
- [ ] Implement in-flight recovery on restart
  - [ ] Messages replayed from WAL with retry count > 0 → treated as NACK candidates
  - [ ] Re-enqueued with existing retry count preserved (not reset)
- [ ] Harden error paths
  - [ ] WAL write failure → do not enqueue, return error to producer
  - [ ] Consumer send failure → treat as NACK immediately
  - [ ] Registry access on nil consumer → safe no-op
- [ ] Unit tests
  - [ ] Compaction produces a WAL that replays identically to original
  - [ ] Graceful shutdown with in-flight messages → messages requeued on next start
  - [ ] Hard kill mid-write → clean recovery on restart

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
