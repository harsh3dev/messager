The WAL is append-only — it only ever grows. Every message write and every ACK tombstone adds bytes to the file. The compactor is what trims it back down.

**The problem it solves**

Imagine a queue that processes 1 million messages per day. Every message gets two WAL records: one when it's enqueued, one tombstone when it's ACKed. After a week, the WAL has 14 million records — but at any given moment, only a few thousand are actually live (un-ACKed). The rest is dead weight that only slows down WAL replay on restart.

**What it does**

```
Before compact:           After compact:
┌─────────────┐           ┌─────────────┐
│ msg-1       │           │ msg-1       │  ← kept (not tombstoned)
│ msg-2       │           │ msg-3       │  ← kept
│ msg-3       │           └─────────────┘
│ tombstone-2 │           ← gone (msg-2 and its tombstone removed)
│ tombstone-1 │
└─────────────┘
```

It replays the WAL (applying tombstones to filter out ACKed messages), writes only the survivors to a temp file, verifies the temp file, then atomically renames it over the original.

**Why atomic (temp file + rename)**

If you rewrote the WAL in place and crashed mid-write, you'd have a partially written file with no way to recover. With the temp file approach:
- Crash before rename → original WAL is untouched, temp file is just deleted
- Crash after rename → compacted WAL is complete and valid
- There's no window where the data is at risk

**When to call it**

Right now `CompactAll()` exists but nothing calls it automatically. In Phase 9 or as an operational tool you'd trigger it:
- On a schedule (e.g., every hour via a goroutine in main.go)
- When the WAL file exceeds a size threshold
- Manually as a maintenance operation

Without compaction, the WAL grows without bound and restart time grows proportionally (it has to replay every record ever written).