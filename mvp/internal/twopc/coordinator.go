package twopc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/foysal/distkv/internal/shard"
)

// Participant is the interface 2PC needs from each shard.
type Participant interface {
	// ApplyOp performs one durable write against the shard's LSM. It is
	// called after the coordinator has decided to commit.
	ApplyOp(op Op) error
}

// shardAdapter wraps a *shard.Shard so it satisfies Participant.
type shardAdapter struct{ sh *shard.Shard }

func (a shardAdapter) ApplyOp(op Op) error {
	switch op.Kind {
	case "put":
		return a.sh.Put(op.Key, op.Value)
	case "delete":
		return a.sh.Delete(op.Key)
	default:
		return fmt.Errorf("unknown 2pc op kind %q", op.Kind)
	}
}

// Coordinator runs the 2PC protocol across participating shards. It owns
// the decision log and lazily opens one ParticipantLog per shard id.
type Coordinator struct {
	dataRoot     string
	decisionPath string
	router       *shard.Router

	mu       sync.Mutex
	pLogs    map[string]*ParticipantLog
	outcomes map[string]string // txn_id → "commit" | "abort" (from decision log)
	seq      atomic.Uint64
}

// NewCoordinator opens (or creates) the decision log at dataRoot/decision.log
// and rehydrates the outcome map.
func NewCoordinator(dataRoot string, router *shard.Router) (*Coordinator, error) {
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return nil, err
	}
	c := &Coordinator{
		dataRoot:     dataRoot,
		decisionPath: filepath.Join(dataRoot, "decision.log"),
		router:       router,
		pLogs:        make(map[string]*ParticipantLog),
		outcomes:     make(map[string]string),
	}
	buf, err := os.ReadFile(c.decisionPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, line := range splitLines(buf) {
		if len(line) == 0 {
			continue
		}
		var d decisionRecord
		if err := json.Unmarshal(line, &d); err != nil {
			return nil, fmt.Errorf("parse decision.log: %w", err)
		}
		c.outcomes[d.TxnID] = d.Outcome
	}
	return c, nil
}

// participantLog returns (opening if needed) the log for shardID.
func (c *Coordinator) participantLog(shardID string) (*ParticipantLog, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok := c.pLogs[shardID]; ok {
		return l, nil
	}
	l, err := OpenParticipantLog(filepath.Join(c.dataRoot, shardID, "prepare.log"))
	if err != nil {
		return nil, err
	}
	c.pLogs[shardID] = l
	return l, nil
}

// NewTxnID mints a fresh txn id.
func (c *Coordinator) NewTxnID() string {
	n := c.seq.Add(1)
	return fmt.Sprintf("t-%d-%d", time.Now().UnixNano(), n)
}

// groupByShard partitions ops by the shard that owns each key. Used by the
// high-level `Commit` helper that takes an unordered op list.
func (c *Coordinator) groupByShard(ops []Op) ([]ShardWrites, error) {
	buckets := map[string][]Op{}
	for _, op := range ops {
		info := c.router.Manifest().Find(op.Key)
		buckets[info.ID] = append(buckets[info.ID], op)
	}
	var out []ShardWrites
	for id, ops := range buckets {
		out = append(out, ShardWrites{ShardID: id, Ops: ops})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out, nil
}

// Commit runs the full 2PC protocol: group ops by shard, PREPARE each
// participant, write the commit decision, then APPLY. On any prepare
// failure the whole txn is aborted and every prepared participant is told
// to drop the record. Returns the final outcome ("commit"/"abort").
func (c *Coordinator) Commit(ops []Op) (string, string, error) {
	txnID := c.NewTxnID()
	outcome, err := c.CommitWithID(txnID, ops)
	return txnID, outcome, err
}

// CommitWithID runs 2PC with a caller-supplied txn id (needed by tests that
// inject crashes).
func (c *Coordinator) CommitWithID(txnID string, ops []Op) (string, error) {
	groups, err := c.groupByShard(ops)
	if err != nil {
		return "", err
	}
	commitTS := uint64(time.Now().UnixNano())

	// Phase 1: PREPARE.
	prepared := make([]string, 0, len(groups))
	for _, g := range groups {
		pl, err := c.participantLog(g.ShardID)
		if err != nil {
			_ = c.abortPrepared(txnID, prepared, groups)
			return "abort", fmt.Errorf("open participant log: %w", err)
		}
		if err := pl.Prepare(txnID, commitTS, g.Ops); err != nil {
			_ = c.abortPrepared(txnID, prepared, groups)
			return "abort", fmt.Errorf("prepare %s: %w", g.ShardID, err)
		}
		prepared = append(prepared, g.ShardID)
	}

	// Phase 2a: write commit decision. Writing this line is the atomic
	// moment of truth: if we crash after this line hits disk, recovery
	// will replay APPLY on every participant.
	shardIDs := make([]string, 0, len(groups))
	for _, g := range groups {
		shardIDs = append(shardIDs, g.ShardID)
	}
	if err := c.writeDecision(decisionRecord{
		TxnID: txnID, CommitTS: commitTS, Outcome: "commit", Shards: shardIDs,
	}); err != nil {
		_ = c.abortPrepared(txnID, prepared, groups)
		return "abort", fmt.Errorf("write decision: %w", err)
	}

	// Phase 2b: apply.
	if err := c.applyTxn(txnID, groups); err != nil {
		return "commit", fmt.Errorf("apply after commit decision: %w", err)
	}
	return "commit", nil
}

// writeDecision appends one line to decision.log and fsyncs.
func (c *Coordinator) writeDecision(d decisionRecord) error {
	buf, err := json.Marshal(d)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(c.decisionPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(buf, '\n')); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	c.mu.Lock()
	c.outcomes[d.TxnID] = d.Outcome
	c.mu.Unlock()
	return nil
}

// abortPrepared undoes any prepared records and writes an abort decision.
func (c *Coordinator) abortPrepared(txnID string, prepared []string, groups []ShardWrites) error {
	for _, id := range prepared {
		pl, err := c.participantLog(id)
		if err == nil {
			_ = pl.MarkAborted(txnID)
		}
	}
	shardIDs := make([]string, 0, len(groups))
	for _, g := range groups {
		shardIDs = append(shardIDs, g.ShardID)
	}
	return c.writeDecision(decisionRecord{TxnID: txnID, Outcome: "abort", Shards: shardIDs})
}

// applyTxn walks each participant's prepare record and replays its ops
// against the live shard. Idempotent: safe to call multiple times (we use
// the MarkApplied flag to skip already-applied txns).
func (c *Coordinator) applyTxn(txnID string, groups []ShardWrites) error {
	for _, g := range groups {
		pl, err := c.participantLog(g.ShardID)
		if err != nil {
			return err
		}
		rec, ok := pl.Lookup(txnID)
		if !ok {
			// Nothing prepared — nothing to apply.
			continue
		}
		if rec.Phase == "applied" {
			continue
		}
		adapter, err := c.participantAdapter(g.ShardID)
		if err != nil {
			return err
		}
		for _, op := range rec.Ops {
			if err := adapter.ApplyOp(op); err != nil {
				return fmt.Errorf("apply on %s: %w", g.ShardID, err)
			}
		}
		if err := pl.MarkApplied(txnID); err != nil {
			return err
		}
	}
	return nil
}

// participantAdapter looks up the live shard for id.
func (c *Coordinator) participantAdapter(shardID string) (Participant, error) {
	sh := c.router.Shard(shardID)
	if sh == nil {
		return nil, fmt.Errorf("shard %q not open", shardID)
	}
	return shardAdapter{sh: sh}, nil
}

// Recover walks every shard's prepare.log and resolves each still-pending
// txn against the coordinator decision log:
//   - decision=commit → apply the prepare on that shard (idempotent).
//   - decision=abort → mark the prepare aborted.
//   - no decision    → abort (we are the coordinator, so if we don't have
//     a decision, nobody does).
//
// Returns the number of recovered txns (committed + aborted).
func (c *Coordinator) Recover() (committed, aborted int, err error) {
	for _, info := range c.router.Manifest().Snapshot() {
		pl, err := c.participantLog(info.ID)
		if err != nil {
			return committed, aborted, err
		}
		for _, rec := range pl.Pending() {
			outcome := c.outcomes[rec.TxnID]
			switch outcome {
			case "commit":
				adapter, err := c.participantAdapter(info.ID)
				if err != nil {
					return committed, aborted, err
				}
				for _, op := range rec.Ops {
					if err := adapter.ApplyOp(op); err != nil {
						return committed, aborted, err
					}
				}
				if err := pl.MarkApplied(rec.TxnID); err != nil {
					return committed, aborted, err
				}
				committed++
			case "abort":
				if err := pl.MarkAborted(rec.TxnID); err != nil {
					return committed, aborted, err
				}
				aborted++
			default:
				// No decision: coordinator never crossed the commit point,
				// so the txn aborts. Write an abort decision so subsequent
				// recoveries are stable.
				if err := pl.MarkAborted(rec.TxnID); err != nil {
					return committed, aborted, err
				}
				if err := c.writeDecision(decisionRecord{
					TxnID: rec.TxnID, Outcome: "abort",
					Shards: []string{info.ID},
				}); err != nil {
					return committed, aborted, err
				}
				aborted++
			}
		}
	}
	return committed, aborted, nil
}

// PrepareAndDecideCommitNoApply is a crash-simulation helper. It runs the
// full PREPARE + write-commit-decision phases but does NOT call applyTxn,
// modeling a coordinator that survives long enough to cross the
// commit-point but crashes before telling participants to apply. A
// subsequent Recover() call on a fresh Coordinator must still finish the
// txn atomically on every shard.
func (c *Coordinator) PrepareAndDecideCommitNoApply(ops []Op) (string, error) {
	txnID := c.NewTxnID()
	groups, err := c.groupByShard(ops)
	if err != nil {
		return "", err
	}
	commitTS := uint64(time.Now().UnixNano())
	for _, g := range groups {
		pl, err := c.participantLog(g.ShardID)
		if err != nil {
			return "", err
		}
		if err := pl.Prepare(txnID, commitTS, g.Ops); err != nil {
			return "", err
		}
	}
	shardIDs := make([]string, 0, len(groups))
	for _, g := range groups {
		shardIDs = append(shardIDs, g.ShardID)
	}
	if err := c.writeDecision(decisionRecord{
		TxnID: txnID, CommitTS: commitTS, Outcome: "commit", Shards: shardIDs,
	}); err != nil {
		return "", err
	}
	return txnID, nil
}

// Outcome reports the decision recorded for txn, or "" if none.
func (c *Coordinator) Outcome(txnID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outcomes[txnID]
}
