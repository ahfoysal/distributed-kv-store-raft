package raft

import (
	"log"
	"time"
)

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
	// Hint for faster conflict resolution
	ConflictIndex int `json:"conflict_index"`
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Check log consistency. Slice positions are offset by snapshotLastIdx.
	base := n.snapshotLastIdx
	if args.PrevLogIndex > 0 {
		prevPos := args.PrevLogIndex - base
		if prevPos < 0 {
			// PrevLogIndex is already covered by our snapshot — treat as ok
			// (caller will trim overlapping entries below).
		} else if prevPos >= len(n.log) {
			reply.ConflictIndex = base + len(n.log)
			return reply
		} else if n.log[prevPos].Term != args.PrevLogTerm {
			reply.ConflictIndex = args.PrevLogIndex
			return reply
		}
	}

	// Append entries, truncating conflicting suffix
	changed := false
	for i, e := range args.Entries {
		absPos := args.PrevLogIndex + 1 + i
		pos := absPos - base
		if pos <= 0 {
			continue // already covered by snapshot
		}
		if pos < len(n.log) {
			if n.log[pos].Term != e.Term {
				n.log = n.log[:pos]
				n.log = append(n.log, e)
				changed = true
			}
		} else {
			n.log = append(n.log, e)
			changed = true
		}
	}
	if changed && n.persister != nil {
		// Rewrite WAL with entries after the snapshot. This is simpler than
		// tracking per-entry append vs truncation, and keeps crash recovery
		// straightforward.
		tail := n.log[1:] // skip the sentinel (or snapshot-derived) index-0 slot
		if err := n.persister.RewriteWAL(tail); err != nil {
			log.Printf("[%s] WAL rewrite error: %v", n.id, err)
		}
	}

	if args.LeaderCommit > n.commitIndex {
		last := n.log[len(n.log)-1].Index
		if args.LeaderCommit < last {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = last
		}
		n.persistStateLocked()
	}

	reply.Term = n.currentTerm
	reply.Success = true
	return reply
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if isLeader {
				n.broadcastHeartbeat()
			}
		}
	}
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderCommit := n.commitIndex
	peers := n.peers
	n.mu.Unlock()

	for _, p := range peers {
		go n.replicateTo(p, term, leaderCommit)
	}
}

func (n *Node) replicateTo(p Peer, term, leaderCommit int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[p.ID]
	if nextIdx < n.snapshotLastIdx+1 {
		// Follower is behind our snapshot boundary; for M2 we don't implement
		// InstallSnapshot RPC, so skip — the follower will catch up once a new
		// entry arrives past the snapshot. This only happens if a follower
		// was offline across many snapshots.
		nextIdx = n.snapshotLastIdx + 1
	}
	prevIdx := nextIdx - 1
	prevPos := prevIdx - n.snapshotLastIdx
	if prevPos < 0 || prevPos >= len(n.log) {
		n.mu.Unlock()
		return
	}
	prevTerm := n.log[prevPos].Term
	var entries []LogEntry
	startPos := nextIdx - n.snapshotLastIdx
	if startPos < len(n.log) {
		entries = append(entries, n.log[startPos:]...)
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	n.mu.Unlock()

	reply, err := n.rpc.SendAppendEntries(p, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}
	if reply.Success {
		n.matchIndex[p.ID] = prevIdx + len(entries)
		n.nextIndex[p.ID] = n.matchIndex[p.ID] + 1
		n.advanceCommit()
	} else {
		if reply.ConflictIndex > 0 {
			n.nextIndex[p.ID] = reply.ConflictIndex
		} else if n.nextIndex[p.ID] > 1 {
			n.nextIndex[p.ID]--
		}
	}
}

// advanceCommit is called with lock held.
func (n *Node) advanceCommit() {
	lastIdx := n.log[len(n.log)-1].Index
	base := n.snapshotLastIdx
	for N := lastIdx; N > n.commitIndex; N-- {
		pos := N - base
		if pos <= 0 || pos >= len(n.log) {
			continue
		}
		if n.log[pos].Term != n.currentTerm {
			continue
		}
		count := 1 // leader
		for _, p := range n.peers {
			if n.matchIndex[p.ID] >= N {
				count++
			}
		}
		if count > (len(n.peers)+1)/2 {
			n.commitIndex = N
			log.Printf("[%s] committed up to %d", n.id, N)
			n.persistStateLocked()
			break
		}
	}
}

func (n *Node) applyLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				pos := n.lastApplied - n.snapshotLastIdx
				if pos < 0 || pos >= len(n.log) {
					n.lastApplied--
					break
				}
				entry := n.log[pos]
				cmd := entry.Command
				applier := n.applier
				n.mu.Unlock()
				if applier != nil {
					applier(cmd)
				} else {
					n.applyCh <- ApplyMsg{Index: entry.Index, Command: cmd}
				}
				n.mu.Lock()
				n.sinceLastSnap++
			}
			n.maybeSnapshotLocked()
			n.mu.Unlock()
		}
	}
}
