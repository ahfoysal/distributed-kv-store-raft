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

	// Check log consistency
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex >= len(n.log) {
			reply.ConflictIndex = len(n.log)
			return reply
		}
		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.ConflictIndex = args.PrevLogIndex
			return reply
		}
	}

	// Append entries, truncating conflicting suffix
	for i, e := range args.Entries {
		pos := args.PrevLogIndex + 1 + i
		if pos < len(n.log) {
			if n.log[pos].Term != e.Term {
				n.log = n.log[:pos]
				n.log = append(n.log, e)
			}
		} else {
			n.log = append(n.log, e)
		}
	}

	if args.LeaderCommit > n.commitIndex {
		last := n.log[len(n.log)-1].Index
		if args.LeaderCommit < last {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = last
		}
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
	if nextIdx < 1 {
		nextIdx = 1
	}
	prevIdx := nextIdx - 1
	prevTerm := n.log[prevIdx].Term
	var entries []LogEntry
	if nextIdx < len(n.log) {
		entries = append(entries, n.log[nextIdx:]...)
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
	for N := lastIdx; N > n.commitIndex; N-- {
		if n.log[N].Term != n.currentTerm {
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
				entry := n.log[n.lastApplied]
				msg := ApplyMsg{Index: entry.Index, Command: entry.Command}
				n.mu.Unlock()
				n.applyCh <- msg
				n.mu.Lock()
			}
			n.mu.Unlock()
		}
	}
}
