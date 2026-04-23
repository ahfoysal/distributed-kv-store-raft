package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[s]
}

type LogEntry struct {
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command string `json:"command"`
}

type Peer struct {
	ID   string
	Addr string
}

type ApplyMsg struct {
	Index   int
	Command string
}

type RPC interface {
	SendRequestVote(peer Peer, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(peer Peer, args AppendEntriesArgs) (AppendEntriesReply, error)
}

type Node struct {
	mu sync.Mutex

	id    string
	peers []Peer
	rpc   RPC

	currentTerm int
	votedFor    string
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	state         State
	electionReset time.Time
	leaderID      string
	applyCh       chan ApplyMsg
	shutdownCh    chan struct{}

	// Persistence (M2). If persister is nil the node runs purely in-memory.
	persister *Persister
	// snapshotEvery: take a snapshot every N newly-applied entries. 0 disables.
	snapshotEvery    int
	sinceLastSnap    int
	snapshotLastIdx  int // index of the last entry covered by the snapshot
	snapshotLastTerm int

	// KV hooks for snapshot/restore. Set by the host process (main.go).
	kvSnapshot func() map[string]string
	kvRestore  func(map[string]string)
	// When set, applyLoop invokes this synchronously instead of writing to
	// applyCh, so a subsequent snapshot observes the post-apply KV state.
	applier func(cmd string)
}

func NewNode(id string, peers []Peer, rpc RPC, applyCh chan ApplyMsg) *Node {
	n := &Node{
		id:         id,
		peers:      peers,
		rpc:        rpc,
		log:        []LogEntry{{Term: 0, Index: 0}}, // sentinel at index 0
		state:      Follower,
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
		applyCh:    applyCh,
		shutdownCh: make(chan struct{}),
	}
	n.resetElectionTimer()
	return n
}

// AttachPersistence wires a Persister and KV snapshot hooks into the node, and
// restores any on-disk state (term, votedFor, snapshot, WAL entries).
// Must be called before Start(). snapshotEvery=0 disables periodic snapshots.
// SetApplier installs a synchronous apply hook, used in place of applyCh so
// the apply→snapshot sequencing is deterministic.
func (n *Node) SetApplier(f func(cmd string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applier = f
}

func (n *Node) AttachPersistence(
	p *Persister,
	snapshotEvery int,
	kvSnapshot func() map[string]string,
	kvRestore func(map[string]string),
) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.persister = p
	n.snapshotEvery = snapshotEvery
	n.kvSnapshot = kvSnapshot
	n.kvRestore = kvRestore

	loaded, err := p.LoadAll()
	if err != nil {
		return err
	}
	n.currentTerm = loaded.CurrentTerm
	n.votedFor = loaded.VotedFor

	// Snapshot: fast-forward the KV state and seed the log with a sentinel
	// whose Index/Term match the snapshot, so existing index math still holds.
	if loaded.Snapshot != nil {
		if kvRestore != nil {
			kvRestore(loaded.Snapshot.Data)
		}
		n.snapshotLastIdx = loaded.Snapshot.LastIncludedIndex
		n.snapshotLastTerm = loaded.Snapshot.LastIncludedTerm
		n.log = []LogEntry{{Term: loaded.Snapshot.LastIncludedTerm, Index: loaded.Snapshot.LastIncludedIndex}}
		n.commitIndex = loaded.Snapshot.LastIncludedIndex
		n.lastApplied = loaded.Snapshot.LastIncludedIndex
	}
	for _, e := range loaded.Entries {
		n.log = append(n.log, e)
	}
	// Restore commitIndex (bounded by last log index) so already-committed
	// entries get re-applied to the KV state without waiting for a fresh
	// majority.
	if loaded.CommitIndex > n.commitIndex {
		lastIdx := n.log[len(n.log)-1].Index
		if loaded.CommitIndex > lastIdx {
			n.commitIndex = lastIdx
		} else {
			n.commitIndex = loaded.CommitIndex
		}
	}
	if len(loaded.Entries) > 0 || loaded.Snapshot != nil {
		log.Printf("[%s] recovered: term=%d votedFor=%q snapshotIdx=%d walEntries=%d commitIdx=%d",
			n.id, n.currentTerm, n.votedFor, n.snapshotLastIdx, len(loaded.Entries), n.commitIndex)
	}
	return nil
}

// persistStateLocked writes currentTerm/votedFor to disk. Caller holds n.mu.
func (n *Node) persistStateLocked() {
	if n.persister == nil {
		return
	}
	if err := n.persister.SaveState(n.currentTerm, n.votedFor, n.commitIndex); err != nil {
		log.Printf("[%s] persist state error: %v", n.id, err)
	}
}

// maybeSnapshotLocked checks the apply counter and, if threshold exceeded,
// captures KV state into snapshot.json and truncates the WAL prefix up through
// lastApplied. Caller holds n.mu.
func (n *Node) maybeSnapshotLocked() {
	if n.persister == nil || n.snapshotEvery <= 0 || n.kvSnapshot == nil {
		return
	}
	if n.sinceLastSnap < n.snapshotEvery {
		return
	}
	if n.lastApplied <= n.snapshotLastIdx {
		return
	}

	// Find the term of lastApplied in our slice.
	slicePos := n.lastApplied - n.snapshotLastIdx
	if slicePos < 0 || slicePos >= len(n.log) {
		return
	}
	snapIdx := n.lastApplied
	snapTerm := n.log[slicePos].Term
	data := n.kvSnapshot()

	snap := Snapshot{
		LastIncludedIndex: snapIdx,
		LastIncludedTerm:  snapTerm,
		Data:              data,
	}
	if err := n.persister.SaveSnapshot(snap); err != nil {
		log.Printf("[%s] snapshot save error: %v", n.id, err)
		return
	}

	// Compact the in-memory log: new sentinel at snapIdx, keep tail.
	tail := append([]LogEntry(nil), n.log[slicePos+1:]...)
	newLog := make([]LogEntry, 0, 1+len(tail))
	newLog = append(newLog, LogEntry{Term: snapTerm, Index: snapIdx})
	newLog = append(newLog, tail...)

	// Re-persist tail entries into the (freshly truncated) WAL.
	if err := n.persister.RewriteWAL(tail); err != nil {
		log.Printf("[%s] WAL rewrite post-snapshot error: %v", n.id, err)
	}

	n.log = newLog
	n.snapshotLastIdx = snapIdx
	n.snapshotLastTerm = snapTerm
	n.sinceLastSnap = 0
	log.Printf("[%s] snapshot taken @ index=%d term=%d keys=%d", n.id, snapIdx, snapTerm, len(data))
}

func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
}

func (n *Node) Stop() {
	close(n.shutdownCh)
}

func (n *Node) State() (State, int, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm, n.leaderID
}

func (n *Node) resetElectionTimer() {
	// 300–600ms randomized election timeout
	n.electionReset = time.Now().Add(time.Duration(300+rand.Intn(300)) * time.Millisecond)
}

func (n *Node) becomeFollower(term int) {
	termChanged := term != n.currentTerm
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = ""
	n.resetElectionTimer()
	if termChanged {
		n.persistStateLocked()
	}
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionTimer()
	n.persistStateLocked()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	lastIdx := n.log[len(n.log)-1].Index
	for _, p := range n.peers {
		n.nextIndex[p.ID] = lastIdx + 1
		n.matchIndex[p.ID] = 0
	}
}

func (n *Node) lastLogInfo() (int, int) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// Propose appends a command to the log if this node is leader.
// Returns (index, term, isLeader).
func (n *Node) Propose(command string) (int, int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, 0, false
	}
	idx := n.log[len(n.log)-1].Index + 1
	entry := LogEntry{Term: n.currentTerm, Index: idx, Command: command}
	n.log = append(n.log, entry)
	if n.persister != nil {
		if err := n.persister.AppendEntry(entry); err != nil {
			log.Printf("[%s] WAL append error: %v", n.id, err)
		}
	}
	return idx, n.currentTerm, true
}
