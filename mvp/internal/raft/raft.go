package raft

import (
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

	state          State
	electionReset  time.Time
	leaderID       string
	applyCh        chan ApplyMsg
	shutdownCh     chan struct{}
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
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = ""
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionTimer()
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
	return idx, n.currentTerm, true
}
