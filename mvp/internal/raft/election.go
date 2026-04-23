package raft

import (
	"log"
	"time"
)

type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	lastIdx, lastTerm := n.lastLogInfo()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOK {
		n.votedFor = args.CandidateID
		n.resetElectionTimer()
		reply.VoteGranted = true
	}
	reply.Term = n.currentTerm
	return reply
}

func (n *Node) electionLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.state != Leader && time.Now().After(n.electionReset) {
				n.startElection()
			}
			n.mu.Unlock()
		}
	}
}

// startElection is called with lock held.
func (n *Node) startElection() {
	n.becomeCandidate()
	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogInfo()
	log.Printf("[%s] starting election for term %d", n.id, term)

	votes := 1 // self
	votesCh := make(chan bool, len(n.peers))

	for _, p := range n.peers {
		go func(peer Peer) {
			reply, err := n.rpc.SendRequestVote(peer, RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				votesCh <- false
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				votesCh <- false
				return
			}
			if n.state == Candidate && n.currentTerm == term && reply.VoteGranted {
				votesCh <- true
			} else {
				votesCh <- false
			}
		}(p)
	}

	go func() {
		needed := (len(n.peers)+1)/2 + 1
		for i := 0; i < len(n.peers); i++ {
			if <-votesCh {
				votes++
				if votes >= needed {
					n.mu.Lock()
					if n.state == Candidate && n.currentTerm == term {
						log.Printf("[%s] elected leader for term %d (%d votes)", n.id, term, votes)
						n.becomeLeader()
						go n.broadcastHeartbeat()
					}
					n.mu.Unlock()
					return
				}
			}
		}
	}()
}
