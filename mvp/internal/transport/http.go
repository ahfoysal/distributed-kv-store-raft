package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/foysal/distkv/internal/raft"
)

type HTTPTransport struct {
	client *http.Client
}

func NewHTTP() *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{Timeout: 200 * time.Millisecond},
	}
}

func (h *HTTPTransport) post(url string, req, reply any) error {
	body, _ := json.Marshal(req)
	resp, err := h.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	return json.Unmarshal(b, reply)
}

func (h *HTTPTransport) SendRequestVote(p raft.Peer, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	err := h.post("http://"+p.Addr+"/raft/vote", args, &reply)
	return reply, err
}

func (h *HTTPTransport) SendAppendEntries(p raft.Peer, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	err := h.post("http://"+p.Addr+"/raft/append", args, &reply)
	return reply, err
}
