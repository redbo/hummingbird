package ec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gholt/ring"
)

var (
	electionFrequency       = time.Hour          // hold elections once per hour
	electionDuration        = time.Minute * 5    // elections last 5 minutes
	gossipFrequency         = time.Minute        // normally gossip once per minute
	electionGossipFrequency = time.Second * 10   // during elections, gossip every 10 seconds
	actionTombstoneRemoval  = time.Hour * 24 * 7 // time to tombstone Actions
	friendTimeout           = time.Hour * 3      // forget about friends you haven't heard from in a while
)

// Server represents a peer, and is used in leader election.
type Server struct {
	Name string
	Id   int64
}

// Action represents an action that must occur for the system to reach consistency.
type Action struct {
	Name      string
	Type      string
	Partition uint32
	Src       uint64
	Dst       uint64
}

// GossipState contains all information gossiped between peers.  It's used for leadership election and replicating the Todo list.
type GossipState struct {
	Friends  map[string]*Server
	Todo     map[string]*Action
	Done     map[string]int64
	m        sync.Mutex
	status   *GossipState
	selfName string
	selfId   int64
}

// MergeState takes the data from a peer's global state and merges it into this one.
func (g *GossipState) MergeState(rg *GossipState) {
	g.m.Lock()
	defer g.m.Unlock()
	for name, server := range rg.Friends {
		g.Friends[name] = server
	}
	for id, value := range rg.Todo {
		if _, ok := g.Done[id]; !ok {
			g.Todo[id] = value
		}
	}
	for id, completedAt := range rg.Done {
		g.Done[id] = completedAt
		delete(g.Todo, id)
	}
	now := time.Now().Unix()
	for id, completed := range g.Done {
		if time.Duration(now-completed)*time.Second > actionTombstoneRemoval {
			delete(g.Done, id)
		}
	}
}

func (g *GossipState) ActionList() []*Action {
	g.m.Lock()
	defer g.m.Unlock()
	actionCopy := make([]*Action, 0, len(g.Todo))
	for _, v := range g.Todo {
		actionCopy = append(actionCopy, v)
	}
	return actionCopy
}

// Serialize dumps the global state as a json document.
func (g *GossipState) Serialize() []byte {
	g.m.Lock()
	defer g.m.Unlock()
	data, _ := json.Marshal(g)
	return data
}

// MarkDone marks a pending action as completed.
func (g *GossipState) MarkDone(action string) {
	if action != "" {
		g.m.Lock()
		defer g.m.Unlock()
		delete(g.Todo, action)
		g.Done[action] = time.Now().Unix()
	}
}

// Leader determines whether the server with the given name is currently the leader.
func (g *GossipState) IsLeader() bool {
	g.m.Lock()
	defer g.m.Unlock()
	lookingFor, ok := g.Friends[g.selfName]
	if !ok {
		return false
	}
	for _, f := range g.Friends {
		if f.Id > lookingFor.Id {
			return false
		}
	}
	return true
}

func (g *GossipState) StartElection() {
	g.m.Lock()
	defer g.m.Unlock()
	g.Friends = map[string]*Server{
		g.selfName: &Server{
			Name: g.selfName,
			Id:   rand.Int63(),
		},
	}
}

func (g *GossipState) Gossip(r ring.Ring) {
	var best int64 = 0
	var node ring.Node
	for _, n := range r.Nodes() {
		if val := rand.Int63(); val > best {
			best = val
			node = n
		}
	}
	if node != nil {
		body := g.status.Serialize()
		host, port, _, err := ParseAddress(node.Address(0))
		if err == nil {
			http.Post(fmt.Sprintf("http://%s:%d/gossip", host, port), "application/json", bytes.NewBuffer(body))
		}
	}
}

func NewGossipState() *GossipState {
	return &GossipState{
		Friends:  make(map[string]*Server),
		Todo:     make(map[string]*Action),
		Done:     make(map[string]int64),
		selfName: fmt.Sprintf("%016x", rand.Int63()),
		selfId:   0,
	}
}
