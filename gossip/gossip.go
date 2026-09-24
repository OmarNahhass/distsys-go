package gossip

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"distsys/rpc"
)

type Member struct {
	Name  string `json:"name"`
	Addr  string `json:"addr"`
	Alive bool   `json:"alive"`
}

type Gossiper struct {
	mu      sync.Mutex
	self    Member
	members map[string]Member
	pos     int
}

func New(selfName, selfAddr string) *Gossiper {
	g := &Gossiper{members: make(map[string]Member)}
	g.self = Member{Name: selfName, Addr: selfAddr, Alive: true}
	g.members[selfName] = g.self
	return g
}

func (g *Gossiper) Join(m Member) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.members[m.Name] = m
}

func (g *Gossiper) Members() []Member {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Member, 0, len(g.members))
	for _, m := range g.members {
		out = append(out, m)
	}
	return out
}

func (g *Gossiper) merge(incoming []Member) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, m := range incoming {
		existing, ok := g.members[m.Name]
		if !ok {
			g.members[m.Name] = m
			continue
		}
		if !m.Alive && existing.Alive {
			g.members[m.Name] = m
		}
	}
}

func (g *Gossiper) markDead(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if m, ok := g.members[name]; ok {
		m.Alive = false
		g.members[name] = m
	}
}

func (g *Gossiper) markAlive(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if m, ok := g.members[name]; ok {
		m.Alive = true
		g.members[name] = m
	}
}

func (g *Gossiper) nextPeer() *Member {
	g.mu.Lock()
	defer g.mu.Unlock()

	names := make([]string, 0, len(g.members))
	for name := range g.members {
		if name != g.self.Name {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)

	name := names[g.pos%len(names)]
	g.pos++
	m := g.members[name]
	return &m
}

func (g *Gossiper) Tick() error {
	peer := g.nextPeer()
	if peer == nil {
		return nil
	}

	client, err := rpc.Dial(peer.Addr)
	if err != nil {
		g.markDead(peer.Name)
		return fmt.Errorf("peer %s unreachable, marked dead: %w", peer.Name, err)
	}
	defer client.Close()

	var pingReply PingReply
	if err := client.Call("Gossip.Ping", PingArgs{}, &pingReply); err != nil {
		g.markDead(peer.Name)
		return fmt.Errorf("peer %s did not respond to ping, marked dead: %w", peer.Name, err)
	}
	g.markAlive(peer.Name)

	var remoteMembers []Member
	if err := client.Call("Gossip.Exchange", g.Members(), &remoteMembers); err != nil {
		return fmt.Errorf("exchange with %s failed: %w", peer.Name, err)
	}
	g.merge(remoteMembers)

	return nil
}

type PingArgs struct{}
type PingReply struct {
	OK bool `json:"ok"`
}

func Register(server *rpc.Server, g *Gossiper) {
	server.Register("Gossip.Ping", func(raw json.RawMessage) (interface{}, error) {
		return PingReply{OK: true}, nil
	})

	server.Register("Gossip.Exchange", func(raw json.RawMessage) (interface{}, error) {
		var incoming []Member
		if err := json.Unmarshal(raw, &incoming); err != nil {
			return nil, err
		}
		g.merge(incoming)
		return g.Members(), nil
	})
}
