# distsys-go

A from-scratch distributed systems learning project in Go, built step by step.

## Build arc

1. **RPC layer from scratch** (this step) — a minimal request/response system over raw TCP, hand-rolled length-prefixed framing, no `net/rpc` or gRPC yet.
2. Primary-backup KV store (single leader, one or more followers, no consensus yet)
3. Raft from scratch — leader election, log replication, safety
4. Raft-backed KV store
5. Sharded KV store (shard controller + consistent hashing)

## Project layout

```
distsys-go/
  go.mod
  rpc/
    rpc.go        # the RPC framing, Server, and Client
  cmd/
    server/       # demo server registering Echo and Add methods
    client/       # demo client calling both methods
```

## How the RPC layer works

- TCP is a byte stream with no built-in message boundaries, so every message
  is prefixed with a 4-byte big-endian length. The reader always knows
  exactly how many bytes make up "the next message," regardless of how TCP
  happened to chunk the underlying bytes.
- Requests/responses are JSON-encoded. Arguments stay as `json.RawMessage`
  in the core RPC layer — the layer itself never needs to know concrete
  argument types; each registered handler decodes its own expected shape.
- The server runs one goroutine per connection (`Serve` calls `Accept` in a
  loop, spawning `go s.handleConn(conn)` for each).

## Running it

```bash
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client

./bin/server &      # starts listening on :9090
./bin/client        # connects, calls Echo and Add, prints results
```

Expected output from the client:

```
Echo response: hello from client
Add response: 13
Expected error calling unknown method: remote error: unknown method: DoesNotExist
```

## Next step

Build a primary-backup key-value store on top of this RPC layer: a leader
that accepts Put/Get requests and forwards writes to one or more followers,
with no consensus/failover logic yet — that gap is exactly what motivates
building Raft next.
