# distsys-go

A distributed key-value store built in Go, based off Amazon's Dynamo design.

**GitHub:** https://github.com/OmarNahhass/distsys-go

---

## Features

- **Custom RPC layer** — nodes communicate over raw TCP, with a framing protocol so messages are fully understood
- **Consistent hashing** — keys are spread across nodes, and adding or removing a node only shifts a bit of the data
- **Tunable replication (quorums)** — each write is copied to multiple nodes, and you can determine how many nodes must confirm a read or write
- **Conflict detection (vector clocks)** — if two nodes get conflicting writes for the same key, the system automaticcally detects it
- **Gossip-based failure detection** — nodes discover each other by talking to only one peer at a time

---

## Tech Stack

| Layer       | Technology                         |
| ----------- | ---------------------------------- |
| Language    | Go                                 |
| Networking  | Raw TCP with a custom RPC protocol |
| Data format | JSON                               |

---

## Architecture

- **rpc** — sends and receives messages between nodes over TCP
- **ring** — decides which nodes are responsible for a given key
- **store** — holds the actual key-value data on each node
- **kvnode** — connects the store to the network so other nodes can read/write it
- **coordinator** — performs read or writes across multiple nodes and enforces the quorum rules
- **vclock** — compares two versions of a value to tell if one is newer or if they conflict
- **gossip** — lets nodes find out about each other and automaticcally detects failures

---

## Verified By

- Adding a 5th node to a 4-node cluster reassigned only 16.7% of keys
- Survived a simulated node failure (killed 1 of 3 replicas) and still returned the correct data on read
- Correctly detected 100% of concurrent write conflicts across test runs (no zero silent data loss)
- A node's failure was correctly learned by another node that never contacted it directly (Gossip spreads)

---

## Local Development

```bash
git clone https://github.com/OmarNahhass/distsys-go.git
cd distsys-go
go build ./...
go test ./... -v
```

---

## Author

**Omar Nahhas** — [Portfolio](https://omarnahhas.com) · [LinkedIn](https://www.linkedin.com/in/omar-nahhas-bb1aa4186/)
