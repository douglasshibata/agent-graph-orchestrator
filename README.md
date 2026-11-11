# ⚙️ Low-Level Go Agent Graph Orchestrator & Ephemeral Firecracker Sandbox

[![Go Version](https://img.shields.io/badge/go-1.24%2B-blue.svg)](https://golang.org)
[![Firecracker](https://img.shields.io/badge/microVM-Firecracker-orange.svg)](https://firecracker-microvm.github.io/)
[![Isolation](https://img.shields.io/badge/security-AF__VSOCK%20%7C%20cgroups%20v2-green.svg)](#)
[![License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](#)

A high-performance, deterministic **Agent Graph Orchestrator State Engine** and **Ephemeral Tool Execution Sandbox Runner** written in Go 1.24+. It manages Directed Acyclic Graphs (DAGs) representing agent actions (LLM inference, RAG retrieval, dynamic condition branching, and human-in-the-loop checkpoints) with bounded concurrency, isolated step errors, token budgets, state checkpointing, and hardware-virtualized tool sandboxes using minimal Firecracker microVMs.

---

## 🏛️ Architecture Overview

```text
       [ Client Request / Event Trigger ]
                       │
                       ▼
         ┌───────────────────────────┐
         │ Engine.Execute(ctx, task) │
         └─────────────┬─────────────┘
                       │
         ┌─────────────▼─────────────┐
         │ Topological DAG Analyzer  │
         │ (In-Degree Dependency Map)│
         └─────────────┬─────────────┘
                       │
     ┌─────────────────┴─────────────────┐
     ▼ (goroutine worker)                ▼ (goroutine worker)
┌──────────────┐                  ┌──────────────┐
│  Node A (LLM)│                  │ Node B (RAG) │
└──────┬───────┘                  └──────┬───────┘
       │                                 │
       └────────────────┬────────────────┘
                        ▼
       ┌─────────────────────────────────┐
       │     Atomic State Reducer &      │
       │     Checkpointer (NATS/PG)      │
       └────────────────┬────────────────┘
                        ▼
       ┌─────────────────────────────────┐
       │   Conditional Dynamic Branch    │
       │    (Evaluates Output State)     │
       └────────────────┬────────────────┘
                        ▼
       ┌─────────────────────────────────┐
       │      Node C (Tool Sandbox)      │
       └─────────────────────────────────┘
```

### Subsystem 1: Low-Level Go Agent Graph Orchestrator State Engine
- **Topological Sorting & Cycle Detection**: Pre-execution verification using Kahn's algorithm (`ValidateDAG`).
- **Dynamic Resolution Loop**: Atomic notification broadcast channels dispatching unblocked dependent nodes as in-degrees reach zero.
- **Concurrency Throttling**: Semaphore-guarded worker pools enforcing concurrency bounds.
- **Deterministic Token Budgeting**: Atomic token usage tracking with `ErrBudgetExceeded` fail-fast guarantees.
- **Pluggable State Checkpointing**: Snapshot serialization committed after every discrete node transition.
- **Conditional Branching & Cascade Skips**: Dynamic evaluation of task state to prune unneeded branches or cascade cancellations upon upstream step failures.

---

### Subsystem 2: Ephemeral Tool Execution Sandbox Runner (Firecracker / gVisor)

```text
┌────────────────────────────────────────────────────────┐
│ Host Operating System (Bare Metal or KVM-Enabled VM)   │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Go Sandbox Controller Manager                    │  │
│  │ - Spawns Jailer Wrapper Process                  │  │
│  │ - Configures Linux TAP device & IPAM             │  │
│  │ - Pushes JSON config via UNIX domain socket      │  │
│  └────────────────────────┬─────────────────────────┘  │
│                           │                            │
│                           ▼                            │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Firecracker Jailer chroot / cgroup v2            │  │
│  │ UID: 10053 | GID: 10053                          │  │
│  │ Read-only Base rootfs (ext4) + tmpfs overlay     │  │
│  │ ┌──────────────────────────────────────────────┐ │  │
│  │ │ MicroVM (Linux Kernel 6.1-minimal)           │ │  │
│  │ │ - Memory: 256MB | vCPUs: 1                   │ │  │
│  │ │ - Guest Agent listening on VSOCK :5252       │ │  │
│  │ │ - Hard Egress Policy via host iptables       │ │  │
│  │ └──────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

- **Jailer Chroot & cgroups v2**: Complete process sandboxing with unshared namespaces (`pid`, `net`, `ipc`, `uts`, `mount`), memory caps (256MB), and CPU quotas (0.5 core).
- **Socket REST API Client**: Direct control of the Firecracker daemon (`/boot-source`, `/drives/rootfs`, `/machine-config`, `/network-interfaces/net1`, `/actions`).
- **AF_VSOCK Communication**: VirtIO Socket multiplexing on port `5252` directly to an initrd-based guest agent, eliminating TCP network overhead for code execution.
- **Egress Firewall Rules**: Automated Linux TAP configuration and iptables firewall rules dropping RFC 1918 internal IP ranges and AWS/GCP cloud metadata (`169.254.169.254/32`).

---

## ⚖️ Lifecycle Comparison: Firecracker MicroVM vs. gVisor (runsc)

| Dimension | Firecracker MicroVM | gVisor (runsc OCI) |
|---|---|---|
| **Isolation Boundary** | Hardware-assisted virtualization (KVM). Independent guest kernel (`vmlinux`). | System call interception in userspace (Go-based application kernel, Sentry). |
| **Startup Overhead** | 5ms to 12ms (with pre-allocated base snapshots). | 50ms to 100ms (standard container startup via containerd). |
| **Memory Footprint** | ≈ 5MB base overhead + allocated guest RAM (256MB). | ≈ 15MB to 30MB Sentry monitor overhead. |
| **Kernel Compatibility**| Full Linux compatibility (standard uncompressed ELF kernel). | Implements subset of Linux syscalls (≈ 350 syscalls; fails on obscure ioctls). |
| **Production Target** | Ideal for untrusted arbitrary multi-tenant code (Python/Bash). | Ideal for high-density microservices and pre-built binaries. |

---

## 📁 Repository Structure

```text
agent-graph-orchestrator/
├── cmd/
│   └── orchestrator/
│       └── main.go              # CLI demo & HTTP REST Server
├── internal/
│   └── integration/
│       └── runner.go            # Step adapter connecting DAG to Sandbox
├── pkg/
│   ├── orchestrator/
│   │   ├── engine.go            # Kahn DAG validator & async scheduler
│   │   ├── state.go             # Thread-safe TaskState & Checkpointer
│   │   └── types.go             # Node, Status, and Error definitions
│   └── sandbox/
│       ├── firecracker.go       # Jailer & Firecracker REST/VSOCK client
│       ├── mock_server.go       # Mock Unix & VSOCK emulator for hermetic testing
│       ├── network.go           # Linux TAP & iptables egress firewall rules
│       └── types.go             # Sandbox request/response contracts
├── tests/
│   ├── e2e_test.go              # Full Agent DAG + Sandbox E2E test
│   ├── orchestrator_test.go     # Cycle detection, parallel execution, retries
│   └── sandbox_test.go          # REST payloads, VSOCK handshake, iptables rules
├── Dockerfile                   # Alpine multi-stage container
├── docker-compose.yml           # Compose V2 specification
├── README.md                    # English architecture & usage guide
└── TUTORIAL.md                  # Portuguese step-by-step tutorial
```

---

## 🚀 Quickstart & Execution

### 1. Run Tests (Race Detector Enabled)
```bash
cd agent-graph-orchestrator
go test -v -race ./...
```

### 2. Run CLI DAG Demonstration
```bash
go run ./cmd/orchestrator/main.go -mode=cli
```

### 3. Run REST Daemon
```bash
go run ./cmd/orchestrator/main.go -mode=server -port=8080
```

Trigger an execution via `curl`:
```bash
curl -X POST http://localhost:8080/api/workflow/run \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "workflow-100",
    "tenant_id": "tenant-alpha",
    "token_budget": 1500,
    "language": "python",
    "code_snippet": "print([x**2 for x in range(5)])"
  }'
```
