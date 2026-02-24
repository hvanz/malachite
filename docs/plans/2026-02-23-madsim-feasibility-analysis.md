# madsim Feasibility Analysis for Malachite DST

## Context

[madsim](https://github.com/madsim-rs/madsim) is a Rust async runtime that enables deterministic simulation testing by replacing tokio with a controlled simulator. This document assesses whether madsim is a practical alternative to Malachite's current hand-rolled DST framework.

## How madsim works

madsim replaces tokio via `cfg(madsim)` conditional compilation. When enabled, `madsim-tokio` simulates some tokio modules and passes others through from real tokio:

| Module | madsim behavior |
|---|---|
| `tokio::net` | **Simulated** — virtual TCP/UDP |
| `tokio::time` | **Simulated** — deterministic clock |
| `tokio::task::spawn` | **Simulated** — deterministic scheduling |
| `tokio::sync` (mpsc, oneshot, broadcast) | **Pass-through** — uses real tokio |
| `tokio::fs` | **Pass-through** (marked "TODO: simulate") |
| `tokio::io`, `tokio::select!`, `join!` | **Pass-through** |

Fault injection (network partitions, delays, message loss) is built in at the IP/port level. Projects integrate by adding `[patch.crates-io]` entries in `Cargo.toml` and building with `RUSTFLAGS="--cfg madsim"`.

## How Malachite uses tokio

The engine layer is deeply coupled to tokio:

- **`tokio::sync::{mpsc, oneshot, broadcast}`** — channels between actors, events, WAL communication
- **`tokio::task::JoinHandle` / `tokio::spawn`** — task management throughout engine and app-channel
- **`tokio::time::{sleep, Instant, interval}`** — timers, ticker, timeouts
- **`tokio::select!`** — network event loop in libp2p integration
- **ractor** actor framework — hardcoded `tokio_runtime` feature, uses tokio internals
- **libp2p** — uses `tokio` feature for the Swarm event loop, TCP/QUIC transports
- **WAL** — uses `std::thread::spawn` with `tokio::sync` channels for OS-thread-based writes

The core layer (`core-*`) has no tokio dependency and several crates are `no_std` compatible.

## Compatibility assessment

### What would work

- `tokio::sync::*` — passed through unchanged
- `tokio::select!`, `join!` — passed through
- `tokio::time::*` — simulated, gives deterministic clock for free
- `tokio::task::spawn` — simulated, deterministic scheduling

### What would NOT work

1. **ractor** — No `madsim-ractor` crate exists. ractor uses tokio internals directly (runtime handle, thread-local storage). Creating a madsim shim for ractor would be a significant project.

2. **libp2p** — No `madsim-libp2p` crate exists. libp2p has its own transport abstraction (`libp2p-tcp`, `libp2p-quic`) that wraps tokio's `TcpStream`/`TcpListener`. Even with madsim's simulated `tokio::net`, libp2p's transport layer would need a dedicated shim. A simulated network layer would still be needed — which we already have.

3. **`std::thread::spawn`** — The WAL in `engine/src/wal/thread.rs` uses OS threads, which madsim does not intercept.

## Comparison

### Pros of madsim over current approach

| Aspect | madsim | Current DST |
|---|---|---|
| Time control | Automatic — `tokio::time` is deterministic | Manual — tick-based clock in `SimulationController` |
| Task scheduling | Deterministic by default | Relies on `tokio::time::sleep` + `start_paused` + `yield_now` |
| Fault injection | Built-in network partition/delay/loss at IP level | Hand-rolled in `SimulationController` |
| Code changes | `Cargo.toml` patches + `cfg(madsim)` | Separate `SimulatedNetwork`, `SimulatedWal` actors |
| Test realism | Higher — runs more of the real stack | Lower — replaces network and WAL entirely |

### Cons of madsim over current approach

| Aspect | madsim | Current DST |
|---|---|---|
| ractor compatibility | Unknown/risky — no `madsim-ractor` | No issue — ractor runs normally |
| libp2p compatibility | No `madsim-libp2p` — still needs simulated network | Already solved with `SimulatedNetwork` |
| Integration effort | Large — patch transitive deps, debug incompatibilities | Already working |
| Control granularity | Coarser — fault injection at IP/port level | Finer — fault injection at message/node level |
| Maintenance burden | Tied to madsim release cadence; patched crates may lag | Self-contained, no external dependency |
| `std::thread` | Not intercepted — WAL thread invisible to simulator | WAL is fully simulated |
| Debugging | Harder — simulation runtime adds indirection | Straightforward actor code |

## Conclusion

**madsim is not a practical fit for Malachite's current architecture.** The two main blockers are ractor and libp2p — neither has a madsim shim, and both are deeply embedded in the engine.

The current DST approach is well-suited to Malachite's three-layer design. The core (`core-*`) is pure and `no_std`-capable. The engine uses ractor actors with clear message boundaries. The DST framework exploits those boundaries by swapping the network and WAL actors with simulated versions — exactly the seam the architecture provides. madsim would intercept at a lower level (the tokio runtime), but Malachite's real I/O boundary is at the actor level, not the socket level.

### If madsim-style benefits are desired

The most impactful improvement would be to use `tokio::time::pause()` more aggressively (the determinism test already uses `start_paused`) and potentially replace the manual tick loop with tokio's paused-time `sleep` + `advance` APIs. But this is an optimization — the current system already achieves determinism across 50 seeds.
