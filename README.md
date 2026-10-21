# jobserver-gate

Cooperative concurrency gate for Rust, bridging the [GNU Make jobserver protocol](https://www.gnu.org/software/make/manual/html_node/Job-Slots.html) and thread pools (such as Rayon or `std::thread`) to prevent CPU over-subscription across parent and child processes.

## Why jobserver-gate?

When multiple independent tools or build steps run concurrently, uncoordinated thread pools saturate all CPU cores in each process, causing heavy context switching and high latency.

`jobserver-gate` coordinates concurrency using the GNU jobserver protocol with the **Implicit Token Rule**:
- The running process automatically owns 1 implicit token for its main thread.
- Secondary threads acquire tokens from the shared jobserver pipe.
- When an implicit token is dropped, it is temporarily donated into the pipe to awaken blocked threads without busy polling.
- Once the parallel batch finishes, the donated implicit token is reclaimed, strictly preserving the jobserver pipe token count.

## Usage

```toml
[dependencies]
jobserver-gate = "0.1"
```

```rust
use jobserver_gate::JobServerGate;

// Initialize from inherited environment variables (e.g. CARGO_MAKEFLAGS)
// or fall back to an in-process pool sized to available CPU cores:
let gate = unsafe { JobServerGate::from_env_or_limit(None)? };

let files = vec!["doc1.txt", "doc2.txt", "doc3.txt"];

std::thread::scope(|s| {
    for file in files {
        let gate = &gate;
        s.spawn(move || {
            let _token = gate.acquire().unwrap();
            // Process file cooperatively...
        });
    }
});

// Reclaim the implicit token after parallel jobs complete:
gate.reclaim_implicit_token();
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
