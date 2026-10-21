//! Cooperative concurrency gate for Rust, bridging the GNU Jobserver protocol
//! and thread pools (such as Rayon or `std::thread`).
//!
//! # Problem
//!
//! When multiple tools run concurrently (e.g. build systems, test runners, or
//! linters), spawning an uncoordinated thread pool in each process leads to
//! **CPU over-subscription** (severe thread contention, latency spikes, and wasted
//! context switching).
//!
//! GNU Make jobserver solves this by passing a fixed token budget (via pipes or
//! semaphores) to child processes.
//!
//! # The Implicit Token Rule
//!
//! A running process already owns one *implicit* token representing its execution
//! right. [`JobServerGate`] coordinates token acquisition:
//! 1. One caller at a time claims the implicit token via an atomic flag.
//! 2. Subsequent concurrent callers block acquiring a token from the jobserver pipe.
//! 3. When the implicit token is dropped after completing a unit of work, it is
//!    donated into the pipe so that blocked threads wake up immediately.
//! 4. Once a parallel phase finishes (e.g. `par_iter`), [`JobServerGate::reclaim_implicit_token`]
//!    reclaims the donated token without releasing it back, strictly preserving the
//!    pipe token count.
//!
//! # Example: Rayon parallel processing
//!
//! ```no_run
//! use jobserver_gate::{JobServerGate, JobToken};
//!
//! let gate = unsafe { JobServerGate::from_env_or_limit(None).unwrap() };
//!
//! let files = vec!["a.txt", "b.txt", "c.txt"];
//!
//! // In a parallel loop:
//! std::thread::scope(|s| {
//!     for file in files {
//!         let gate_ref = &gate;
//!         s.spawn(move || {
//!             let _token = gate_ref.acquire().unwrap();
//!             // ... process file ...
//!         });
//!     }
//! });
//!
//! // After parallel tasks join, reclaim the donated implicit token:
//! gate.reclaim_implicit_token();
//! ```

use std::io;
use std::process::Command;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

pub use jobserver::{Acquired, Client};

/// The jobserver gate governing parallel task execution.
pub struct JobServerGate {
    client: Client,
    implicit_token_held: AtomicBool,
}

/// Shorthand alias for [`JobServerGate`].
pub type Gate = JobServerGate;

impl JobServerGate {
    /// Create a new gate wrapping an existing [`Client`].
    pub fn new(client: Client) -> Self {
        Self {
            client,
            implicit_token_held: AtomicBool::new(false),
        }
    }

    /// Access the underlying [`Client`].
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Configure a child [`Command`] to inherit this jobserver.
    pub fn configure(&self, cmd: &mut Command) {
        self.client.configure(cmd);
    }

    /// Initialize a gate from environment variables if present, or create an
    /// in-process fallback jobserver client with `fallback_limit` tokens.
    ///
    /// If `fallback_limit` is `None`, defaults to `std::thread::available_parallelism()`.
    ///
    /// # Safety
    /// This function reads and takes ownership of file descriptors specified in
    /// environment variables (such as `CARGO_MAKEFLAGS`). It should be called once,
    /// early in `main` before any other descriptors are opened or background threads
    /// are spawned.
    pub unsafe fn from_env_or_limit(fallback_limit: Option<usize>) -> io::Result<Self> {
        let from_env = unsafe { Client::from_env_ext(true) };
        let client = match from_env.client {
            Ok(client) => client,
            Err(err) => {
                if let Some((var_name, var_val)) = from_env.var
                    && !matches!(
                        err.kind(),
                        jobserver::FromEnvErrorKind::NoJobserver
                            | jobserver::FromEnvErrorKind::NoEnvVar
                    )
                {
                    eprintln!(
                        "warning: failed to connect to jobserver from environment variable `{}=\"{}\"`: {}",
                        var_name,
                        var_val.to_string_lossy(),
                        err
                    );
                }
                let limit = fallback_limit.unwrap_or_else(|| {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4)
                });
                let pipe_tokens = limit.saturating_sub(1);
                match Client::new(pipe_tokens) {
                    Ok(client) => client,
                    Err(err) => {
                        eprintln!("warning: failed to create in-process jobserver: {err}");
                        return Err(err);
                    }
                }
            }
        };
        Ok(Self::new(client))
    }

    /// Acquire a concurrency token, blocking until one is available.
    pub fn acquire(&self) -> io::Result<JobToken<'_>> {
        if self
            .implicit_token_held
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Ok(JobToken::Implicit(self))
        } else {
            let acquired = self.client.acquire()?;
            Ok(JobToken::Acquired(acquired))
        }
    }

    /// Reclaim the donated implicit token after parallel jobs have completed.
    ///
    /// When the implicit token is released, it is donated into the jobserver
    /// pipe so that blocked threads wake up. Once all jobs have joined, this
    /// reclaims that single token without releasing it back, restoring the
    /// pipe to its original token count and resetting `implicit_token_held`.
    pub fn reclaim_implicit_token(&self) {
        if self.implicit_token_held.load(Ordering::Acquire) {
            if let Ok(acquired) = self.client.acquire() {
                acquired.drop_without_releasing();
            }
            self.implicit_token_held.store(false, Ordering::Release);
        }
    }
}

/// An RAII guard representing a held token.
///
/// Dropping this token releases it back to the gate or the jobserver pipe.
pub enum JobToken<'a> {
    Implicit(&'a JobServerGate),
    Acquired(Acquired),
}

impl Drop for JobToken<'_> {
    fn drop(&mut self) {
        if let JobToken::Implicit(gate) = self {
            let _ = gate.client.release_raw();
        }
    }
}

static GLOBAL_GATE: OnceLock<JobServerGate> = OnceLock::new();

/// Set the process-wide global jobserver gate.
pub fn init_global(gate: JobServerGate) -> Result<(), JobServerGate> {
    GLOBAL_GATE.set(gate)
}

/// Initialize the global gate from environment variables or a fallback limit.
///
/// # Safety
/// See [`JobServerGate::from_env_or_limit`].
pub unsafe fn init_global_from_env_or_limit(fallback_limit: Option<usize>) -> io::Result<()> {
    let gate = unsafe { JobServerGate::from_env_or_limit(fallback_limit)? };
    let _ = init_global(gate);
    Ok(())
}

/// Returns a reference to the process-wide global gate, if initialized.
pub fn global() -> Option<&'static JobServerGate> {
    GLOBAL_GATE.get()
}

/// Acquire a token from the global gate, if initialized.
pub fn acquire_global() -> Option<JobToken<'static>> {
    GLOBAL_GATE.get().and_then(|g| g.acquire().ok())
}

/// Reclaim the implicit token from the global gate, if initialized.
pub fn reclaim_global() {
    if let Some(gate) = GLOBAL_GATE.get() {
        gate.reclaim_implicit_token();
    }
}

/// Initialize the process-wide global gate from environment variables or a fallback limit.
///
/// If connecting to or creating the jobserver fails, this emits any appropriate
/// warnings to stderr and proceeds without initializing a global gate.
pub fn init_from_env() {
    let _ = unsafe { init_global_from_env_or_limit(None) };
}

/// Acquire a token from the global gate, blocking until one is available.
///
/// If token acquisition fails or no gate is initialized, this returns `None`.
pub fn acquire() -> Option<JobToken<'static>> {
    acquire_global()
}

/// Reclaim the donated implicit token from the global gate after a parallel phase.
pub fn reclaim_implicit_token() {
    reclaim_global();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn gate_concurrency_ceiling() {
        for pipe_tokens in [0, 1, 3] {
            let total_capacity = pipe_tokens + 1;
            let client = Client::new(pipe_tokens).unwrap();
            let gate = Arc::new(JobServerGate::new(client));

            let current_active = Arc::new(AtomicUsize::new(0));
            let max_observed = Arc::new(AtomicUsize::new(0));

            thread::scope(|s| {
                for _ in 0..10 {
                    let gate = Arc::clone(&gate);
                    let current = Arc::clone(&current_active);
                    let max_active = Arc::clone(&max_observed);

                    s.spawn(move || {
                        let _token = gate.acquire().unwrap();
                        let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(active, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(20));
                        current.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            });

            gate.reclaim_implicit_token();
            let peak = max_observed.load(Ordering::SeqCst);
            assert_eq!(
                peak, total_capacity,
                "for capacity {total_capacity}, peak observed was {peak}"
            );
            assert_eq!(gate.client().available().unwrap_or(0), pipe_tokens);
        }
    }
}
