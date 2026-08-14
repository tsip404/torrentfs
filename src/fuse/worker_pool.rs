//! Bounded download worker pool (TSI-2144).
//!
//! Replaces the unbounded per-read `thread::spawn` with a fixed pool of
//! workers fed by a bounded `sync_channel`. `try_submit` is non-blocking:
//! when the queue is full — or the pool is shutting down — the job is
//! returned to the caller so the FUSE adapter can apply `EBUSY` backpressure
//! instead of spawning an unbounded thread.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// A unit of work dispatched to the download worker pool.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// Bounded pool of download worker threads.
///
/// The pool owns `workers` threads that pull `Job`s off a bounded
/// `sync_channel` of capacity `queue_depth`. The single sender lives behind a
/// mutex so `shutdown` can drop it (via `take`) and thereby signal the workers
/// to drain the remaining queue and exit.
pub struct WorkerPool {
    tx: Mutex<Option<SyncSender<Job>>>,
    /// Set once `shutdown` begins so `try_submit` can reject new work fast.
    stopping: Arc<AtomicBool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    workers: usize,
    queue_depth: usize,
}

impl WorkerPool {
    /// Spawn `workers` threads fed by a bounded queue of `queue_depth` jobs.
    ///
    /// Panics if either bound is zero.
    pub fn new(workers: usize, queue_depth: usize) -> Arc<Self> {
        assert!(workers > 0, "WorkerPool requires at least one worker");
        assert!(queue_depth > 0, "WorkerPool requires non-zero queue depth");

        let (tx, rx) = sync_channel::<Job>(queue_depth);
        let stopping = Arc::new(AtomicBool::new(false));
        let rx = Arc::new(Mutex::new(rx));

        let mut handles = Vec::with_capacity(workers);
        for i in 0..workers {
            let rx = Arc::clone(&rx);
            let handle = thread::Builder::new()
                .name(format!("download-worker-{i}"))
                .spawn(move || worker_loop(rx))
                .expect("failed to spawn download worker");
            handles.push(handle);
        }

        Arc::new(Self {
            tx: Mutex::new(Some(tx)),
            stopping,
            handles: Mutex::new(handles),
            workers,
            queue_depth,
        })
    }

    /// Number of worker threads in the pool.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Capacity of the bounded submission queue.
    pub fn queue_depth(&self) -> usize {
        self.queue_depth
    }

    /// The pool's stopping flag — shared with worker jobs so an in-flight
    /// `read_file_range_blocking` can observe shutdown and abort its piece
    /// wait instead of blocking the `shutdown` join forever.
    pub fn stopping_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stopping)
    }

    /// Try to enqueue a job without blocking.
    ///
    /// Returns `Err(job)` when the queue is full (backpressure) or the pool is
    /// shutting down.
    pub fn try_submit(&self, job: Job) -> Result<(), Job> {
        if self.stopping.load(Ordering::SeqCst) {
            return Err(job);
        }
        let guard = match self.tx.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(job),
        };
        let tx = match guard.as_ref() {
            Some(tx) => tx,
            None => return Err(job),
        };
        match tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(job)) => Err(job),
            Err(TrySendError::Disconnected(job)) => Err(job),
        }
    }

    /// Graceful shutdown: stop accepting new jobs, drain the queue, and join
    /// every worker. Returns only after all queued jobs have completed.
    ///
    /// Idempotent — subsequent calls are no-ops.
    pub fn shutdown(&self) {
        // 1. Reject new submissions.
        self.stopping.store(true, Ordering::SeqCst);
        // 2. Drop the single sender: workers consume the remaining queued jobs
        //    and then observe `recv() == Disconnected` and exit.
        if let Ok(mut guard) = self.tx.lock() {
            drop(guard.take());
        }
        // 3. Join every worker.
        let handles = {
            let mut guard = match self.handles.lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = handle.join();
        }
    }
}

/// Worker loop: pull jobs until the sender is dropped (queue drained).
fn worker_loop(rx: Arc<Mutex<Receiver<Job>>>) {
    loop {
        // Hold the receiver lock only while dequeuing; run the job outside the
        // lock so sibling workers can dequeue concurrently while this one
        // executes.
        let job = {
            let guard = match rx.lock() {
                Ok(guard) => guard,
                Err(_) => break,
            };
            match guard.recv() {
                Ok(job) => job,
                Err(_) => break, // sender dropped — queue drained
            }
        };
        job();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[test]
    fn queue_is_bounded_with_backpressure() {
        let pool = WorkerPool::new(1, 2);
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        // First job blocks the single worker: signal start, then wait to be
        // released.
        assert!(pool
            .try_submit(Box::new(move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
            }))
            .is_ok());

        // Wait until the worker has dequeued (and is now blocked inside) the
        // first job, so the queue is empty and the worker cannot drain.
        started_rx.recv().unwrap();

        // Fill the queue to capacity.
        for _ in 0..2 {
            assert!(pool.try_submit(Box::new(|| {})).is_ok());
        }
        // The next submit must be rejected: the queue is full.
        assert!(pool.try_submit(Box::new(|| {})).is_err());

        // Release the blocked worker and shut down cleanly.
        let _ = release_tx.send(());
        pool.shutdown();
    }

    #[test]
    fn jobs_are_drained_on_shutdown() {
        let pool = WorkerPool::new(2, 4);
        let ran = Arc::new(AtomicUsize::new(0));
        // Submit exactly `queue_depth` jobs so none are rejected by
        // backpressure: workers execute some immediately, the rest sit in the
        // queue and must be drained by `shutdown`.
        for _ in 0..4 {
            let ran = Arc::clone(&ran);
            assert!(pool
                .try_submit(Box::new(move || {
                    ran.fetch_add(1, Ordering::SeqCst);
                }))
                .is_ok());
        }
        pool.shutdown();
        assert_eq!(ran.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn concurrency_is_bounded_by_worker_count() {
        let pool = WorkerPool::new(2, 8);
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        for _ in 0..8 {
            let running = Arc::clone(&running);
            let peak = Arc::clone(&peak);
            assert!(pool
                .try_submit(Box::new(move || {
                    let cur = running.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(cur, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(10));
                    running.fetch_sub(1, Ordering::SeqCst);
                }))
                .is_ok());
        }
        pool.shutdown();
        // Hard invariant: concurrency never exceeds the worker count.
        assert!(
            peak.load(Ordering::SeqCst) <= 2,
            "concurrency peak {} exceeded worker count",
            peak.load(Ordering::SeqCst)
        );
        assert_eq!(running.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn shutdown_rejects_new_submissions() {
        let pool = WorkerPool::new(2, 4);
        pool.shutdown();
        assert!(pool.try_submit(Box::new(|| {})).is_err());
        // Idempotent.
        pool.shutdown();
    }

    #[test]
    fn shutdown_interrupts_inflight_job() {
        let pool = WorkerPool::new(1, 1);
        let stop = pool.stopping_flag();
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();

        // A job that blocks until the pool's stopping flag is set, simulating
        // an in-flight read observing shutdown and aborting its piece-wait.
        assert!(pool
            .try_submit(Box::new(move || {
                let _ = started_tx.send(());
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }))
            .is_ok());

        // Wait until the worker is inside the job.
        started_rx.recv().unwrap();

        // Shutdown must return promptly: the job observes the stopping flag
        // and exits, so `join` is not blocked by the in-flight job.
        pool.shutdown();
    }
}
