use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use crate::worker::Outcome;

// Upper bounds in seconds, shared by every histogram.
const BUCKETS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
];

// (bucket upper bound, cumulative count), sum in seconds, count.
pub type HistogramSnapshot = (Vec<(f64, u64)>, f64, u64);

// fn_name, priority, succeeded, retried, failed, aborted, run, wait.
pub type TaskSnapshot = (
    String,
    String,
    u64,
    u64,
    u64,
    u64,
    HistogramSnapshot,
    HistogramSnapshot,
);

#[derive(Default)]
struct Histogram {
    counts: [u64; BUCKETS.len()],
    sum: f64,
    count: u64,
}

impl Histogram {
    fn observe(&mut self, secs: f64) {
        let secs = secs.max(0.0);
        if let Some(i) = BUCKETS.iter().position(|&bound| secs <= bound) {
            self.counts[i] += 1;
        }
        self.sum += secs;
        self.count += 1;
    }

    fn snapshot(&self) -> HistogramSnapshot {
        let mut total = 0;
        let buckets = BUCKETS
            .iter()
            .zip(self.counts)
            .map(|(&bound, n)| {
                total += n;
                (bound, total)
            })
            .collect();
        (buckets, self.sum, self.count)
    }
}

#[derive(Default)]
struct TaskStats {
    succeeded: u64,
    retried: u64,
    failed: u64,
    aborted: u64,
    run: Histogram,
    wait: Histogram,
}

#[derive(Default)]
pub struct Metrics {
    tasks: Mutex<HashMap<(String, String), TaskStats>>,
}

impl Metrics {
    pub fn record(
        &self,
        fn_name: &str,
        priority: &str,
        outcome: &Outcome,
        run_secs: f64,
        wait_secs: Option<f64>,
    ) {
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        let stats = tasks
            .entry((fn_name.to_string(), priority.to_string()))
            .or_default();
        match outcome {
            Outcome::Success => stats.succeeded += 1,
            Outcome::Retry { .. } => stats.retried += 1,
            Outcome::Failure | Outcome::Dead => stats.failed += 1,
            Outcome::Aborted => stats.aborted += 1,
        }
        stats.run.observe(run_secs);
        if let Some(wait) = wait_secs {
            stats.wait.observe(wait);
        }
    }

    pub fn snapshot(&self) -> Vec<TaskSnapshot> {
        let tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        let mut out: Vec<TaskSnapshot> = tasks
            .iter()
            .map(|((fn_name, priority), s)| {
                (
                    fn_name.clone(),
                    priority.clone(),
                    s.succeeded,
                    s.retried,
                    s.failed,
                    s.aborted,
                    s.run.snapshot(),
                    s.wait.snapshot(),
                )
            })
            .collect();
        out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        out
    }
}
