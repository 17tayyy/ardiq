use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use redis::aio::{ConnectionManager, MultiplexedConnection};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::queue::{now_ms, Queue, ResultTtl, StreamMessage};

#[derive(Debug)]
pub enum Outcome {
    Success,
    Failure,
    Retry { delay_ms: Option<i64> },
}

#[derive(Debug)]
pub struct ExecOutcome {
    pub outcome: Outcome,
    pub result: Vec<u8>,
}

#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(&self, task_id: String, payload: Vec<u8>, tries: i64) -> ExecOutcome;
}

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub concurrency: usize,
    pub prefetch: i64,
    pub idle_timeout_ms: i64,
    pub poll_block_ms: i64,
    pub result_ttl_ms: i64,
    pub burst: bool,
}

#[derive(Clone)]
pub struct Worker {
    queue: Arc<Queue>,
    executor: Arc<dyn TaskExecutor>,
    client: redis::Client,
    config: WorkerConfig,
    cancel: CancellationToken,
}

#[derive(Clone)]
struct RunState {
    permits: Arc<AtomicI64>,
    in_flight: Arc<Mutex<HashMap<String, (String, String)>>>,
}

impl Worker {
    pub fn new(
        client: redis::Client,
        queue: Arc<Queue>,
        config: WorkerConfig,
        executor: Arc<dyn TaskExecutor>,
        cancel: CancellationToken,
    ) -> Self {
        Worker {
            queue,
            executor,
            client,
            config,
            cancel,
        }
    }

    pub async fn run(&self) -> redis::RedisResult<()> {
        let producer_cfg = redis::AsyncConnectionConfig::new().set_response_timeout(None);
        let mut producer_conn = self
            .client
            .get_multiplexed_async_connection_with_config(&producer_cfg)
            .await?;
        let shared = ConnectionManager::new(self.client.clone()).await?;

        self.queue.create_groups(&mut producer_conn).await?;

        tracing::info!(
            worker_id = %self.config.worker_id,
            concurrency = self.config.concurrency,
            prefetch = self.config.prefetch,
            burst = self.config.burst,
            "ardiq worker started"
        );

        let state = RunState {
            permits: Arc::new(AtomicI64::new(self.config.prefetch)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        };
        let (tx, rx) =
            async_channel::bounded::<StreamMessage>(self.config.prefetch.max(1) as usize);

        let mut handles = Vec::new();

        for _ in 0..self.config.concurrency.max(1) {
            let worker = self.clone();
            let state = state.clone();
            let rx = rx.clone();
            let conn = shared.clone();
            handles.push(tokio::spawn(async move {
                worker.consumer(rx, state, conn).await;
            }));
        }
        drop(rx);

        {
            let worker = self.clone();
            let state = state.clone();
            let conn = shared.clone();
            handles.push(tokio::spawn(async move {
                worker.heartbeat(state, conn).await;
            }));
        }

        {
            let worker = self.clone();
            let state = state.clone();
            handles.push(tokio::spawn(async move {
                if let Err(err) = worker.producer(tx, state, producer_conn).await {
                    tracing::error!("ardiq producer stopped: {err}");
                }
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        tracing::info!(
            worker_id = %self.config.worker_id,
            "ardiq worker stopped"
        );

        Ok(())
    }

    async fn producer(
        &self,
        tx: Sender<StreamMessage>,
        state: RunState,
        mut conn: MultiplexedConnection,
    ) -> redis::RedisResult<()> {
        let worker_id = &self.config.worker_id;
        while !self.cancel.is_cancelled() {
            let count = state.permits.load(Ordering::Acquire).max(0);

            let mut messages = Vec::new();
            if count > 0 {
                messages = self
                    .queue
                    .read_batch(&mut conn, worker_id, count, self.config.idle_timeout_ms)
                    .await?;
                if messages.is_empty() {
                    messages = self
                        .queue
                        .read_blocking(&mut conn, worker_id, count, self.config.poll_block_ms)
                        .await?;
                }
            } else {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }

            for msg in &messages {
                state.permits.fetch_sub(1, Ordering::AcqRel);
                if tx.send(msg.clone()).await.is_err() {
                    return Ok(());
                }
            }

            let promoted = self.queue.publish_delayed(&mut conn, now_ms()).await?;

            if self.config.burst
                && messages.is_empty()
                && promoted == 0
                && state.permits.load(Ordering::Acquire) >= self.config.prefetch
            {
                self.cancel.cancel();
                break;
            }
        }
        Ok(())
    }

    async fn consumer(
        &self,
        rx: Receiver<StreamMessage>,
        state: RunState,
        mut conn: ConnectionManager,
    ) {
        loop {
            let msg = tokio::select! {
                _ = self.cancel.cancelled() => break,
                msg = rx.recv() => match msg {
                    Ok(msg) => msg,
                    Err(_) => break,
                },
            };
            self.run_task(msg, &state, &mut conn).await;
            state.permits.fetch_add(1, Ordering::AcqRel); // free a slot for the producer
        }
    }

    async fn run_task(&self, msg: StreamMessage, state: &RunState, conn: &mut ConnectionManager) {
        let task_id = msg.task_id.clone();

        let tries = match self.queue.incr_retry(conn, &task_id).await {
            Ok(n) => n,
            Err(err) => {
                tracing::error!("ardiq incr_retry failed for {task_id}: {err}");
                return;
            }
        };
        if let Err(err) = self.queue.mark_running(conn, &task_id).await {
            tracing::error!("ardiq mark_running failed for {task_id}: {err}");
        }
        state.in_flight.lock().await.insert(
            task_id.clone(),
            (msg.priority.clone(), msg.message_id.clone()),
        );

        if let Err(err) = self.execute_and_finish(&msg, tries, conn).await {
            tracing::error!("ardiq failed to finalize {task_id}: {err}");
        }

        state.in_flight.lock().await.remove(&task_id);
    }

    async fn execute_and_finish(
        &self,
        msg: &StreamMessage,
        tries: i64,
        conn: &mut ConnectionManager,
    ) -> redis::RedisResult<()> {
        let payload = match self.queue.fetch_payload(conn, &msg.task_id).await? {
            Some(payload) => payload,
            None => {
                tracing::warn!("ardiq task {} expired before running", msg.task_id);
                return self.queue.discard(conn, msg).await;
            }
        };

        let exec = self
            .executor
            .execute(msg.task_id.clone(), payload, tries)
            .await;

        match exec.outcome {
            Outcome::Success | Outcome::Failure => {
                let ttl = ResultTtl::from_ms(self.config.result_ttl_ms);
                self.queue
                    .complete(conn, msg, &exec.result, ttl, now_ms())
                    .await
            }
            Outcome::Retry { delay_ms } => {
                let delay = delay_ms.unwrap_or_else(|| tries * tries * 1000);
                self.queue.retry_later(conn, msg, now_ms() + delay).await
            }
        }
    }

    async fn heartbeat(&self, state: RunState, mut conn: ConnectionManager) {
        let interval = Duration::from_millis(((self.config.idle_timeout_ms as f64) * 0.9) as u64);
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {}
            }

            let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
            for (priority, message_id) in state.in_flight.lock().await.values() {
                grouped
                    .entry(priority.clone())
                    .or_default()
                    .push(message_id.clone());
            }

            let running: usize = grouped.values().map(Vec::len).sum();
            let status = format!("{} running={running}", self.config.worker_id);
            if let Err(err) = self
                .queue
                .heartbeat(
                    &mut conn,
                    &self.config.worker_id,
                    &status,
                    self.config.idle_timeout_ms,
                    &grouped,
                    now_ms(),
                )
                .await
            {
                tracing::warn!("ardiq heartbeat failed: {err}");
            }
        }
    }
}
