mod queue;
mod worker;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_async_runtimes::TaskLocals;
use redis::aio::ConnectionManager;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::queue::{now_ms, Queue, ResultTtl, StagedTask};
use crate::worker::{ExecOutcome, Outcome, TaskExecutor, Worker, WorkerConfig};

const DEFAULT_ABORT_MARKER_MS: i64 = 300_000;

type EnqueueItem = (String, Vec<u8>, Option<String>, i64, i64, i64, bool);

create_exception!(
    ardiq,
    ArdiqError,
    PyRuntimeError,
    "Base class for the errors ArdiQ's core raises."
);
create_exception!(
    ardiq,
    BrokerError,
    ArdiqError,
    "Redis was unreachable, refused the connection, or dropped it."
);

struct PyExecutor {
    callback: Py<PyAny>,
    locals: TaskLocals,
}

#[async_trait::async_trait]
impl TaskExecutor for PyExecutor {
    async fn execute(
        &self,
        task_id: String,
        payload: Vec<u8>,
        tries: i64,
        aborted: bool,
    ) -> ExecOutcome {
        let future = Python::attach(|py| -> PyResult<_> {
            let bytes = PyBytes::new(py, &payload);
            let coro = self
                .callback
                .bind(py)
                .call1((task_id, bytes, tries, aborted))?;
            pyo3_async_runtimes::into_future_with_locals(&self.locals, coro)
        });
        let future = match future {
            Ok(future) => future,
            Err(err) => {
                tracing::error!("ardiq executor could not start task: {err}");
                return failure();
            }
        };

        match future.await {
            Ok(obj) => Python::attach(|py| parse_outcome(py, &obj)).unwrap_or_else(|err| {
                tracing::error!("ardiq executor returned an unreadable result: {err}");
                failure()
            }),
            Err(err) => {
                tracing::error!("ardiq task raised: {err}");
                failure()
            }
        }
    }
}

fn parse_outcome(py: Python<'_>, obj: &Py<PyAny>) -> PyResult<ExecOutcome> {
    let bound = obj.bind(py);
    let code: i64 = bound.get_item(0)?.extract()?;
    let result: Vec<u8> = bound.get_item(1)?.extract()?;
    let retry_after_ms: i64 = bound.get_item(2)?.extract()?;
    let outcome = match code {
        0 => Outcome::Success,
        2 => Outcome::Retry {
            delay_ms: (retry_after_ms > 0).then_some(retry_after_ms),
        },
        _ => Outcome::Failure,
    };
    Ok(ExecOutcome { outcome, result })
}

fn failure() -> ExecOutcome {
    ExecOutcome {
        outcome: Outcome::Failure,
        result: Vec::new(),
    }
}

#[pyclass]
struct ArdiqCore {
    client: redis::Client,
    #[pyo3(get)]
    redis_url: String,
    #[pyo3(get)]
    queue_name: String,
    #[pyo3(get)]
    priorities: Vec<String>,
    queue: Arc<Queue>,
    config: WorkerConfig,
    cancel: CancellationToken,
    conn: Arc<OnceCell<ConnectionManager>>,
    aborts: Arc<OnceCell<async_channel::Receiver<String>>>,
}

#[pymethods]
impl ArdiqCore {
    #[new]
    fn new(config: Bound<'_, PyDict>) -> PyResult<Self> {
        let redis_url: String = opt(&config, "redis_url")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_else(|| "redis://localhost:6379".to_string());
        let queue_name: String = opt(&config, "queue_name")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_else(|| "default".to_string());
        let priorities: Vec<String> = opt(&config, "priorities")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_else(|| vec!["default".to_string()]);
        let queue_priorities: Vec<String> = priorities.iter().rev().cloned().collect();

        let concurrency: usize = opt(&config, "concurrency")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(16);
        let prefetch: i64 = opt(&config, "prefetch")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or((concurrency as i64) * 2)
            .max(1);
        let idle_timeout_ms: i64 = opt(&config, "idle_timeout_ms")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(60_000);
        let poll_block_ms: i64 = opt(&config, "poll_block_ms")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(500);
        let result_ttl_ms: i64 = opt(&config, "result_ttl_ms")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(300_000);
        let burst: bool = opt(&config, "burst")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(false);
        let worker_id: String = opt(&config, "worker_id")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_else(default_worker_id);

        let client = redis::Client::open(redis_url.as_str()).map_err(to_py_err)?;
        let queue = Arc::new(Queue::new(&queue_name, queue_priorities));
        let worker_config = WorkerConfig {
            worker_id,
            concurrency,
            prefetch,
            idle_timeout_ms,
            poll_block_ms,
            result_ttl_ms,
            burst,
        };

        Ok(ArdiqCore {
            client,
            redis_url,
            queue_name,
            priorities,
            queue,
            config: worker_config,
            cancel: CancellationToken::new(),
            conn: Arc::new(OnceCell::new()),
            aborts: Arc::new(OnceCell::new()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (task_id, payload, priority=None, delay_ms=0, schedule_ms=0, expire_ms=0, reset_result=false))]
    fn enqueue<'py>(
        &self,
        py: Python<'py>,
        task_id: String,
        payload: Vec<u8>,
        priority: Option<String>,
        delay_ms: i64,
        schedule_ms: i64,
        expire_ms: i64,
        reset_result: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let priority = priority.unwrap_or_else(|| queue.default_priority().to_string());
        let conn = self.conn.clone();
        let client = self.client.clone();

        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;
            let now = now_ms();
            let score = if schedule_ms > 0 {
                schedule_ms
            } else if delay_ms > 0 {
                now + delay_ms
            } else {
                0
            };
            let queued = queue
                .enqueue(
                    &mut conn,
                    &task_id,
                    &payload,
                    &priority,
                    score,
                    expire_ms,
                    now,
                    reset_result,
                )
                .await
                .map_err(to_py_err)?;
            Ok(queued)
        })
    }

    fn enqueue_many<'py>(
        &self,
        py: Python<'py>,
        items: Vec<EnqueueItem>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();

        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;
            let now = now_ms();
            let staged: Vec<StagedTask> = items
                .into_iter()
                .map(
                    |(
                        task_id,
                        payload,
                        priority,
                        delay_ms,
                        schedule_ms,
                        expire_ms,
                        reset_result,
                    )| {
                        StagedTask {
                            task_id,
                            payload,
                            priority: priority
                                .unwrap_or_else(|| queue.default_priority().to_string()),
                            score_ms: if schedule_ms > 0 {
                                schedule_ms
                            } else if delay_ms > 0 {
                                now + delay_ms
                            } else {
                                0
                            },
                            expire_ms,
                            reset_result,
                        }
                    },
                )
                .collect();
            let queued = queue
                .enqueue_many(&mut conn, &staged, now)
                .await
                .map_err(to_py_err)?;
            Ok(queued)
        })
    }

    fn run<'py>(&self, py: Python<'py>, callback: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;
        let executor = Arc::new(PyExecutor { callback, locals });
        let worker = Worker::new(
            self.client.clone(),
            self.queue.clone(),
            self.config.clone(),
            executor,
            self.cancel.clone(),
        );
        future_into_py(py, async move {
            worker.run().await.map_err(to_py_err)?;
            Ok(())
        })
    }

    fn stop(&self) {
        self.cancel.cancel();
    }

    #[getter]
    fn burst(&self) -> bool {
        self.config.burst
    }

    #[setter]
    fn set_burst(&mut self, value: bool) {
        self.config.burst = value;
    }

    fn queue_size<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;
            let size = queue.queue_size(&mut conn).await.map_err(to_py_err)?;
            Ok(size)
        })
    }

    fn result<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;
            let raw = queue
                .fetch_result(&mut conn, &task_id)
                .await
                .map_err(to_py_err)?;
            Python::attach(|py| -> PyResult<Py<PyAny>> {
                Ok(match raw {
                    Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                    None => py.None(),
                })
            })
        })
    }

    #[pyo3(signature = (task_id, timeout_ms))]
    fn await_result<'py>(
        &self,
        py: Python<'py>,
        task_id: String,
        timeout_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            let mut pubsub = client.get_async_pubsub().await.map_err(to_py_err)?;
            pubsub
                .subscribe(queue.result_channel(&task_id))
                .await
                .map_err(to_py_err)?;

            let mut shared = shared_conn(&conn, &client).await?;
            let raw = match queue
                .fetch_result(&mut shared, &task_id)
                .await
                .map_err(to_py_err)?
            {
                Some(bytes) => Some(bytes),
                None => {
                    let dur = std::time::Duration::from_millis(timeout_ms);
                    let _ = tokio::time::timeout(dur, pubsub.on_message().next()).await;
                    queue
                        .fetch_result(&mut shared, &task_id)
                        .await
                        .map_err(to_py_err)?
                }
            };

            Python::attach(|py| -> PyResult<Py<PyAny>> {
                Ok(match raw {
                    Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                    None => py.None(),
                })
            })
        })
    }

    #[pyo3(signature = (task_id, result))]
    fn abort<'py>(
        &self,
        py: Python<'py>,
        task_id: String,
        result: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();
        let result_ttl_ms = self.config.result_ttl_ms;
        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;

            let marker_ttl_ms = if result_ttl_ms > 0 {
                result_ttl_ms
            } else {
                DEFAULT_ABORT_MARKER_MS
            };
            queue
                .abort(
                    &mut conn,
                    &task_id,
                    &result,
                    ResultTtl::from_ms(result_ttl_ms),
                    marker_ttl_ms,
                    now_ms(),
                )
                .await
                .map_err(to_py_err)
        })
    }

    #[pyo3(signature = (timeout_ms))]
    fn next_abort<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let aborts = self.aborts.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            let rx = abort_stream(&aborts, &client, queue.abort_channel()).await?;
            let dur = std::time::Duration::from_millis(timeout_ms);
            Ok(tokio::time::timeout(dur, rx.recv())
                .await
                .ok()
                .and_then(Result::ok))
        })
    }

    fn status<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;
            let status = queue.status(&mut conn, &task_id).await.map_err(to_py_err)?;
            Ok(status.to_string())
        })
    }

    /// `(payload_bytes | None, tries, scheduled_at_ms)` for an unfinished task.
    fn task_info<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;
            let (payload, tries, scheduled) = queue
                .fetch_info(&mut conn, &task_id)
                .await
                .map_err(to_py_err)?;
            Python::attach(|py| -> PyResult<(Py<PyAny>, i64, i64)> {
                let payload = match payload {
                    Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                    None => py.None(),
                };
                Ok((payload, tries, scheduled))
            })
        })
    }

    #[getter]
    fn worker_id(&self) -> &str {
        &self.config.worker_id
    }

    #[getter]
    fn concurrency(&self) -> usize {
        self.config.concurrency
    }

    #[getter]
    fn prefetch(&self) -> i64 {
        self.config.prefetch
    }

    #[getter]
    fn idle_timeout_ms(&self) -> i64 {
        self.config.idle_timeout_ms
    }

    #[getter]
    fn poll_block_ms(&self) -> i64 {
        self.config.poll_block_ms
    }

    #[getter]
    fn result_ttl_ms(&self) -> i64 {
        self.config.result_ttl_ms
    }
}

async fn abort_stream(
    cell: &OnceCell<async_channel::Receiver<String>>,
    client: &redis::Client,
    channel: String,
) -> PyResult<async_channel::Receiver<String>> {
    let rx = cell
        .get_or_try_init(|| async {
            let mut pubsub = client.get_async_pubsub().await?;
            pubsub.subscribe(&channel).await?;
            let (tx, rx) = async_channel::unbounded();
            tokio::spawn(async move {
                let mut stream = pubsub.on_message();
                while let Some(msg) = stream.next().await {
                    match msg.get_payload::<String>() {
                        Ok(task_id) => {
                            if tx.send(task_id).await.is_err() {
                                break; // no receivers left
                            }
                        }
                        Err(err) => tracing::warn!("ardiq unreadable abort message: {err}"),
                    }
                }
            });
            Ok::<_, redis::RedisError>(rx)
        })
        .await
        .map_err(to_py_err)?;
    Ok(rx.clone())
}

async fn shared_conn(
    cell: &OnceCell<ConnectionManager>,
    client: &redis::Client,
) -> PyResult<ConnectionManager> {
    let conn = cell
        .get_or_try_init(|| async { ConnectionManager::new(client.clone()).await })
        .await
        .map_err(to_py_err)?;
    Ok(conn.clone())
}

fn opt<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(dict.get_item(key)?.filter(|value| !value.is_none()))
}

fn default_worker_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:08x}", (nanos as u64) & 0xffff_ffff)
}

fn to_py_err(err: redis::RedisError) -> PyErr {
    if err.is_io_error() {
        BrokerError::new_err(err.to_string())
    } else {
        ArdiqError::new_err(err.to_string())
    }
}

/// Install a tracing subscriber that forwards Rust logs to stderr.
/// Level: DEBUG when verbose=True, else INFO. Safe to call multiple times
/// (subsequent calls are no-ops once a global subscriber is set).
#[pyfunction]
fn init_logging(verbose: bool) {
    use tracing_subscriber::fmt;
    use tracing_subscriber::EnvFilter;

    let level = if verbose { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let _ = fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init(); // no-op if already initialized
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArdiqCore>()?;
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    m.add("ArdiqError", m.py().get_type::<ArdiqError>())?;
    m.add("BrokerError", m.py().get_type::<BrokerError>())?;
    Ok(())
}
