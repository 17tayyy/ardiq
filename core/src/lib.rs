//! PyO3 bridge for ArdiQ.
//!
//! The Rust core owns the loop and all Redis I/O; Python owns task definitions,
//! the wire format (msgpack), and execution of the actual functions. They meet
//! here: [`ArdiqCore::run`] starts the Rust worker and hands each ready task to
//! a Python callback (the async executor) via `pyo3-async-runtimes`.
//!
//! ## Python-facing contract
//!
//! ```python
//! core = ArdiqCore({"redis_url": ..., "queue_name": ..., "concurrency": 16, ...})
//!
//! # client side
//! await core.enqueue(task_id, payload_bytes, priority=None,
//!                    delay_ms=0, schedule_ms=0, expire_ms=0)
//! await core.result(task_id)  # stored result bytes, or None
//! await core.status(task_id)  # "complete" | "running" | "queued" | "not_found"
//!
//! # worker side: `execute` runs the registered fn and returns
//! #   (outcome, result_bytes, retry_after_ms)
//! #     outcome: 0 = success, 1 = terminal failure, 2 = retry
//! #     retry_after_ms: explicit backoff for outcome 2 (0 = use default)
//! async def execute(task_id: str, payload: bytes, tries: int): ...
//! await core.run(execute)   # blocks until core.stop()
//! ```

mod queue;
mod worker;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_async_runtimes::TaskLocals;
use redis::aio::ConnectionManager;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::queue::{now_ms, Queue};
use crate::worker::{ExecOutcome, Outcome, TaskExecutor, Worker, WorkerConfig};

/// Bridges the Rust worker to a Python async callback.
struct PyExecutor {
    callback: Py<PyAny>,
    locals: TaskLocals,
}

#[async_trait::async_trait]
impl TaskExecutor for PyExecutor {
    async fn execute(&self, task_id: String, payload: Vec<u8>, tries: i64) -> ExecOutcome {
        // GIL held only to build the coroutine + bridge it into a Rust future.
        let future = Python::attach(|py| -> PyResult<_> {
            let bytes = PyBytes::new(py, &payload);
            let coro = self.callback.bind(py).call1((task_id, bytes, tries))?;
            pyo3_async_runtimes::into_future_with_locals(&self.locals, coro)
        });
        let future = match future {
            Ok(future) => future,
            Err(err) => {
                tracing::error!("ardiq executor could not start task: {err}");
                return failure();
            }
        };

        // Awaited without the GIL; reacquired only to read the result tuple.
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

/// Read the `(outcome, result_bytes, retry_after_ms)` tuple returned by Python.
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

/// Handle held by Python: owns the Redis client, key layout and worker config.
#[pyclass]
struct ArdiqCore {
    client: redis::Client,
    queue: Arc<Queue>,
    config: WorkerConfig,
    cancel: CancellationToken,
    // Lazily-created shared connection for client-side ops (enqueue, size).
    conn: Arc<OnceCell<ConnectionManager>>,
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
        // Python passes priorities lowest-first; the Queue wants highest-first.
        let mut priorities: Vec<String> = opt(&config, "priorities")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_else(|| vec!["default".to_string()]);
        priorities.reverse();

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

        let client = redis::Client::open(redis_url).map_err(to_py_err)?;
        let queue = Arc::new(Queue::new(&queue_name, priorities));
        let config = WorkerConfig {
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
            queue,
            config,
            cancel: CancellationToken::new(),
            conn: Arc::new(OnceCell::new()),
        })
    }

    /// Enqueue a task. `score`-style timing is split into `delay_ms` (relative)
    /// and `schedule_ms` (absolute epoch ms); `0` means run immediately.
    /// Returns `False` if a task with the same id already existed.
    #[pyo3(signature = (task_id, payload, priority=None, delay_ms=0, schedule_ms=0, expire_ms=0))]
    fn enqueue<'py>(
        &self,
        py: Python<'py>,
        task_id: String,
        payload: Vec<u8>,
        priority: Option<String>,
        delay_ms: i64,
        schedule_ms: i64,
        expire_ms: i64,
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
                .enqueue(&mut conn, &task_id, &payload, &priority, score, expire_ms, now)
                .await
                .map_err(to_py_err)?;
            Ok(queued)
        })
    }

    /// Start the loop; resolves when the worker stops. `callback` is the async
    /// executor from the module docs.
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

    /// Graceful shutdown: consumers finish their current task, then exit.
    fn stop(&self) {
        self.cancel.cancel();
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

    /// Stored result bytes, or `None` if absent/expired.
    fn result<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.queue.clone();
        let conn = self.conn.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            let mut conn = shared_conn(&conn, &client).await?;
            let raw = queue.fetch_result(&mut conn, &task_id).await.map_err(to_py_err)?;
            Python::attach(|py| -> PyResult<Py<PyAny>> {
                Ok(match raw {
                    Some(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
                    None => py.None(),
                })
            })
        })
    }

    /// Lifecycle of a task: "complete" | "running" | "queued" | "not_found".
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

    #[getter]
    fn worker_id(&self) -> &str {
        &self.config.worker_id
    }
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

/// Config lookup that treats an explicit `None` as absent.
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

fn to_py_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArdiqCore>()?;
    Ok(())
}
