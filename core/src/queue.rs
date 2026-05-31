//! Redis-facing layer: pure Rust, no PyO3. Payloads are opaque bytes (msgpack
//! from the Python client), so the wire format stays owned by Python. Data
//! model mirrors streaq: a stream + consumer group per priority for delivery,
//! a sorted set per priority for delayed tasks.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use redis::aio::ConnectionLike;
use redis::streams::{StreamAutoClaimReply, StreamReadReply};
use redis::{RedisResult, Script};

const GROUP: &str = "workers";

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as i64
}

#[derive(Clone, Debug)]
pub struct StreamMessage {
    pub message_id: String,
    pub task_id: String,
    pub priority: String,
    // Carried for forthcoming schedule-drift checks; unused on the core path.
    #[allow(dead_code)]
    pub enqueue_time: i64,
}

#[derive(Clone, Copy, Debug)]
pub enum ResultTtl {
    None,
    Forever,
    Millis(i64),
}

impl ResultTtl {
    // PyO3-boundary encoding: 0 = drop, negative = forever, positive = ms.
    pub fn from_ms(ms: i64) -> Self {
        match ms {
            0 => ResultTtl::None,
            n if n < 0 => ResultTtl::Forever,
            n => ResultTtl::Millis(n),
        }
    }
}

/// Owns the Redis key layout and atomic ops for one named queue. `priorities`
/// is ordered highest-first (read first); the last entry is the enqueue default.
pub struct Queue {
    priorities: Vec<String>,
    prefix: String,
    stream_to_priority: HashMap<String, String>,
    publish_task: Script,
    publish_delayed: Script,
}

impl Queue {
    pub fn new(queue_name: &str, priorities: Vec<String>) -> Self {
        let prefix = format!("ardiq:{queue_name}");
        let priorities = if priorities.is_empty() {
            vec!["default".to_string()]
        } else {
            priorities
        };
        let stream_to_priority = priorities
            .iter()
            .map(|p| (format!("{prefix}:queues:{p}"), p.clone()))
            .collect();

        Queue {
            priorities,
            prefix,
            stream_to_priority,
            publish_task: Script::new(include_str!("scripts/publish_task.lua")),
            publish_delayed: Script::new(include_str!("scripts/publish_delayed.lua")),
        }
    }

    pub fn default_priority(&self) -> &str {
        self.priorities.last().expect("priorities is never empty")
    }

    fn stream_key(&self, priority: &str) -> String {
        format!("{}:queues:{priority}", self.prefix)
    }
    fn delayed_key(&self, priority: &str) -> String {
        format!("{}:queues:delayed:{priority}", self.prefix)
    }
    fn task_key(&self, task_id: &str) -> String {
        format!("{}:task:data:{task_id}", self.prefix)
    }
    fn result_key(&self, task_id: &str) -> String {
        format!("{}:task:results:{task_id}", self.prefix)
    }
    fn retry_key(&self, task_id: &str) -> String {
        format!("{}:task:retry:{task_id}", self.prefix)
    }
    fn results_set(&self) -> String {
        format!("{}:index:results", self.prefix)
    }
    fn running_set(&self) -> String {
        format!("{}:index:running", self.prefix)
    }
    fn health_key(&self, worker_id: &str) -> String {
        format!("{}:health:{worker_id}", self.prefix)
    }

    pub async fn create_groups<C: ConnectionLike>(&self, conn: &mut C) -> RedisResult<()> {
        for priority in &self.priorities {
            let res: RedisResult<()> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(self.stream_key(priority))
                .arg(GROUP)
                .arg("0")
                .arg("MKSTREAM")
                .query_async(conn)
                .await;
            if let Err(err) = res {
                if !matches!(err.code(), Some("BUSYGROUP")) {
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Returns `false` if a task with the same id already existed.
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue<C: ConnectionLike>(
        &self,
        conn: &mut C,
        task_id: &str,
        payload: &[u8],
        priority: &str,
        score_ms: i64,
        expire_ms: i64,
        now_ms: i64,
    ) -> RedisResult<bool> {
        let queued: i64 = self
            .publish_task
            .arg(self.task_key(task_id))
            .arg(self.stream_key(priority))
            .arg(self.delayed_key(priority))
            .arg(task_id)
            .arg(payload)
            .arg(score_ms)
            .arg(expire_ms)
            .arg(now_ms)
            .invoke_async(conn)
            .await?;
        Ok(queued == 1)
    }

    /// Atomic per priority so concurrent workers can't double-publish.
    pub async fn publish_delayed<C: ConnectionLike>(
        &self,
        conn: &mut C,
        now_ms: i64,
    ) -> RedisResult<i64> {
        let mut moved = 0;
        for priority in &self.priorities {
            moved += self
                .publish_delayed
                .arg(self.delayed_key(priority))
                .arg(self.stream_key(priority))
                .arg(now_ms)
                .invoke_async::<i64>(conn)
                .await?;
        }
        Ok(moved)
    }

    /// Priority-ordered, non-blocking. Per priority: first reclaim idle messages
    /// abandoned by crashed workers (XAUTOCLAIM), then take fresh ones, until
    /// `count` is satisfied.
    pub async fn read_batch<C: ConnectionLike>(
        &self,
        conn: &mut C,
        consumer: &str,
        mut count: i64,
        idle_ms: i64,
    ) -> RedisResult<Vec<StreamMessage>> {
        let mut out = Vec::new();
        for priority in &self.priorities {
            if count <= 0 {
                break;
            }
            let stream = self.stream_key(priority);

            let claim: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
                .arg(&stream)
                .arg(GROUP)
                .arg(consumer)
                .arg(idle_ms)
                .arg("0-0")
                .arg("COUNT")
                .arg(count)
                .query_async(conn)
                .await?;
            for entry in claim.claimed {
                if let Some(msg) = parse_entry(priority, &entry) {
                    out.push(msg);
                    count -= 1;
                }
            }
            if count <= 0 {
                break;
            }

            let read: Option<StreamReadReply> = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(GROUP)
                .arg(consumer)
                .arg("COUNT")
                .arg(count)
                .arg("STREAMS")
                .arg(&stream)
                .arg(">")
                .query_async(conn)
                .await?;
            if let Some(read) = read {
                for key in read.keys {
                    for entry in key.ids {
                        if let Some(msg) = parse_entry(priority, &entry) {
                            out.push(msg);
                            count -= 1;
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// One blocking read across all priorities, used when `read_batch` was empty
    /// so we don't busy-poll.
    pub async fn read_blocking<C: ConnectionLike>(
        &self,
        conn: &mut C,
        consumer: &str,
        count: i64,
        block_ms: i64,
    ) -> RedisResult<Vec<StreamMessage>> {
        let mut command = redis::cmd("XREADGROUP");
        command
            .arg("GROUP")
            .arg(GROUP)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block_ms)
            .arg("STREAMS");
        for priority in &self.priorities {
            command.arg(self.stream_key(priority));
        }
        for _ in &self.priorities {
            command.arg(">");
        }

        let read: Option<StreamReadReply> = command.query_async(conn).await?;
        let mut out = Vec::new();
        if let Some(read) = read {
            for key in read.keys {
                let priority = match self.stream_to_priority.get(&key.key) {
                    Some(p) => p.clone(),
                    None => continue,
                };
                for entry in key.ids {
                    if let Some(msg) = parse_entry(&priority, &entry) {
                        out.push(msg);
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn incr_retry<C: ConnectionLike>(
        &self,
        conn: &mut C,
        task_id: &str,
    ) -> RedisResult<i64> {
        redis::cmd("INCR")
            .arg(self.retry_key(task_id))
            .query_async(conn)
            .await
    }

    pub async fn mark_running<C: ConnectionLike>(
        &self,
        conn: &mut C,
        task_id: &str,
    ) -> RedisResult<()> {
        redis::cmd("SADD")
            .arg(self.running_set())
            .arg(task_id)
            .query_async(conn)
            .await
    }

    pub async fn fetch_payload<C: ConnectionLike>(
        &self,
        conn: &mut C,
        task_id: &str,
    ) -> RedisResult<Option<Vec<u8>>> {
        redis::cmd("GET")
            .arg(self.task_key(task_id))
            .query_async(conn)
            .await
    }

    /// Success or terminal failure: ack + delete the entry, drop bookkeeping
    /// keys, optionally store the result.
    pub async fn complete<C: ConnectionLike>(
        &self,
        conn: &mut C,
        msg: &StreamMessage,
        result: &[u8],
        ttl: ResultTtl,
        now_ms: i64,
    ) -> RedisResult<()> {
        let stream = self.stream_key(&msg.priority);
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.cmd("XACK").arg(&stream).arg(GROUP).arg(&msg.message_id).ignore();
        pipe.cmd("XDEL").arg(&stream).arg(&msg.message_id).ignore();
        pipe.cmd("SREM").arg(self.running_set()).arg(&msg.task_id).ignore();
        pipe.cmd("DEL")
            .arg(self.retry_key(&msg.task_id))
            .arg(self.task_key(&msg.task_id))
            .ignore();
        match ttl {
            ResultTtl::None => {}
            ResultTtl::Forever => {
                pipe.cmd("SET").arg(self.result_key(&msg.task_id)).arg(result).ignore();
                pipe.cmd("ZADD")
                    .arg(self.results_set())
                    .arg(now_ms + FAR_FUTURE_MS)
                    .arg(&msg.task_id)
                    .ignore();
            }
            ResultTtl::Millis(ms) => {
                pipe.cmd("SET")
                    .arg(self.result_key(&msg.task_id))
                    .arg(result)
                    .arg("PX")
                    .arg(ms)
                    .ignore();
                pipe.cmd("ZADD")
                    .arg(self.results_set())
                    .arg(now_ms + ms)
                    .arg(&msg.task_id)
                    .ignore();
            }
        }
        pipe.query_async(conn).await
    }

    /// Requeue for a later attempt: payload + retry counter are kept so the next
    /// run resumes the same task.
    pub async fn retry_later<C: ConnectionLike>(
        &self,
        conn: &mut C,
        msg: &StreamMessage,
        fire_at_ms: i64,
    ) -> RedisResult<()> {
        let stream = self.stream_key(&msg.priority);
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.cmd("XACK").arg(&stream).arg(GROUP).arg(&msg.message_id).ignore();
        pipe.cmd("XDEL").arg(&stream).arg(&msg.message_id).ignore();
        pipe.cmd("SREM").arg(self.running_set()).arg(&msg.task_id).ignore();
        pipe.cmd("ZADD")
            .arg(self.delayed_key(&msg.priority))
            .arg(fire_at_ms)
            .arg(&msg.task_id)
            .ignore();
        pipe.query_async(conn).await
    }

    /// Drop a task that can't run (e.g. payload expired): ack + clean up, no result.
    pub async fn discard<C: ConnectionLike>(
        &self,
        conn: &mut C,
        msg: &StreamMessage,
    ) -> RedisResult<()> {
        let stream = self.stream_key(&msg.priority);
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.cmd("XACK").arg(&stream).arg(GROUP).arg(&msg.message_id).ignore();
        pipe.cmd("XDEL").arg(&stream).arg(&msg.message_id).ignore();
        pipe.cmd("SREM").arg(self.running_set()).arg(&msg.task_id).ignore();
        pipe.cmd("DEL").arg(self.retry_key(&msg.task_id)).ignore();
        pipe.query_async(conn).await
    }

    /// Refresh health and re-assert ownership of in-flight messages (XCLAIM) so
    /// other workers don't reclaim them mid-run. `in_flight`: priority -> entry ids.
    pub async fn heartbeat<C: ConnectionLike>(
        &self,
        conn: &mut C,
        worker_id: &str,
        status: &str,
        idle_ms: i64,
        in_flight: &HashMap<String, Vec<String>>,
        now_ms: i64,
    ) -> RedisResult<()> {
        let mut pipe = redis::pipe();
        pipe.cmd("SET")
            .arg(self.health_key(worker_id))
            .arg(status)
            .arg("PX")
            .arg(idle_ms)
            .ignore();
        pipe.cmd("ZREMRANGEBYSCORE")
            .arg(self.results_set())
            .arg(0)
            .arg(now_ms)
            .ignore();
        for (priority, ids) in in_flight {
            if ids.is_empty() {
                continue;
            }
            let mut claim = redis::cmd("XCLAIM");
            claim
                .arg(self.stream_key(priority))
                .arg(GROUP)
                .arg(worker_id)
                .arg(0);
            for id in ids {
                claim.arg(id);
            }
            claim.arg("JUSTID");
            pipe.add_command(claim).ignore();
        }
        pipe.query_async(conn).await
    }

    pub async fn queue_size<C: ConnectionLike>(&self, conn: &mut C) -> RedisResult<i64> {
        let mut pipe = redis::pipe();
        for priority in &self.priorities {
            pipe.cmd("XLEN").arg(self.stream_key(priority));
            pipe.cmd("ZCARD").arg(self.delayed_key(priority));
        }
        let counts: Vec<i64> = pipe.query_async(conn).await?;
        Ok(counts.iter().sum())
    }

    /// Stored result envelope (opaque bytes), or `None` if absent/expired.
    pub async fn fetch_result<C: ConnectionLike>(
        &self,
        conn: &mut C,
        task_id: &str,
    ) -> RedisResult<Option<Vec<u8>>> {
        redis::cmd("GET")
            .arg(self.result_key(task_id))
            .query_async(conn)
            .await
    }

    /// Lifecycle, result-first so a just-finished task reads `complete`:
    /// complete | running | queued | not_found.
    pub async fn status<C: ConnectionLike>(
        &self,
        conn: &mut C,
        task_id: &str,
    ) -> RedisResult<&'static str> {
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.cmd("EXISTS").arg(self.result_key(task_id));
        pipe.cmd("SISMEMBER").arg(self.running_set()).arg(task_id);
        pipe.cmd("EXISTS").arg(self.task_key(task_id));
        let (has_result, running, has_data): (i64, i64, i64) = pipe.query_async(conn).await?;
        Ok(if has_result == 1 {
            "complete"
        } else if running == 1 {
            "running"
        } else if has_data == 1 {
            "queued"
        } else {
            "not_found"
        })
    }
}

fn parse_entry(priority: &str, entry: &redis::streams::StreamId) -> Option<StreamMessage> {
    Some(StreamMessage {
        message_id: entry.id.clone(),
        task_id: entry.get("task_id")?,
        priority: priority.to_string(),
        enqueue_time: entry.get("enqueue_time").unwrap_or(0),
    })
}

// Far-future score for never-expiring results, so the cleanup ZSET still has a
// (very distant) deadline for them.
const FAR_FUTURE_MS: i64 = 100_000_000_000;
