use super::alternator_error::{AlternatorError, AlternatorErrorKind};
use super::model::{Reply, Request};
use crate::clock;
use crate::config::{RetryInterval, ValidationStrategy};
use crate::error::LatteError;
use crate::exec::runtime::{FutureSlots, Reactor};
use crate::local::TryLock;
use crate::ops::{AlternatorOp, DriverError, Json as OpJson, OpKind, Response};
use crate::scripting::cluster_info::ClusterInfo;
use crate::scripting::row_distribution::RowDistributionPreset;
use crate::stats::session::SessionStats;
use crate::stats::session::SEND_JITTER_METRIC;
use rune::runtime::Object;
use rune::{Any, Value};
use serde_json::Value as Json;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

/// The output of a paged request: the items and the token of the next page.
pub(super) type PagedResult =
    Result<(Vec<Value>, Option<super::traits::PaginationToken>), AlternatorError>;

#[derive(Any)]
pub struct Context {
    reactor: Option<Rc<Reactor>>,
    page_size: u64,
    pub stats: Rc<TryLock<SessionStats>>,
    /// Slots for the futures of this worker's requests; see [`FutureSlots`].
    pub(super) calls: Rc<FutureSlots<PagedResult>>,
    /// Slots for the futures of this worker's validated requests.
    pub(super) validated_calls: Rc<FutureSlots<Result<Vec<Value>, AlternatorError>>>,
    pub report_metadata: Rc<TryLock<HashMap<String, String>>>,
    pub metric_orientations: Rc<TryLock<HashMap<String, i8>>>,
    pub start_time: TryLock<Instant>,
    pub retry_number: u64,
    pub retry_interval: RetryInterval,
    pub validation_strategy: ValidationStrategy,
    pub partition_row_presets: Rc<TryLock<HashMap<String, RowDistributionPreset>>>,
    /// Number of cycles the `load` command runs, set by the script's `prepare`
    /// function. Shared - not copied - by [`Context::shallow_clone`], so that a
    /// write made inside the Rune VM reaches the `Context` the caller reads it
    /// back from. Its Rune-visible `get`/`set`/`add_assign` accessors are
    /// registered by hand in `scripting::init_context_module`, because
    /// `#[rune(get, set)]` cannot be derived for a shared field.
    /// The `Rc` keeps the per-cycle clones of a worker free of atomic operations; the
    /// counter itself is atomic because the run reads it after the prepare phase.
    #[allow(clippy::redundant_allocation)]
    load_cycle_count: Rc<Arc<AtomicU64>>,
    /// True on per-worker deep copies made by [`Context::clone`].
    /// Run-level state written through such a copy (report metadata, metric
    /// orientations) is never merged back, so the scripting API rejects those calls.
    pub is_worker_clone: bool,
    /// True while the script's `prepare_worker` function runs on this context.
    /// Per-cycle stats written during it (e.g. metrics) are cleared before the
    /// run starts, so the scripting API rejects those calls.
    pub in_prepare_worker: bool,
    /// 0-based index of this worker among `worker_count` workers; 0 on the
    /// main context used by setup functions.
    #[rune(get, copy)]
    pub worker_id: u64,
    /// Number of worker threads in the run; 1 on the main context.
    #[rune(get, copy)]
    pub worker_count: u64,
    #[rune(get)]
    pub data: Value,
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(
        reactor: Option<Rc<Reactor>>,
        retry_number: u64,
        retry_interval: RetryInterval,
        validation_strategy: ValidationStrategy,
        page_size: u64,
    ) -> Context {
        Context {
            reactor,
            page_size,
            stats: Rc::new(TryLock::new(SessionStats::new())),
            calls: Rc::default(),
            validated_calls: Rc::default(),
            report_metadata: Rc::new(TryLock::new(HashMap::new())),
            metric_orientations: Rc::new(TryLock::new(HashMap::new())),
            start_time: TryLock::new(Instant::now()),
            retry_number,
            retry_interval,
            validation_strategy,
            partition_row_presets: Rc::new(TryLock::new(HashMap::new())),
            load_cycle_count: Rc::new(Arc::new(AtomicU64::new(0))),
            is_worker_clone: false,
            in_prepare_worker: false,
            worker_id: 0,
            worker_count: 1,
            data: Value::new(Object::new()).unwrap(),
        }
    }

    /// Number of cycles the `load` command should run, as set by the script's
    /// `prepare` function through `ctx.load_cycle_count = ...`.
    pub fn load_cycle_count(&self) -> u64 {
        self.load_cycle_count.load(Ordering::Relaxed)
    }

    pub(crate) fn set_load_cycle_count(&mut self, value: u64) {
        self.load_cycle_count.store(value, Ordering::Relaxed);
    }

    pub(crate) fn add_assign_load_cycle_count(&mut self, value: u64) {
        self.load_cycle_count.fetch_add(value, Ordering::Relaxed);
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Result<Self, LatteError> {
        let serialized = rmp_serde::to_vec(&self.data)
            .map_err(|e| LatteError::context_data_encode(&self.data, &e))?;
        let deserialized: Value = rmp_serde::from_slice(&serialized)?;
        Ok(Context {
            reactor: self.reactor.clone(),
            page_size: self.page_size,
            stats: Rc::new(TryLock::new(SessionStats::default())),
            calls: Rc::default(),
            validated_calls: Rc::default(),
            report_metadata: Rc::new(TryLock::new(
                self.report_metadata.try_lock().unwrap().clone(),
            )),
            metric_orientations: Rc::new(TryLock::new(
                self.metric_orientations.try_lock().unwrap().clone(),
            )),
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
            retry_number: self.retry_number,
            retry_interval: self.retry_interval,
            validation_strategy: self.validation_strategy,
            partition_row_presets: Rc::new(TryLock::new(
                self.partition_row_presets.try_lock().unwrap().clone(),
            )),
            load_cycle_count: Rc::new(Arc::new(AtomicU64::new(self.load_cycle_count()))),
            is_worker_clone: true,
            in_prepare_worker: false,
            worker_id: self.worker_id,
            worker_count: self.worker_count,
            data: deserialized,
        })
    }

    /// Creates a shallow clone that shares the Arc-backed fields (stats, presets)
    /// with the original. Used to create a rune-owned `Value` for function call arguments.
    pub fn shallow_clone(&self) -> Self {
        Context {
            reactor: self.reactor.clone(),
            page_size: self.page_size,
            stats: Rc::clone(&self.stats),
            calls: Rc::clone(&self.calls),
            validated_calls: Rc::clone(&self.validated_calls),
            report_metadata: Rc::clone(&self.report_metadata),
            metric_orientations: Rc::clone(&self.metric_orientations),
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
            retry_number: self.retry_number,
            retry_interval: self.retry_interval,
            validation_strategy: self.validation_strategy,
            partition_row_presets: Rc::clone(&self.partition_row_presets),
            load_cycle_count: Rc::clone(&self.load_cycle_count),
            is_worker_clone: self.is_worker_clone,
            in_prepare_worker: self.in_prepare_worker,
            worker_id: self.worker_id,
            worker_count: self.worker_count,
            data: self.data.clone(),
        }
    }

    /// Builds the main context of a run from the command line settings.
    pub fn from_connection(conf: &crate::config::ConnectionConf) -> Context {
        Context::new(
            None,
            conf.retry_number,
            conf.retry_interval,
            conf.validation_strategy,
            conf.page_size.get() as u64,
        )
    }

    /// Binds this context to the reactor of the instance that runs it.
    pub fn set_reactor(&mut self, reactor: Rc<Reactor>) {
        self.reactor = Some(reactor);
    }

    /// Waits on the shared clock. Returns at once when the context has no reactor.
    pub async fn sleep(&self, duration: Duration) {
        if let Some(reactor) = &self.reactor {
            let deadline = reactor.now_ns() + duration.as_nanos() as u64;
            reactor.sleep_until(deadline).await;
        }
    }

    /// Hands `body` to the host. Returns the response body and how long the host's client
    /// took, or the error the host reported. Records how late the host started the request
    /// after its send time as the `send_jitter_ms` metric.
    async fn submit(
        &self,
        op: AlternatorOp,
        body: OpJson,
    ) -> Result<Result<(OpJson, Duration), DriverError>, AlternatorError> {
        let reactor = self.reactor.as_ref().ok_or_else(|| {
            AlternatorError::new(AlternatorErrorKind::Error(
                "DynamoDB client is not initialized".to_string(),
            ))
        })?;
        let sent = reactor.submit(OpKind::Alternator { op, body }).await;
        let completion = sent.completion;
        if completion.start_ns > 0 {
            let jitter_ns = completion.start_ns.saturating_sub(sent.send_at_ns);
            self.stats
                .try_lock()
                .unwrap()
                .record_metric(SEND_JITTER_METRIC, jitter_ns as f64 / 1_000_000.0);
        }
        let duration = Duration::from_nanos(completion.end_ns.saturating_sub(completion.start_ns));
        Ok(match completion.result {
            Ok(Response::Json(body)) => Ok((body, duration)),
            Ok(Response::Done) => Ok((OpJson::Text(Default::default()), duration)),
            Ok(other) => Err(DriverError::core(format!(
                "Unexpected response from the driver host: {other:?}"
            ))),
            Err(e) => Err(e),
        })
    }

    /// Sends a request whose response the caller reads as JSON. For setup requests only:
    /// it parses the whole response.
    pub(super) async fn send(
        &self,
        request: &Request,
    ) -> Result<Result<(Json, Duration), DriverError>, AlternatorError> {
        let body = OpJson::Request(request.body());
        Ok(match self.submit(request.op, body).await? {
            Ok((body, duration)) => match body.into_value() {
                Ok(json) => Ok((json, duration)),
                Err(e) => Err(DriverError::core(format!("Invalid response JSON: {e}"))),
            },
            Err(e) => Err(e),
        })
    }

    /// Sends a request and parses only the response fields that scripts and validation read.
    pub(super) async fn send_page(
        &self,
        request: &Request,
    ) -> Result<Result<(Reply, Duration), DriverError>, AlternatorError> {
        let body = OpJson::Request(request.body());
        Ok(match self.submit(request.op, body).await? {
            Ok((body, duration)) => match Reply::parse(body) {
                Ok(reply) => Ok((reply, duration)),
                Err(e) => Err(DriverError::core(format!("Invalid response JSON: {e}"))),
            },
            Err(e) => Err(e),
        })
    }

    /// Returns cluster metadata.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, AlternatorError> {
        // Try ScyllaDB-specific system table via the alternator virtual interface
        let scan = Request::new(AlternatorOp::Scan)
            .table_name(".scylla.alternator.system.versions")
            .projection_expression("version, build_id".to_string());
        if let Ok((output, _)) = self.send(&scan).await? {
            if let Some(item) = output
                .get("Items")
                .and_then(Json::as_array)
                .and_then(|items| items.first())
            {
                let field = |name: &str| {
                    item.get(name)
                        .and_then(|v| v.get("S"))
                        .and_then(Json::as_str)
                        .unwrap_or("unknown")
                        .to_string()
                };
                return Ok(Some(ClusterInfo {
                    name: "".to_string(),
                    db_version: format!(
                        "ScyllaDB {} with build-id {}",
                        field("version"),
                        field("build_id")
                    ),
                }));
            }
        }

        // If the ScyllaDB-specific table is not available, try to determine if it's AWS.
        let describe = Request::new(AlternatorOp::DescribeEndpoints);
        if let Ok((output, _)) = self.send(&describe).await? {
            let aws_endpoint = output
                .get("Endpoints")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .filter_map(|e| e.get("Address").and_then(Json::as_str))
                .find(|address| address.contains("amazonaws.com"));
            if let Some(address) = aws_endpoint {
                return Ok(Some(ClusterInfo {
                    name: address.to_string(),
                    db_version: "AWS DynamoDB".to_string(),
                }));
            }
        }

        // We couldn't determine the cluster info.
        Ok(None)
    }

    pub fn set_report_field(&self, key: &str, value: &str) {
        self.report_metadata
            .try_lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
    }

    pub fn report_metadata_snapshot(&self) -> HashMap<String, String> {
        self.report_metadata.try_lock().unwrap().clone()
    }

    pub fn record_metric(&self, name: &str, value: f64) {
        self.stats.try_lock().unwrap().record_metric(name, value);
    }

    pub fn declare_metric(&self, name: &str, orientation: i8) {
        self.metric_orientations
            .try_lock()
            .unwrap()
            .insert(name.to_string(), orientation);
    }

    pub fn metric_orientations_snapshot(&self) -> HashMap<String, i8> {
        self.metric_orientations.try_lock().unwrap().clone()
    }

    pub fn take_session_stats(&self) -> SessionStats {
        let mut stats = self.stats.try_lock().unwrap();
        let result = stats.clone();
        stats.reset();
        result
    }

    pub fn reset(&self) {
        self.stats.try_lock().unwrap().reset();
        *self.start_time.try_lock().unwrap() = clock::instant_at(
            self.reactor
                .as_ref()
                .map_or_else(clock::now_ns, |r| r.now_ns()),
        );
    }

    pub fn get_page_size(&self) -> u64 {
        self.page_size
    }
}
