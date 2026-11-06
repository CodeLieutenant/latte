use super::cass_error::{CassError, CassErrorKind};
use super::deserialize::RuneRow;
use super::serialize::RuneQueryParams;
use crate::clock;
use crate::config::{RetryInterval, ValidationStrategy};
use crate::error::LatteError;
use crate::exec::runtime::{FutureSlots, Reactor};
use crate::ops::{
    next_stmt_id, Completion, DriverError, NativeValues, OpKind, Response, RowMode, Rows, StmtId,
    Values,
};
use crate::scripting::cluster_info::ClusterInfo;
use crate::scripting::retry_error::handle_retry_error;
use crate::scripting::row_distribution::RowDistributionPreset;
use crate::stats::session::{SessionStats, SEND_JITTER_METRIC};

use crate::local::TryLock;
use once_cell::sync::Lazy;
use regex_automata::{meta, Input};
use rune::runtime::{Object, Vec as RuneVec};
use rune::{Any, Value};
use scylla_cql_core::deserialize::result::{RawRowIterator, TypedRowIterator};
use scylla_cql_core::deserialize::row::DeserializeRow;
use scylla_cql_core::deserialize::FrameSlice;
use scylla_cql_core::frame::response::result::ColumnSpec;
use scylla_cql_core::serialize::row::{RowSerializationContext, SerializedValues};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

static IS_SELECT_QUERY: Lazy<meta::Regex> =
    Lazy::new(|| meta::Regex::new(r"(?i)^\s*select\b").unwrap());
static IS_SELECT_COUNT_QUERY: Lazy<meta::Regex> =
    Lazy::new(|| meta::Regex::new(r"(?i)^\s*select\s+count\s*\(\s*[^)]*\s*\)").unwrap());

// Each thread searches with its own cache, so threads never share a lock to match a query.
thread_local! {
    static IS_SELECT_CACHE: RefCell<meta::Cache> = RefCell::new(IS_SELECT_QUERY.create_cache());
    static IS_SELECT_COUNT_CACHE: RefCell<meta::Cache> =
        RefCell::new(IS_SELECT_COUNT_QUERY.create_cache());
}

fn is_select_query(cql: &str) -> bool {
    IS_SELECT_CACHE.with(|cache| {
        IS_SELECT_QUERY
            .search_with(&mut cache.borrow_mut(), &Input::new(cql))
            .is_some()
    })
}

fn is_select_count_query(cql: &str) -> bool {
    IS_SELECT_COUNT_CACHE.with(|cache| {
        IS_SELECT_COUNT_QUERY
            .search_with(&mut cache.borrow_mut(), &Input::new(cql))
            .is_some()
    })
}

/// A statement the host prepared, with the bind-marker types it returned.
#[derive(Debug)]
pub struct PreparedStatement {
    pub stmt: StmtId,
    pub cql: String,
    pub bind_specs: Rc<[ColumnSpec<'static>]>,
    /// The statement is a `SELECT`; decided once, at prepare time.
    pub is_select: bool,
    /// The statement is a `SELECT COUNT(...)`; decided once, at prepare time.
    pub is_select_count: bool,
}

impl PreparedStatement {
    fn new(stmt: StmtId, cql: String, bind_specs: Rc<[ColumnSpec<'static>]>) -> Self {
        PreparedStatement {
            stmt,
            is_select: is_select_query(&cql),
            is_select_count: is_select_count_query(&cql),
            cql,
            bind_specs,
        }
    }
}

/// What a single `_execute` call sends to the server: a statement registered
/// earlier by `prepare`, or an ad-hoc one sent unprepared.
enum ExecTarget {
    Prepared(Rc<PreparedStatement>),
    AdHoc(String),
}

impl ExecTarget {
    fn cql(&self) -> &str {
        match self {
            Self::Prepared(stmt) => &stmt.cql,
            Self::AdHoc(cql) => cql,
        }
    }

    fn is_select(&self) -> bool {
        match self {
            Self::Prepared(stmt) => stmt.is_select,
            Self::AdHoc(cql) => is_select_query(cql),
        }
    }

    fn is_select_count(&self) -> bool {
        match self {
            Self::Prepared(stmt) => stmt.is_select_count,
            Self::AdHoc(cql) => is_select_count_query(cql),
        }
    }
}

fn no_session() -> CassError {
    CassError(CassErrorKind::Error("'session' is not defined".to_string()))
}

fn decode_rows<'a, R: DeserializeRow<'a, 'a>>(
    rows: &'a Rows,
) -> Result<TypedRowIterator<'a, 'a, R>, CassError> {
    let raw = RawRowIterator::new(
        rows.row_count as usize,
        &rows.specs,
        FrameSlice::new_borrowed(&rows.data),
    );
    TypedRowIterator::new(raw).map_err(|e| CassError(CassErrorKind::Error(e.to_string())))
}

/// This is the main object that a workload script uses to interface with the outside world.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Context {
    pub start_time: TryLock<Instant>,
    // NOTE: 'reactor' is optional for being able to test methods
    // which don't 'depend on'/'use' the database.
    reactor: Option<Rc<Reactor>>,
    page_size: u64,
    statements: Rc<TryLock<HashMap<String, Rc<PreparedStatement>>>>,
    pub stats: Rc<TryLock<SessionStats>>,
    /// Slots for the futures of this worker's execute calls; see [`FutureSlots`].
    pub(crate) calls: Rc<FutureSlots<Result<Value, CassError>>>,
    pub report_metadata: Rc<TryLock<HashMap<String, String>>>,
    pub metric_orientations: Rc<TryLock<HashMap<String, i8>>>,
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
    #[rune(get)]
    pub preferred_datacenter: String,
    #[rune(get)]
    pub preferred_rack: String,
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

// Needed, because Rune `Value` is !Send, as it may contain some internal pointers.
// Therefore, it is not safe to pass a `Value` to another thread by cloning it, because
// both objects could accidentally share some unprotected, `!Sync` data.
// To make it safe, the same `Context` is never used by more than one thread at once, and
// we make sure in `clone` to make a deep copy of the `data` field by serializing
// and deserializing it, so no pointers could get through.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(
        reactor: Option<Rc<Reactor>>,
        page_size: u64,
        preferred_datacenter: String,
        preferred_rack: String,
        retry_number: u64,
        retry_interval: RetryInterval,
        validation_strategy: ValidationStrategy,
    ) -> Context {
        let data = Value::new(Object::new()).unwrap();
        Context {
            start_time: TryLock::new(Instant::now()),
            reactor,
            page_size,
            statements: Rc::new(TryLock::new(HashMap::new())),
            stats: Rc::new(TryLock::new(SessionStats::new())),
            calls: Rc::default(),
            report_metadata: Rc::new(TryLock::new(HashMap::new())),
            metric_orientations: Rc::new(TryLock::new(HashMap::new())),
            retry_number,
            retry_interval,
            validation_strategy,
            partition_row_presets: Rc::new(TryLock::new(HashMap::new())),
            load_cycle_count: Rc::new(Arc::new(AtomicU64::new(0))),
            preferred_datacenter,
            preferred_rack,
            is_worker_clone: false,
            in_prepare_worker: false,
            worker_id: 0,
            worker_count: 1,
            data,
        }
    }

    /// Builds the main context of a run from the command line settings.
    pub fn from_connection(conf: &crate::config::ConnectionConf) -> Context {
        Context::new(
            None,
            conf.page_size.get() as u64,
            conf.datacenter.clone().unwrap_or_default(),
            conf.rack.clone().unwrap_or_default(),
            conf.retry_number,
            conf.retry_interval,
            conf.validation_strategy,
        )
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

    /// Clones the context for use by another thread.
    /// The new clone gets fresh statistics.
    /// The user data gets passed through serialization and deserialization to avoid
    /// accidental data sharing.
    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Result<Self, LatteError> {
        let serialized = rmp_serde::to_vec(&self.data)
            .map_err(|e| LatteError::context_data_encode(&self.data, &e))?;
        let deserialized: Value = rmp_serde::from_slice(&serialized)?;
        Ok(Context {
            reactor: self.reactor.clone(),
            page_size: self.page_size,
            // A worker runs on its own thread, so it gets its own copy of every statement
            // instead of a second owner of the same `Rc`.
            statements: Rc::new(TryLock::new(
                self.statements
                    .try_lock()
                    .unwrap()
                    .iter()
                    .map(|(key, stmt)| {
                        (
                            key.clone(),
                            Rc::new(PreparedStatement {
                                stmt: stmt.stmt,
                                cql: stmt.cql.clone(),
                                bind_specs: stmt.bind_specs.iter().cloned().collect(),
                                is_select: stmt.is_select,
                                is_select_count: stmt.is_select_count,
                            }),
                        )
                    })
                    .collect(),
            )),
            stats: Rc::new(TryLock::new(SessionStats::default())),
            calls: Rc::default(),
            report_metadata: Rc::new(TryLock::new(
                self.report_metadata.try_lock().unwrap().clone(),
            )),
            metric_orientations: Rc::new(TryLock::new(
                self.metric_orientations.try_lock().unwrap().clone(),
            )),
            retry_number: self.retry_number,
            retry_interval: self.retry_interval,
            validation_strategy: self.validation_strategy,
            partition_row_presets: Rc::new(TryLock::new(
                self.partition_row_presets.try_lock().unwrap().clone(),
            )),
            load_cycle_count: Rc::new(Arc::new(AtomicU64::new(self.load_cycle_count()))),
            preferred_datacenter: self.preferred_datacenter.clone(),
            preferred_rack: self.preferred_rack.clone(),
            is_worker_clone: true,
            in_prepare_worker: false,
            worker_id: self.worker_id,
            worker_count: self.worker_count,
            data: deserialized,
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
        })
    }

    /// Creates a shallow clone that shares the Arc-backed fields (stats, statements, presets)
    /// with the original. Used to create a rune-owned `Value` for function call arguments
    /// without losing stats tracking.
    pub fn shallow_clone(&self) -> Self {
        Context {
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
            reactor: self.reactor.clone(),
            page_size: self.page_size,
            statements: Rc::clone(&self.statements),
            stats: Rc::clone(&self.stats),
            calls: Rc::clone(&self.calls),
            report_metadata: Rc::clone(&self.report_metadata),
            metric_orientations: Rc::clone(&self.metric_orientations),
            retry_number: self.retry_number,
            retry_interval: self.retry_interval,
            validation_strategy: self.validation_strategy,
            partition_row_presets: Rc::clone(&self.partition_row_presets),
            load_cycle_count: Rc::clone(&self.load_cycle_count),
            preferred_datacenter: self.preferred_datacenter.clone(),
            preferred_rack: self.preferred_rack.clone(),
            is_worker_clone: self.is_worker_clone,
            in_prepare_worker: self.in_prepare_worker,
            worker_id: self.worker_id,
            worker_count: self.worker_count,
            data: self.data.clone(),
        }
    }

    /// True when the host takes native values and rows; see [`crate::native`].
    fn native(&self) -> bool {
        self.reactor.as_ref().is_some_and(|r| r.native())
    }

    /// The bound values of one execute: script values for a native host, CQL bytes for a
    /// C host.
    #[allow(clippy::result_large_err)]
    fn bind_values(
        &self,
        stmt: &Rc<PreparedStatement>,
        params: Option<&Value>,
        cql: &str,
    ) -> Result<Values, CassError> {
        if self.native() {
            return Ok(Values::Native(NativeValues {
                params: params.cloned(),
                specs: Rc::clone(&stmt.bind_specs),
                serialize: super::native::serialize_params,
            }));
        }
        Ok(Values::Serialized(serialize_params(stmt, params, cql)?))
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

    /// Sends an op to the host and waits for its completion. Records how late the host
    /// started the op after the send time the core gave it, as the `send_jitter_ms` metric.
    async fn submit(&self, kind: OpKind) -> Result<Completion, CassError> {
        let reactor = self.reactor.as_ref().ok_or_else(no_session)?;
        let record_jitter = !matches!(kind, OpKind::Prepare { .. } | OpKind::Datacenters);
        let sent = reactor.submit(kind).await;
        if record_jitter && sent.completion.start_ns > 0 {
            let jitter_ns = sent.completion.start_ns.saturating_sub(sent.send_at_ns);
            self.stats
                .try_lock()
                .unwrap()
                .record_metric(SEND_JITTER_METRIC, jitter_ns as f64 / 1_000_000.0);
        }
        Ok(sent.completion)
    }

    async fn query_rows(&self, cql: &str) -> Result<Result<Rows, DriverError>, CassError> {
        let completion = self
            .submit(OpKind::Query {
                cql: cql.to_string(),
                page_size: self.page_size as i32,
                paging_state: None,
                rows: RowMode::Rows,
            })
            .await?;
        Ok(match completion.result {
            Ok(Response::Page { rows, .. }) => Ok(rows),
            Ok(other) => Err(unexpected_response(&other)),
            Err(e) => Err(e),
        })
    }

    /// Returns cluster metadata such as cluster name and DB version.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, CassError> {
        let scylla_cql = "SELECT version, build_id FROM system.versions";
        match self.query_rows(scylla_cql).await? {
            Ok(rows) => {
                if let Some(Ok((scylla_version, build_id))) = decode_rows::<(&str, &str)>(&rows)
                    .ok()
                    .and_then(|mut it| it.next())
                {
                    return Ok(Some(ClusterInfo {
                        name: "".to_string(),
                        db_version: format!("ScyllaDB {scylla_version} with build-id {build_id}"),
                    }));
                }
                Ok(None)
            }
            Err(_e) => {
                // NOTE: following exists in both cases
                // and if we run against ScyllaDB then it has static '3.0.8' version.
                let cass_cql = "SELECT cluster_name, release_version FROM system.local";
                match self.query_rows(cass_cql).await? {
                    Ok(rows) => {
                        if let Some(Ok((name, cass_version))) = decode_rows::<(&str, &str)>(&rows)
                            .ok()
                            .and_then(|mut it| it.next())
                        {
                            return Ok(Some(ClusterInfo {
                                name: name.to_string(),
                                db_version: format!("Cassandra {cass_version}"),
                            }));
                        }
                        Ok(None)
                    }
                    Err(e) => {
                        eprintln!("WARNING: {e}");
                        Ok(None)
                    }
                }
            }
        }
    }

    /// Returns list of datacenters used by nodes
    pub async fn get_datacenters(&self) -> Result<Vec<String>, CassError> {
        let completion = self.submit(OpKind::Datacenters).await?;
        match completion.result {
            Ok(Response::Datacenters(mut datacenters)) => {
                datacenters.sort();
                datacenters.dedup();
                Ok(datacenters)
            }
            Ok(other) => Err(CassError(CassErrorKind::Error(
                unexpected_response(&other).message,
            ))),
            Err(e) => Err(CassError(CassErrorKind::Error(e.message))),
        }
    }

    /// Prepares a statement and stores it in an internal statement map for future use.
    pub async fn prepare(&self, key: &str, cql: &str) -> Result<(), CassError> {
        let stmt = next_stmt_id();
        let completion = self
            .submit(OpKind::Prepare {
                stmt,
                cql: cql.to_string(),
                page_size: self.page_size as i32,
            })
            .await?;
        match completion.result {
            Ok(Response::Prepared { bind_specs }) => {
                self.statements.try_lock().unwrap().insert(
                    key.to_string(),
                    Rc::new(PreparedStatement::new(
                        stmt,
                        cql.to_string(),
                        bind_specs.into(),
                    )),
                );
                Ok(())
            }
            Ok(other) => Err(CassError::prepare_error(cql, unexpected_response(&other))),
            Err(e) => Err(CassError::prepare_error(cql, e)),
        }
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    pub async fn execute(&self, cql: &str) -> Result<Value, CassError> {
        self._execute(Some(cql), None, None, None, None, None, false)
            .await
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    /// Validates returning rows for `select` queries.
    pub async fn execute_with_validation(
        &self,
        cql: &str,
        expected_rows_num_min: u64,
        expected_rows_num_max: u64,
        custom_err_msg: &str,
    ) -> Result<Value, CassError> {
        if expected_rows_num_min > expected_rows_num_max {
            return Err(CassError(CassErrorKind::Error(format!(
                "Expected 'minimum' ({expected_rows_num_min}) of rows number \
                     cannot be less than 'maximum' ({expected_rows_num_max})"
            ))));
        }
        self._execute(
            Some(cql),
            None,
            None,
            Some(expected_rows_num_min),
            Some(expected_rows_num_max),
            Some(custom_err_msg),
            false,
        )
        .await
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare`.
    pub async fn execute_prepared(&self, key: &str, params: Value) -> Result<Value, CassError> {
        self._execute(None, Some(key), Some(params), None, None, None, false)
            .await
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare` validating
    /// returning rows for `select` queries.
    pub async fn execute_prepared_with_validation(
        &self,
        key: &str,
        params: Value,
        expected_rows_num_min: u64,
        expected_rows_num_max: u64,
        custom_err_msg: &str,
    ) -> Result<Value, CassError> {
        if expected_rows_num_min > expected_rows_num_max {
            return Err(CassError(CassErrorKind::Error(format!(
                "Expected 'minimum' ({expected_rows_num_min}) of rows number \
                     cannot be less than 'maximum' ({expected_rows_num_max})"
            ))));
        }
        self._execute(
            None,
            Some(key),
            Some(params),
            Some(expected_rows_num_min),
            Some(expected_rows_num_max),
            Some(custom_err_msg),
            false,
        )
        .await
    }

    /// Executes an ad-hoc CQL statement and returns the result data.
    pub async fn execute_with_result(&self, cql: &str) -> Result<Value, CassError> {
        self._execute(Some(cql), None, None, None, None, None, true)
            .await
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare` and returns the result data.
    pub async fn execute_prepared_with_result(
        &self,
        key: &str,
        params: Value,
    ) -> Result<Value, CassError> {
        self._execute(None, Some(key), Some(params), None, None, None, true)
            .await
    }

    /// Picks what a single `_execute` call sends to the server. Ad-hoc statements
    /// are left unprepared on purpose: preparing them would create a server-side
    /// entry per unique CQL text and thrash the cache. Needs no session.
    #[allow(clippy::result_large_err)] // consistent with the async CassError APIs around it
    fn resolve_target(
        &self,
        cql: Option<&str>,
        key: Option<&str>,
    ) -> Result<ExecTarget, CassError> {
        match (cql, key) {
            (Some(_), Some(_)) | (None, None) => Err(CassError(CassErrorKind::Error(
                "Either 'cql' or 'key' is allowed, not both".to_string(),
            ))),
            (None, Some(key)) => Ok(ExecTarget::Prepared(
                self.statements
                    .try_lock()
                    .unwrap()
                    .get(key)
                    .cloned()
                    .ok_or_else(|| {
                        CassError(CassErrorKind::PreparedStatementNotFound(key.to_string()))
                    })?,
            )),
            (Some(cql), None) => Ok(ExecTarget::AdHoc(cql.to_string())),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn _execute(
        &self,
        cql: Option<&str>,
        key: Option<&str>,
        params: Option<Value>,
        expected_rows_num_min: Option<u64>,
        expected_rows_num_max: Option<u64>,
        custom_err_msg: Option<&str>,
        process_and_return_data: bool,
    ) -> Result<Value, CassError> {
        if self.reactor.is_none() {
            return Err(no_session());
        }
        let target = self.resolve_target(cql, key)?;
        let cql = target.cql();
        let values = match &target {
            ExecTarget::Prepared(stmt) => Some(self.bind_values(stmt, params.as_ref(), cql)?),
            ExecTarget::AdHoc(_) => None,
        };
        if (expected_rows_num_min.is_some() || expected_rows_num_max.is_some())
            && !target.is_select()
        {
            return Err(CassError::query_response_validation_not_applicable_error(
                cql,
                params.as_ref(),
            ));
        }
        if (expected_rows_num_min.is_some() || expected_rows_num_max.is_some())
            && process_and_return_data
        {
            return Err(CassError(CassErrorKind::Error(
                "Row count validation and rows data processing are not supported together"
                    .to_string(),
            )));
        }
        let is_select_count = target.is_select_count();
        // A plain execute needs no rows, a row-count validation only the count, and reading
        // results or the value of a `SELECT COUNT` needs the rows.
        let row_mode = if process_and_return_data || is_select_count {
            RowMode::Rows
        } else if expected_rows_num_min.is_some() {
            RowMode::Count
        } else {
            RowMode::None
        };
        let mut all_pages_duration = Duration::ZERO;
        let mut paging_state: Option<Vec<u8>> = None;
        let mut rune_rows = RuneVec::new();
        let mut rows_num: u64 = 0;
        let mut last_rows: Option<Rows> = None;
        let mut current_attempt_num = 0;
        while current_attempt_num <= self.retry_number {
            self.stats.try_lock().unwrap().start_request();
            let kind = match (&target, &values) {
                (ExecTarget::Prepared(stmt), Some(values)) => OpKind::Execute {
                    stmt: stmt.stmt,
                    values: values.clone(),
                    paging_state: paging_state.clone(),
                    rows: row_mode,
                },
                _ => OpKind::Query {
                    cql: cql.to_string(),
                    page_size: self.page_size as i32,
                    paging_state: paging_state.clone(),
                    rows: row_mode,
                },
            };
            let completion = self.submit(kind).await?;
            let current_duration =
                Duration::from_nanos(completion.end_ns.saturating_sub(completion.start_ns));
            let (mut rows, paging_state_response) = match completion.result {
                Ok(Response::Page { rows, paging_state }) => (rows, paging_state),
                Ok(other) => {
                    return Err(CassError(CassErrorKind::Error(
                        unexpected_response(&other).message,
                    )))
                }
                Err(e) => {
                    let current_error = CassError::query_execution_error(cql, params.as_ref(), e);
                    Box::pin(handle_retry_error(self, current_attempt_num, current_error)).await;
                    current_attempt_num += 1;
                    continue; // try again the same query
                }
            };
            if process_and_return_data {
                let objects = rows.objects.take();
                let decoded = match objects {
                    Some(_) => None,
                    None => Some(decode_rows::<RuneRow>(&rows)?),
                };
                for row in objects.into_iter().flatten() {
                    rune_rows.push(row).map_err(|_| {
                        CassError(CassErrorKind::Error(
                            "Failed to push row to result vector".to_string(),
                        ))
                    })?;
                }
                for row_result in decoded.into_iter().flatten() {
                    match row_result {
                        Ok(RuneRow(row_obj)) => {
                            rune_rows
                                .push(Value::new(row_obj).map_err(|_| {
                                    CassError(CassErrorKind::Error(
                                        "Failed to create shared row object".to_string(),
                                    ))
                                })?)
                                .map_err(|_| {
                                    CassError(CassErrorKind::Error(
                                        "Failed to push row to result vector".to_string(),
                                    ))
                                })?;
                        }
                        Err(_) => {
                            break; // Exit the loop if row_result is invalid
                        }
                    }
                }
                rows_num = rune_rows.len() as u64;
            } else {
                rows_num += rows.row_count;
                if is_select_count {
                    last_rows = Some(rows);
                }
            }
            all_pages_duration += current_duration;
            match paging_state_response {
                None => {
                    self.stats
                        .try_lock()
                        .unwrap()
                        .complete_request(all_pages_duration, rows_num);
                    if process_and_return_data {
                        return Value::vec(rune_rows.into_inner()).map_err(|_| {
                            CassError(CassErrorKind::Error(
                                "Failed to create shared result vector".to_string(),
                            ))
                        });
                    } else {
                        let empty_rune_vec = Value::vec(Default::default())?;
                        let rows_min = match expected_rows_num_min {
                            None => return Ok(empty_rune_vec),
                            Some(rows_min) => rows_min,
                        };
                        let (rows_max, mut rows_cnt) = (expected_rows_num_max.unwrap(), rows_num);
                        if is_select_count {
                            let rows = last_rows.take().expect("SELECT COUNT should have rows");
                            rows_cnt = match first_count(&rows) {
                                Some(count) => count as u64,
                                None => match decode_rows::<(i64,)>(&rows)?.next() {
                                    Some(Ok((count,))) => count as u64,
                                    Some(Err(e)) => {
                                        return Err(CassError(CassErrorKind::Error(e.to_string())))
                                    }
                                    None => {
                                        return Err(CassError(CassErrorKind::Error(
                                            "SELECT COUNT returned no rows".to_string(),
                                        )))
                                    }
                                },
                            };
                            if rows_num == 1 && rows_min <= rows_cnt && rows_cnt <= rows_max {
                                return Ok(empty_rune_vec); // SELECT COUNT(...) returned expected rows number
                            }
                        } else if rows_min <= rows_num && rows_num <= rows_max {
                            return Ok(empty_rune_vec); // Common 'SELECT' returned expected number of rows in total
                        }
                        let current_error = CassError::query_validation_error(
                            cql,
                            params.as_ref(),
                            rows_min,
                            rows_max,
                            rows_cnt,
                            custom_err_msg.unwrap_or("").to_string(),
                        );
                        match self.validation_strategy {
                            ValidationStrategy::Retry => {
                                Box::pin(handle_retry_error(
                                    self,
                                    current_attempt_num,
                                    current_error,
                                ))
                                .await;
                                current_attempt_num += 1;
                                rows_num = 0; // we retry all pages, so reset cnt
                                last_rows = None;
                                paging_state = None;
                                continue; // try again the same query
                            }
                            ValidationStrategy::FailFast => {
                                return Err(current_error); // stop stress execution
                            }
                            ValidationStrategy::Ignore => {
                                Box::pin(handle_retry_error(
                                    self,
                                    current_attempt_num,
                                    current_error,
                                ))
                                .await;
                                return Ok(empty_rune_vec); // handle/print error and go on.
                            }
                        }
                    }
                }
                Some(new_paging_state) => {
                    paging_state = Some(new_paging_state);
                    current_attempt_num = 0;
                    continue; // get next page
                }
            }
        }
        Err(CassError::query_retries_exceeded(self.retry_number))
    }

    pub async fn batch_prepared(
        &self,
        keys: Vec<&str>,
        params: Vec<Value>,
    ) -> Result<(), CassError> {
        let keys_len = keys.len();
        let params_len = params.len();
        if keys_len != params_len {
            return Err(CassError(CassErrorKind::Error(format!(
                "Number of prepared statements ({keys_len}) and values ({params_len}) must be equal"
            ))));
        } else if keys_len == 0 {
            return Err(CassError(CassErrorKind::Error("Empty batch".to_string())));
        }
        if self.reactor.is_none() {
            return Err(no_session());
        }
        let mut statements = Vec::with_capacity(keys_len);
        for (key, value) in keys.into_iter().zip(params.iter()) {
            let statement = self
                .statements
                .try_lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| {
                    CassError(CassErrorKind::PreparedStatementNotFound(key.to_string()))
                })?;
            let values = self.bind_values(&statement, Some(value), &statement.cql)?;
            statements.push((statement.stmt, values));
        }
        let mut current_attempt_num = 0;
        while current_attempt_num <= self.retry_number {
            self.stats.try_lock().unwrap().start_request();
            let completion = self
                .submit(OpKind::Batch {
                    statements: statements.clone(),
                })
                .await?;
            let duration =
                Duration::from_nanos(completion.end_ns.saturating_sub(completion.start_ns));
            match completion.result {
                Ok(_) => {
                    self.stats
                        .try_lock()
                        .unwrap()
                        .complete_request(duration, keys_len as u64);
                    return Ok(());
                }
                Err(e) => {
                    let current_error =
                        CassError(CassErrorKind::Error(format!("batch execution failed: {e}")));
                    Box::pin(handle_retry_error(self, current_attempt_num, current_error)).await;
                    current_attempt_num += 1;
                    continue;
                }
            }
        }
        Err(CassError::query_retries_exceeded(self.retry_number))
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

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        let mut stats = self.stats.try_lock().unwrap();
        let result = stats.clone();
        stats.reset();
        result
    }

    /// Resets query and request counters
    pub fn reset(&self) {
        self.stats.try_lock().unwrap().reset();
        *self.start_time.try_lock().unwrap() = clock::instant_at(
            self.reactor
                .as_ref()
                .map_or_else(clock::now_ns, |r| r.now_ns()),
        );
    }
}

/// The count of a `SELECT COUNT` page a native host decoded into script objects.
fn first_count(rows: &Rows) -> Option<i64> {
    let row = rows.objects.as_ref()?.first()?;
    let obj = row.borrow_ref::<Object>().ok()?;
    let (_, value) = obj.iter().next()?;
    value.as_signed().ok()
}

fn unexpected_response(response: &Response) -> DriverError {
    DriverError::core(format!(
        "Unexpected response from the driver host: {response:?}"
    ))
}

/// Serializes script values into the CQL wire format, typed by the bind markers of `stmt`.
#[allow(clippy::result_large_err)]
fn serialize_params(
    stmt: &PreparedStatement,
    params: Option<&Value>,
    cql: &str,
) -> Result<SerializedValues, CassError> {
    let ctx = RowSerializationContext::from_specs(&stmt.bind_specs);
    SerializedValues::from_serializable(&ctx, &RuneQueryParams::new(params)).map_err(|e| {
        CassError::query_execution_error(cql, params, DriverError::core(e.to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE: u64 = 501;

    fn test_context() -> Context {
        Context::new(
            None,
            PAGE_SIZE,
            "dc".to_string(),
            "rack".to_string(),
            0,
            RetryInterval::new("1,2").expect("failed to parse retry interval"),
            ValidationStrategy::Ignore,
        )
    }

    /// Ad-hoc statements must never be prepared: one server-side entry per unique
    /// CQL text evicts the prepared statement cache and makes the server answer
    /// with 'Unprepared' errors. Resolving with no session proves no PREPARE can
    /// happen, since preparing requires one.
    #[test]
    fn ad_hoc_statement_is_not_prepared() {
        let ctx = test_context();
        assert!(ctx.reactor.is_none());

        let cql = "SELECT pk, ck FROM latte.validation WHERE pk = 42 LIMIT 16";
        let target = ctx
            .resolve_target(Some(cql), None)
            .expect("ad-hoc statement should resolve without a session");

        match target {
            ExecTarget::AdHoc(ref stmt) => assert_eq!(stmt, cql),
            ExecTarget::Prepared(_) => panic!("ad-hoc statement must not be prepared"),
        }
        assert_eq!(target.cql(), cql);
    }

    #[test]
    fn unknown_prepared_statement_key_is_rejected() {
        let Err(err) = test_context().resolve_target(None, Some("missing")) else {
            panic!("unknown key should be rejected");
        };
        let CassError(CassErrorKind::PreparedStatementNotFound(key)) = &err else {
            panic!("unexpected error: {err}");
        };
        assert_eq!(key, "missing");
    }

    #[test]
    fn cql_and_key_are_mutually_exclusive() {
        let ctx = test_context();
        assert!(ctx.resolve_target(Some("SELECT 1"), Some("key")).is_err());
        assert!(ctx.resolve_target(None, None).is_err());
    }
}
