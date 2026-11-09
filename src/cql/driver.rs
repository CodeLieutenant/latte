//! Runs the ops of the core on the Rust ScyllaDB driver.

use std::sync::{Arc, RwLock};

use latte_core::clock;
use latte_core::native::CoreRow;
use latte_core::ops::{
    ClientKind, Completion, CoreBytes, DriverError, Op, OpKind, Response, RowMode, Rows, StmtId,
};
use scylla::client::session::Session;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;

/// The session and the statements prepared on it, shared by every instance. Statements sit
/// at the index of the handle the core gave them.
pub struct Driver {
    session: Session,
    statements: RwLock<Vec<Option<Arc<PreparedStatement>>>>,
}

/// The CQL protocol code of a server error, as `latte_core::ops::codes` lists them.
pub fn db_error_code(e: &DbError) -> u32 {
    use latte_core::ops::codes;
    match e {
        DbError::AuthenticationError => codes::BAD_CREDENTIALS,
        DbError::Unavailable { .. } => codes::UNAVAILABLE,
        DbError::Overloaded => codes::OVERLOADED,
        DbError::IsBootstrapping => codes::IS_BOOTSTRAPPING,
        DbError::WriteTimeout { .. } => codes::WRITE_TIMEOUT,
        DbError::ReadTimeout { .. } => codes::READ_TIMEOUT,
        DbError::ReadFailure { .. } => codes::READ_FAILURE,
        DbError::WriteFailure { .. } => codes::WRITE_FAILURE,
        DbError::SyntaxError => codes::SYNTAX_ERROR,
        DbError::Unauthorized => codes::UNAUTHORIZED,
        DbError::Invalid => codes::INVALID,
        DbError::ConfigError => codes::CONFIG_ERROR,
        DbError::AlreadyExists { .. } => codes::ALREADY_EXISTS,
        DbError::RateLimitReached { .. } => codes::RATE_LIMIT,
        DbError::ServerError => 0x0000,
        DbError::ProtocolError => 0x000A,
        DbError::TruncateError => 0x1003,
        DbError::FunctionFailure { .. } => 0x1400,
        DbError::Unprepared { .. } => 0x2500,
        DbError::Other(code) => *code as u32,
        _ => 0,
    }
}

/// Maps an execution error to a server code and a client kind, with the variant name as
/// the class for the report.
pub fn driver_error(e: ExecutionError) -> DriverError {
    let class = format!("{e:?}")
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .next()
        .unwrap_or("ExecutionError")
        .to_string();
    let (code, kind) = match &e {
        ExecutionError::RequestTimeout(_) => (0, ClientKind::ClientTimeout),
        ExecutionError::EmptyPlan => (0, ClientKind::NoHostAvailable),
        ExecutionError::ConnectionPoolError(_) => (0, ClientKind::ConnectionLost),
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(db, _)) => {
            (db_error_code(db), ClientKind::None)
        }
        ExecutionError::LastAttemptError(RequestAttemptError::BrokenConnectionError(_)) => {
            (0, ClientKind::ConnectionLost)
        }
        _ => (0, ClientKind::Other),
    };
    DriverError::new(code, kind, class, e.to_string())
}

fn other(class: &str, e: impl std::fmt::Display) -> DriverError {
    DriverError::new(0, ClientKind::Other, class, e.to_string())
}

fn paging(state: &Option<CoreBytes>) -> PagingState {
    match state {
        Some(bytes) => PagingState::new_from_raw_bytes(bytes.as_slice()),
        None => PagingState::start(),
    }
}

fn next_page(response: PagingStateResponse) -> Option<CoreBytes> {
    match response {
        PagingStateResponse::HasMorePages { state } => {
            state.as_bytes_slice().map(|b| CoreBytes::from(b.to_vec()))
        }
        PagingStateResponse::NoMorePages => None,
    }
}

impl Driver {
    pub fn new(session: Session) -> Self {
        Driver {
            session,
            statements: RwLock::new(Vec::new()),
        }
    }

    fn statement(&self, stmt: StmtId) -> Result<Arc<PreparedStatement>, DriverError> {
        self.statements
            .read()
            .unwrap()
            .get(stmt as usize)
            .cloned()
            .flatten()
            .ok_or_else(|| {
                other(
                    "UnknownStatement",
                    format!("unknown statement handle {stmt}"),
                )
            })
    }

    /// Runs one op and stamps its start and end on the shared clock.
    pub async fn run(&self, op: Op) -> Completion {
        let start_ns = clock::now_ns();
        let result = self.run_kind(op.kind).await;
        Completion {
            id: op.id,
            start_ns,
            end_ns: clock::now_ns(),
            result,
        }
    }

    async fn run_kind(&self, kind: OpKind) -> Result<Response, DriverError> {
        match kind {
            OpKind::Prepare {
                stmt,
                cql,
                page_size,
            } => {
                let prepared = self
                    .session
                    .prepare(Statement::new(cql).with_page_size(page_size))
                    .await
                    .map_err(|e| other("DriverError", e))?;
                let bind_specs = prepared
                    .get_variable_col_specs()
                    .iter()
                    .map(|spec| spec.clone().into_owned())
                    .collect();
                let mut statements = self.statements.write().unwrap();
                let index = stmt as usize;
                if statements.len() <= index {
                    statements.resize(index + 1, None);
                }
                statements[index] = Some(Arc::new(prepared));
                Ok(Response::Prepared { bind_specs })
            }
            OpKind::Query {
                cql,
                page_size,
                paging_state,
                rows,
            } => {
                let statement = Statement::new(cql).with_page_size(page_size);
                let (result, response) = self
                    .session
                    .query_single_page(statement, (), paging(&paging_state))
                    .await
                    .map_err(driver_error)?;
                page(result, response, rows)
            }
            OpKind::Execute {
                stmt,
                values,
                paging_state,
                rows,
            } => {
                let prepared = self.statement(stmt)?;
                let (result, response) = self
                    .session
                    .execute_single_page(&prepared, &values, paging(&paging_state))
                    .await
                    .map_err(driver_error)?;
                page(result, response, rows)
            }
            OpKind::Batch { statements } => {
                let mut batch = Batch::new(BatchType::Logged);
                for (stmt, _) in &statements {
                    batch.append_statement((*self.statement(*stmt)?).clone());
                }
                let values: Vec<_> = statements.iter().map(|(_, values)| values).collect();
                self.session
                    .batch(&batch, &values)
                    .await
                    .map_err(driver_error)?;
                Ok(Response::Done)
            }
            OpKind::Alternator { op, .. } => Err(other(
                "UnsupportedOp",
                format!("this host runs CQL and cannot run DynamoDB {}", op.name()),
            )),
            OpKind::Datacenters => {
                let state = self.session.get_cluster_state();
                Ok(Response::Datacenters(
                    state
                        .get_nodes_info()
                        .iter()
                        .filter_map(|n| n.datacenter.clone())
                        .collect(),
                ))
            }
        }
    }
}

fn page(
    result: scylla::response::query_result::QueryResult,
    response: PagingStateResponse,
    mode: RowMode,
) -> Result<Response, DriverError> {
    let paging_state = next_page(response);
    let Ok(rows_result) = result.into_rows_result() else {
        return Ok(Response::Page {
            rows: Rows::default(),
            paging_state,
        });
    };
    let mut rows = Rows {
        row_count: rows_result.rows_num() as u64,
        ..Rows::default()
    };
    if mode == RowMode::Rows {
        let objects = rows_result
            .rows::<CoreRow>()
            .map_err(|e| other("DriverError", e))?
            .map(|row| row.map(|CoreRow(value)| value))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| other("DriverError", e))?;
        rows.objects = Some(objects);
    }
    Ok(Response::Page { rows, paging_state })
}

impl crate::host::OpRunner for Driver {
    fn run(&self, op: Op) -> impl std::future::Future<Output = Completion> {
        Driver::run(self, op)
    }
}
