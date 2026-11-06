//! The driver-neutral boundary between the core and a driver host.
//!
//! The core writes an [`Op`] for every database call a script makes. The host runs the op
//! on its driver and answers with a [`Completion`]. Bound values travel already serialized
//! in the CQL wire format, typed by the bind markers the host returned for the statement.

use scylla_cql_core::frame::response::result::ColumnSpec;
use scylla_cql_core::frame::types::RawValue;
use scylla_cql_core::serialize::row::{RowSerializationContext, SerializeRow, SerializedValues};
use scylla_cql_core::serialize::writers::RowWriter;
use scylla_cql_core::serialize::SerializationError;
use std::fmt::{Display, Formatter};

pub type OpId = u64;

/// Handle of a prepared statement. The core assigns handles from 0 up, unique in the
/// process, so a host keeps its prepared statements in a plain array indexed by handle.
pub type StmtId = u64;

static NEXT_STMT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Assigns the handle of the next prepared statement.
pub fn next_stmt_id() -> StmtId {
    NEXT_STMT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug)]
pub struct Op {
    /// Unique in the run and never reused, so a late completion is dropped by id.
    pub id: OpId,
    /// The time on the shared clock the host should send the op at. The core releases an op
    /// once this time is reached, so a host may send every op it receives at once.
    pub send_at_ns: u64,
    /// The send time plus the run's request timeout. Without a completion by then, the core
    /// counts the op as a client timeout and drops a later completion.
    pub deadline_ns: u64,
    pub kind: OpKind,
}

#[derive(Debug)]
pub enum OpKind {
    /// Prepares `cql` and stores the statement at index `stmt` of the host's statement array.
    /// The host answers with [`Response::Prepared`].
    Prepare {
        stmt: StmtId,
        cql: String,
        page_size: i32,
    },
    /// Runs one page of an unprepared statement with no bound values.
    Query {
        cql: String,
        page_size: i32,
        paging_state: Option<Vec<u8>>,
        rows: RowMode,
    },
    /// Runs one page of a prepared statement.
    Execute {
        stmt: StmtId,
        values: Values,
        paging_state: Option<Vec<u8>>,
        rows: RowMode,
    },
    /// Runs a logged batch of prepared statements.
    Batch { statements: Vec<(StmtId, Values)> },
    /// Lists the datacenters of the nodes the driver knows about.
    Datacenters,
    /// Runs one DynamoDB operation. `body` is the request JSON of the DynamoDB wire protocol.
    /// The host answers with [`Response::Json`] holding the response JSON.
    Alternator { op: AlternatorOp, body: Json },
}

/// What a host returns of the rows of a page.
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowMode {
    /// No row data. The host reports the row count when its driver knows it for free.
    None = 0,
    /// Only the row count; no row data, no arena use.
    Count = 1,
    /// The rows themselves.
    Rows = 2,
}

impl RowMode {
    pub fn from_u32(v: u32) -> RowMode {
        match v & 3 {
            1 => RowMode::Count,
            2 => RowMode::Rows,
            _ => RowMode::None,
        }
    }
}

/// DynamoDB operations the Alternator scripting context uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlternatorOp {
    CreateTable,
    DeleteTable,
    DescribeTable,
    PutItem,
    GetItem,
    UpdateItem,
    DeleteItem,
    Query,
    Scan,
    BatchGetItem,
    BatchWriteItem,
    DescribeEndpoints,
}

impl AlternatorOp {
    pub const ALL: [AlternatorOp; 12] = [
        AlternatorOp::CreateTable,
        AlternatorOp::DeleteTable,
        AlternatorOp::DescribeTable,
        AlternatorOp::PutItem,
        AlternatorOp::GetItem,
        AlternatorOp::UpdateItem,
        AlternatorOp::DeleteItem,
        AlternatorOp::Query,
        AlternatorOp::Scan,
        AlternatorOp::BatchGetItem,
        AlternatorOp::BatchWriteItem,
        AlternatorOp::DescribeEndpoints,
    ];

    /// The operation name of the DynamoDB API, as in the `X-Amz-Target` header.
    pub fn name(self) -> &'static str {
        match self {
            AlternatorOp::CreateTable => "CreateTable",
            AlternatorOp::DeleteTable => "DeleteTable",
            AlternatorOp::DescribeTable => "DescribeTable",
            AlternatorOp::PutItem => "PutItem",
            AlternatorOp::GetItem => "GetItem",
            AlternatorOp::UpdateItem => "UpdateItem",
            AlternatorOp::DeleteItem => "DeleteItem",
            AlternatorOp::Query => "Query",
            AlternatorOp::Scan => "Scan",
            AlternatorOp::BatchGetItem => "BatchGetItem",
            AlternatorOp::BatchWriteItem => "BatchWriteItem",
            AlternatorOp::DescribeEndpoints => "DescribeEndpoints",
        }
    }

    /// The op kind code in the ring: 16 and up, in the order of [`AlternatorOp::ALL`].
    pub fn code(self) -> u32 {
        16 + Self::ALL.iter().position(|op| *op == self).unwrap() as u32
    }

    pub fn from_code(code: u32) -> Option<AlternatorOp> {
        code.checked_sub(16)
            .and_then(|i| Self::ALL.get(i as usize).copied())
    }
}

impl OpKind {
    pub fn code(&self) -> u32 {
        match self {
            OpKind::Prepare { .. } => 1,
            OpKind::Query { .. } => 2,
            OpKind::Execute { .. } => 3,
            OpKind::Batch { .. } => 4,
            OpKind::Datacenters => 5,
            OpKind::Alternator { op, .. } => op.code(),
        }
    }
}

/// The answer of the host to one op. `start_ns` and `end_ns` come from [`crate::clock::now_ns`]
/// read around the driver call, so request latency excludes the time the op waited in a ring.
#[derive(Debug)]
pub struct Completion {
    pub id: OpId,
    pub start_ns: u64,
    pub end_ns: u64,
    pub result: Result<Response, DriverError>,
}

#[derive(Debug)]
pub enum Response {
    Prepared {
        bind_specs: Vec<ColumnSpec<'static>>,
    },
    Page {
        rows: Rows,
        paging_state: Option<Vec<u8>>,
    },
    Datacenters(Vec<String>),
    /// The response JSON of a DynamoDB operation.
    Json(Json),
    Done,
}

/// Rows of one page. `data` holds every cell of every row in order, each cell as a CQL
/// `[bytes]`: a big-endian `i32` length (`-1` for null) followed by the bytes. `data` is empty
/// when the op did not ask for rows.
#[derive(Debug, Default)]
pub struct Rows {
    pub row_count: u64,
    pub specs: Vec<ColumnSpec<'static>>,
    pub data: Vec<u8>,
    /// Rows a native host decoded straight into script objects with
    /// [`crate::native::CoreRow`]. When set, `specs` and `data` are unused.
    pub objects: Option<Vec<rune::Value>>,
}

/// A DynamoDB request held as typed fields. A C host gets it written straight into the
/// arena as JSON text. A native host downcasts it to
/// [`crate::native::AlternatorRequest`] and reads the fields.
pub trait AlternatorBody: std::fmt::Debug {
    /// Writes the request body as JSON text.
    fn write_json(&self, out: &mut dyn std::io::Write) -> std::io::Result<()>;
    /// The request body as a JSON value.
    fn to_value(&self) -> serde_json::Value;
    fn as_any(&self) -> &dyn std::any::Any;
}

/// The JSON of a DynamoDB request or response. A C host gets and gives text. A native host
/// gets the typed request and gives the parsed response.
#[derive(Clone, Debug)]
pub enum Json {
    Text(String),
    Value(serde_json::Value),
    /// A request, shared by its retries and pages.
    Request(std::rc::Rc<dyn AlternatorBody>),
}

impl Json {
    /// The JSON text, made from the value or the request when needed.
    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        match self {
            Json::Text(t) => std::borrow::Cow::Borrowed(t),
            Json::Value(v) => std::borrow::Cow::Owned(v.to_string()),
            Json::Request(r) => {
                let mut out = Vec::new();
                r.write_json(&mut out).expect("a request always serializes");
                std::borrow::Cow::Owned(String::from_utf8(out).expect("JSON is UTF-8"))
            }
        }
    }

    /// The parsed value. Empty text is an empty object.
    pub fn into_value(self) -> Result<serde_json::Value, serde_json::Error> {
        match self {
            Json::Value(v) => Ok(v),
            Json::Text(t) if t.trim().is_empty() => {
                Ok(serde_json::Value::Object(Default::default()))
            }
            Json::Text(t) => serde_json::from_str(&t),
            Json::Request(r) => Ok(r.to_value()),
        }
    }
}

/// Bound values of a prepared statement. A C host gets them serialized to CQL bytes. A
/// native host gets the script values and serializes them straight into the driver's
/// `RowWriter` through [`SerializeRow`], with no copy in between.
#[derive(Clone, Debug)]
pub enum Values {
    Serialized(SerializedValues),
    Native(NativeValues),
}

/// Script values and the bind-marker types they serialize to.
#[derive(Clone)]
pub struct NativeValues {
    pub params: Option<rune::Value>,
    pub specs: std::rc::Rc<[ColumnSpec<'static>]>,
    pub(crate) serialize: fn(
        Option<&rune::Value>,
        &RowSerializationContext<'_>,
        &mut RowWriter<'_>,
    ) -> Result<(), SerializationError>,
}

impl std::fmt::Debug for NativeValues {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeValues")
            .field("columns", &self.specs.len())
            .finish()
    }
}

impl Values {
    /// The values as CQL bytes. Native values are serialized here.
    pub fn to_serialized(
        &self,
    ) -> Result<std::borrow::Cow<'_, SerializedValues>, SerializationError> {
        match self {
            Values::Serialized(v) => Ok(std::borrow::Cow::Borrowed(v)),
            Values::Native(n) => {
                let ctx = RowSerializationContext::from_specs(&n.specs);
                SerializedValues::from_serializable(&ctx, self).map(std::borrow::Cow::Owned)
            }
        }
    }

    pub fn element_count(&self) -> usize {
        match self {
            Values::Serialized(v) => v.element_count() as usize,
            Values::Native(n) => n.specs.len(),
        }
    }
}

impl SerializeRow for Values {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        match self {
            Values::Serialized(values) => {
                for value in values.iter() {
                    let cell = writer.make_cell_writer();
                    match value {
                        RawValue::Null => {
                            cell.set_null();
                        }
                        RawValue::Unset => {
                            cell.set_unset();
                        }
                        RawValue::Value(bytes) => {
                            cell.set_value(bytes).map_err(SerializationError::new)?;
                        }
                    }
                }
                Ok(())
            }
            Values::Native(n) => (n.serialize)(n.params.as_ref(), ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        self.element_count() == 0
    }
}

/// How a cycle treats an error: an `Overloaded` error fails the cycle and the run goes on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverErrorKind {
    Overloaded,
    Other,
}

/// What went wrong on the client side, as the host reports it.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientKind {
    /// The server answered with an error; see `server_code`.
    None = 0,
    ClientTimeout = 1,
    ConnectionLost = 2,
    NoHostAvailable = 3,
    ResultTooLarge = 4,
    Other = 5,
}

impl ClientKind {
    pub fn from_u8(v: u8) -> ClientKind {
        match v {
            0 => ClientKind::None,
            1 => ClientKind::ClientTimeout,
            2 => ClientKind::ConnectionLost,
            3 => ClientKind::NoHostAvailable,
            4 => ClientKind::ResultTooLarge,
            _ => ClientKind::Other,
        }
    }
}

/// CQL protocol error codes the core maps to categories.
pub mod codes {
    pub const NONE: u32 = 0;
    pub const BAD_CREDENTIALS: u32 = 0x0100;
    pub const UNAVAILABLE: u32 = 0x1000;
    pub const OVERLOADED: u32 = 0x1001;
    pub const IS_BOOTSTRAPPING: u32 = 0x1002;
    pub const WRITE_TIMEOUT: u32 = 0x1100;
    pub const READ_TIMEOUT: u32 = 0x1200;
    pub const READ_FAILURE: u32 = 0x1300;
    pub const WRITE_FAILURE: u32 = 0x1500;
    pub const SYNTAX_ERROR: u32 = 0x2000;
    pub const UNAUTHORIZED: u32 = 0x2100;
    pub const INVALID: u32 = 0x2200;
    pub const CONFIG_ERROR: u32 = 0x2300;
    pub const ALREADY_EXISTS: u32 = 0x2400;
    /// ScyllaDB's rate-limit error. Its wire code is negotiated per connection, so hosts
    /// report this fixed value for it.
    pub const RATE_LIMIT: u32 = 0xF000;
}

/// The one error list of the core. Every host error falls in exactly one category.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    Overloaded,
    Unavailable,
    ReadTimeout,
    WriteTimeout,
    ReadFailure,
    WriteFailure,
    Invalid,
    Unauthorized,
    Throttled,
    ClientTimeout,
    ConnectionLost,
    NoHostAvailable,
    ResultTooLarge,
    Other,
}

impl ErrorCategory {
    pub fn name(self) -> &'static str {
        match self {
            ErrorCategory::Overloaded => "overloaded",
            ErrorCategory::Unavailable => "unavailable",
            ErrorCategory::ReadTimeout => "read_timeout",
            ErrorCategory::WriteTimeout => "write_timeout",
            ErrorCategory::ReadFailure => "read_failure",
            ErrorCategory::WriteFailure => "write_failure",
            ErrorCategory::Invalid => "invalid",
            ErrorCategory::Unauthorized => "unauthorized",
            ErrorCategory::Throttled => "throttled",
            ErrorCategory::ClientTimeout => "client_timeout",
            ErrorCategory::ConnectionLost => "connection_lost",
            ErrorCategory::NoHostAvailable => "no_host_available",
            ErrorCategory::ResultTooLarge => "result_too_large",
            ErrorCategory::Other => "other",
        }
    }

    /// Assigns the category from the server code and the client kind only. For a DynamoDB
    /// error, which has no server code, `dynamodb_type` is the error type name.
    pub fn of(server_code: u32, client_kind: ClientKind, dynamodb_type: Option<&str>) -> Self {
        use codes as c;
        match client_kind {
            ClientKind::ClientTimeout => return ErrorCategory::ClientTimeout,
            ClientKind::ConnectionLost => return ErrorCategory::ConnectionLost,
            ClientKind::NoHostAvailable => return ErrorCategory::NoHostAvailable,
            ClientKind::ResultTooLarge => return ErrorCategory::ResultTooLarge,
            ClientKind::None | ClientKind::Other => {}
        }
        match server_code {
            c::OVERLOADED => return ErrorCategory::Overloaded,
            c::UNAVAILABLE | c::IS_BOOTSTRAPPING => return ErrorCategory::Unavailable,
            c::READ_TIMEOUT => return ErrorCategory::ReadTimeout,
            c::WRITE_TIMEOUT => return ErrorCategory::WriteTimeout,
            c::READ_FAILURE => return ErrorCategory::ReadFailure,
            c::WRITE_FAILURE => return ErrorCategory::WriteFailure,
            c::SYNTAX_ERROR | c::INVALID | c::CONFIG_ERROR | c::ALREADY_EXISTS => {
                return ErrorCategory::Invalid
            }
            c::UNAUTHORIZED | c::BAD_CREDENTIALS => return ErrorCategory::Unauthorized,
            c::RATE_LIMIT => return ErrorCategory::Throttled,
            _ => {}
        }
        match dynamodb_type.map(|t| t.rsplit('#').next().unwrap_or(t)) {
            Some(
                "ProvisionedThroughputExceededException"
                | "ThrottlingException"
                | "RequestLimitExceeded",
            ) => ErrorCategory::Throttled,
            Some(
                "ValidationException"
                | "ResourceNotFoundException"
                | "ResourceInUseException"
                | "SerializationException",
            ) => ErrorCategory::Invalid,
            Some(
                "AccessDeniedException"
                | "UnrecognizedClientException"
                | "MissingAuthenticationTokenException",
            ) => ErrorCategory::Unauthorized,
            Some("ServiceUnavailable" | "ServiceUnavailableException") => {
                ErrorCategory::Unavailable
            }
            Some("InternalServerError") => ErrorCategory::Other,
            _ => ErrorCategory::Other,
        }
    }

    /// Categories after which a cycle counts as failed and the run goes on.
    pub fn kind(self) -> DriverErrorKind {
        match self {
            ErrorCategory::Overloaded
            | ErrorCategory::ReadTimeout
            | ErrorCategory::WriteTimeout
            | ErrorCategory::Throttled
            | ErrorCategory::ClientTimeout => DriverErrorKind::Overloaded,
            _ => DriverErrorKind::Other,
        }
    }
}

/// An error a host reported. `class` and `message` are for the report only; the category
/// comes from `server_code` and `client_kind`.
#[derive(Clone, Debug)]
pub struct DriverError {
    pub server_code: u32,
    pub client_kind: ClientKind,
    pub class: String,
    pub message: String,
    pub category: ErrorCategory,
    pub kind: DriverErrorKind,
}

impl DriverError {
    /// An error of a CQL host.
    pub fn new(
        server_code: u32,
        client_kind: ClientKind,
        class: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::build(
            server_code,
            client_kind,
            class.into(),
            message.into(),
            false,
        )
    }

    /// An error of an Alternator host: `error_type` is the DynamoDB error type name.
    pub fn dynamodb(
        client_kind: ClientKind,
        error_type: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::build(0, client_kind, error_type.into(), message.into(), true)
    }

    fn build(
        server_code: u32,
        client_kind: ClientKind,
        class: String,
        message: String,
        dynamodb: bool,
    ) -> Self {
        let category =
            ErrorCategory::of(server_code, client_kind, dynamodb.then_some(class.as_str()));
        DriverError {
            server_code,
            client_kind,
            class,
            message,
            category,
            kind: category.kind(),
        }
    }

    /// An error the core raises itself, not one a driver reported.
    pub fn core(message: impl Into<String>) -> Self {
        Self::new(0, ClientKind::Other, "LatteCoreError", message)
    }

    /// The op passed its deadline before the host answered.
    pub fn deadline_passed() -> Self {
        Self::new(
            0,
            ClientKind::ClientTimeout,
            "DeadlinePassed",
            "no completion before the op deadline",
        )
    }
}

impl Display for DriverError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}: {}",
            self.category.name(),
            self.class,
            self.message
        )
    }
}

impl std::error::Error for DriverError {}

/// Appends one CQL `[bytes]` cell to a row buffer.
pub fn push_cell(data: &mut Vec<u8>, cell: Option<&[u8]>) {
    match cell {
        Some(bytes) => {
            data.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            data.extend_from_slice(bytes);
        }
        None => data.extend_from_slice(&(-1i32).to_be_bytes()),
    }
}
