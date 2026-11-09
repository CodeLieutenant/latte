#ifndef LATTE_H
#define LATTE_H

/* Generated from latte-core/src/ffi.rs by cbindgen. Edit the Rust source, not this file. */

#include <stdint.h>
#include <stddef.h>

/*
 Version of this ABI. A host refuses to run with a core of another version.
 */
#define LATTE_ABI_VERSION 4

/*
 `latte_pump` results. Negative values are errors.
 */
#define LATTE_OK 0

#define LATTE_DONE 1

#define LATTE_DRAINING 2

#define LATTE_CANCELLED 3

#define LATTE_ERR_PANIC -1

#define LATTE_ERR_ARG -2

#define LATTE_ERR_DECODE -4

#define LATTE_ERR_BUSY -5

#define LATTE_RING_RECORDS 4096

#define LATTE_RING_ARENA (4 << 20)

/*
 Largest result a host may hand over through `latte_reserve`. A bigger result is
 reported as `LATTE_CLIENT_RESULT_TOO_LARGE`.
 */
#define LATTE_MAX_RESULT_BYTES (256 << 20)

/*
 CQL op kinds in [`LatteRecord::kind`] of the op ring.
 */
#define LATTE_OP_PREPARE 1

#define LATTE_OP_QUERY 2

#define LATTE_OP_EXECUTE 3

#define LATTE_OP_BATCH 4

#define LATTE_OP_DATACENTERS 5

/*
 DynamoDB op kinds. The text range holds the operation name, the data range the request
 JSON of the DynamoDB wire protocol.
 */
#define LATTE_OP_DDB_CREATE_TABLE 16

#define LATTE_OP_DDB_DELETE_TABLE 17

#define LATTE_OP_DDB_DESCRIBE_TABLE 18

#define LATTE_OP_DDB_PUT_ITEM 19

#define LATTE_OP_DDB_GET_ITEM 20

#define LATTE_OP_DDB_UPDATE_ITEM 21

#define LATTE_OP_DDB_DELETE_ITEM 22

#define LATTE_OP_DDB_QUERY 23

#define LATTE_OP_DDB_SCAN 24

#define LATTE_OP_DDB_BATCH_GET_ITEM 25

#define LATTE_OP_DDB_BATCH_WRITE_ITEM 26

#define LATTE_OP_DDB_DESCRIBE_ENDPOINTS 27

/*
 Row mode in the low two bits of [`LatteRecord::flags`] of a query or execute op.
 None: return no row data; set `rows` when the driver knows the count for free.
 Count: return only the row count in `rows`, with no row data and no arena use.
 Rows: return the column specs and the cells.
 Paging is the core's job in every mode: the host runs one page per op and returns the
 paging state, and the core requests the next page and sums the counts.
 */
#define LATTE_ROWS_NONE 0

#define LATTE_ROWS_COUNT 1

#define LATTE_ROWS_ALL 2

/*
 Status in [`LatteRecord::kind`] of a completion.
 */
#define LATTE_STATUS_OK 0

#define LATTE_STATUS_ERROR 1

/*
 Response layouts in [`LatteRecord::flags`] of a successful completion.
 */
#define LATTE_RESP_DONE 0

#define LATTE_RESP_PREPARED 1

#define LATTE_RESP_PAGE 2

#define LATTE_RESP_DATACENTERS 3

#define LATTE_RESP_JSON 4

/*
 Client kinds in [`LatteRecord::client_kind`] of an error completion.
 */
#define LATTE_CLIENT_NONE 0

#define LATTE_CLIENT_TIMEOUT 1

#define LATTE_CLIENT_CONNECTION_LOST 2

#define LATTE_CLIENT_NO_HOST_AVAILABLE 3

#define LATTE_CLIENT_RESULT_TOO_LARGE 4

#define LATTE_CLIENT_OTHER 5

#define NONE 0

#define BAD_CREDENTIALS 256

#define UNAVAILABLE 4096

#define OVERLOADED 4097

#define IS_BOOTSTRAPPING 4098

#define WRITE_TIMEOUT 4352

#define READ_TIMEOUT 4608

#define READ_FAILURE 4864

#define WRITE_FAILURE 5376

#define SYNTAX_ERROR 8192

#define UNAUTHORIZED 8448

#define INVALID 8704

#define CONFIG_ERROR 8960

#define ALREADY_EXISTS 9216

/*
 ScyllaDB's rate-limit error. Its wire code is negotiated per connection, so hosts
 report this fixed value for it.
 */
#define RATE_LIMIT 61440

typedef struct LatteInstance LatteInstance;

/*
 A run, as seen by a C host. The cancel token sits outside the run so `latte_cancel` never
 touches memory another thread may be changing.
 */
typedef struct LatteRun LatteRun;

/*
 Borrowed UTF-8 text: `len` bytes at `ptr`, with no NUL terminator. `ptr` may be null when
 `len` is 0. The side that makes it keeps the bytes alive; each function that takes or
 returns one states for how long.
 */
typedef struct LatteStr {
  const uint8_t *ptr;
  size_t len;
} LatteStr;

/*
 One op or one completion at a fixed offset in a ring. Hosts read and write the fields in
 place. Numbers inside the arena are big-endian, as in the CQL native protocol.

 An op:
 * `kind` is a `LATTE_OP_*` value, `flags` holds a `LATTE_ROWS_*` row mode.
 * `id` is unique in the run and never reused.
 * `send_at_ns` is the time on `latte_clock_ns` to send the op at; `latte_pump` only hands
   out ops whose time has come. `deadline_ns` is the send time plus the request timeout:
   pass that timeout to the driver. The core times the op out itself at `deadline_ns`
   and drops a later completion.
 * `stmt` is the statement handle: the array index to store a prepared statement at, or
   the statement to execute. `page_size` applies to prepare and query.
 * The text range holds the CQL of prepare and query, or the DynamoDB operation name.
   The paging range holds the paging state of the page to fetch.
 * The data range holds the bound values of execute: the CQL `[value]` sequence of the
   statement (`i32` length then bytes, -1 null, -2 unset). A batch holds per statement:
   `u64` handle, `u16` value count, the values. A DynamoDB op holds the request JSON.

 A completion:
 * `id` repeats the op id. `start_ns` and `end_ns` are `latte_clock_ns` readings around the
   driver call.
 * `kind` is `LATTE_STATUS_OK` or `LATTE_STATUS_ERROR`.
 * On error, `server_code` is the CQL protocol error code (0 if none), `client_kind` a
   `LATTE_CLIENT_*` value, the text range the exception class name (for DynamoDB, the
   error type) and the data range its message. The core assigns the error category from
   `server_code` and `client_kind`, and from the DynamoDB error type.
 * On success, `flags` is a `LATTE_RESP_*` layout of the data range:
   prepared: the column specs of the bind markers;
   page: the column specs, then the cells of `rows` rows, each a CQL `[bytes]`, with the
   paging state of the next page in the paging range;
   datacenters: `u16` count, then per name a `u16` length and UTF-8 bytes;
   json: the response JSON of a DynamoDB op; done: nothing.
 * Column specs are a `u16` count, then per column a CQL `[string]` name and a CQL
   `[option]` type, the encoding of result metadata in the native protocol.
 * `buffer` is null when the ranges point into the ring arena. A result too big for the
   arena goes into a buffer from `latte_reserve`; `buffer` then holds its address and the
   ranges point into it.
 */
typedef struct LatteRecord {
  uint64_t id;
  uint64_t send_at_ns;
  uint64_t deadline_ns;
  uint64_t start_ns;
  uint64_t end_ns;
  uint64_t stmt;
  uint64_t rows;
  uint8_t *buffer;
  uint32_t kind;
  uint32_t flags;
  int32_t page_size;
  uint32_t server_code;
  uint8_t client_kind;
  uint8_t reserved[7];
  uint32_t text_off;
  uint32_t text_len;
  uint32_t paging_off;
  uint32_t paging_len;
  uint32_t data_off;
  uint32_t data_len;
} LatteRecord;

/*
 A ring: `len` valid records and `arena_len` used arena bytes. Plain memory, owned by the
 owner of the instance.
 */
typedef struct LatteRing {
  uint32_t len;
  uint32_t arena_len;
  struct LatteRecord records[LATTE_RING_RECORDS];
  uint8_t arena[LATTE_RING_ARENA];
} LatteRing;

/*
 Borrowed bytes: `len` bytes at `ptr`. `ptr` may be null when `len` is 0. The side that
 makes it keeps the bytes alive; each function that takes or returns one states for how
 long.
 */
typedef struct LatteBytes {
  const uint8_t *ptr;
  size_t len;
} LatteBytes;



#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/*
 Returns `LATTE_ABI_VERSION`. Safe from any thread.
 */
uint32_t latte_abi_version(void);

/*
 Reads the clock that op times and pump times use, in nanoseconds since the first read in
 the process. Safe from any thread.

 Hosts must stamp the start and end of every driver call with this function and with no
 other clock, so the core and the host share one time base. It reads the CPU counter
 directly through `quanta` (`cntvct_el0` on aarch64, an invariant TSC on x86_64, else
 `CLOCK_MONOTONIC`): one instruction and a multiply, with no system call.
 */
uint64_t latte_clock_ns(void);

/*
 The last error of the calling thread, with no copy. Empty when there is none. Safe from
 any thread; each thread has its own last error. The text stays valid until the calling
 thread makes its next call into the core.
 */
struct LatteStr latte_last_error(void);

/*
 Parses the Latte command line. Commands that need no database run to completion and
 return null with `*exit_code` set. `schema`, `load` and `run` return a run handle.
 Call it from the thread that will drive the run.

 # Safety
 `argv` must point to `argc` strings, each valid for the length of the call. The core
 keeps no pointer into them.
 */
struct LatteRun *latte_main(int argc, const struct LatteStr *argv, int *exit_code);

/*
 Connection settings as JSON: `backend` ("cql" or "alternator"), `client_id`,
 `connection` and `secrets`. A host that does not serve the backend must stop with an
 error. The text is UTF-8 with no NUL terminator, and stays valid until `latte_finish`.
 Empty for a null run. Call it from the thread that drives the run.

 # Safety
 `run` must come from `latte_main`.
 */
struct LatteStr latte_connection_json(const struct LatteRun *run);

/*
 Asks the run to stop. A single atomic store: safe from any thread and from a signal
 handler. Pumps then release no new op, drain the ops in flight, and end with
 `LATTE_CANCELLED`. `latte_finish` still writes the report, marked cancelled.

 # Safety
 `run` must come from `latte_main` and not be finished.
 */
void latte_cancel(const struct LatteRun *run);

/*
 Returns 1 after `latte_cancel`, else 0. Safe from any thread.

 # Safety
 `run` must come from `latte_main` and not be finished.
 */
int latte_is_cancelled(const struct LatteRun *run);

/*
 Stores the effective settings of the host's driver, as a JSON document. The core writes
 them into the report. Returns `LATTE_OK`, or `LATTE_ERR_ARG` when the text is not valid
 JSON. Call it from the thread that drives the run.

 # Safety
 `run` must come from `latte_main`. `json` must be valid for the length of the call; the
 core keeps no pointer into it.
 */
int latte_set_driver_settings(struct LatteRun *run, struct LatteStr json);

/*
 Starts the next phase. Returns the number of instances to pump, or 0 when the command
 is complete, failed or cancelled. Call it from the thread that drives the run.

 # Safety
 `run` must come from `latte_main`.
 */
int latte_begin_phase(struct LatteRun *run);

/*
 Takes instance `index` of the current phase. Returns null if it was taken already.
 The caller becomes the owner of the instance. Call it from the thread that drives the run.

 # Safety
 `run` must come from `latte_main`.
 */
struct LatteInstance *latte_take_instance(struct LatteRun *run, int index);

/*
 Op ring of an instance. Only the owner reads it, after its own `latte_pump`.

 # Safety
 `instance` must come from `latte_take_instance`.
 */
struct LatteRing *latte_ops(struct LatteInstance *instance);

/*
 Completion ring of an instance. Only the owner writes it, before its next `latte_pump`.

 # Safety
 `instance` must come from `latte_take_instance`.
 */
struct LatteRing *latte_completions(struct LatteInstance *instance);

/*
 Hands over `n` completions made on any thread. Their ranges point into the `len` bytes at
 `bytes`, or into a `latte_reserve` buffer when `buffer` is set. The core copies the batch
 and publishes it with one atomic operation; the next `latte_pump` applies it.

 # Safety
 `instance` must come from `latte_take_instance`. `records` must point to `n` records and
 `bytes` to `len` readable bytes (or be null with `len` 0).
 */
int latte_submit(const struct LatteInstance *instance,
                 const struct LatteRecord *records,
                 size_t n,
                 const uint8_t *bytes,
                 size_t len);

/*
 Allocates a core-owned buffer of `bytes` bytes for the result of op `op_id` and returns
 its address, or null above `LATTE_MAX_RESULT_BYTES`. The completion of the op points to
 it through `buffer`; the core frees it after reading the completion. Safe from any thread.

 # Safety
 `instance` must come from `latte_take_instance`.
 */
uint8_t *latte_reserve(const struct LatteInstance *instance, uint64_t op_id, size_t bytes);

/*
 Applies the completion ring and the inbox, runs the instance and fills the op ring.
 Clears the completion ring. Returns `LATTE_OK` (running), `LATTE_DRAINING`,
 `LATTE_DONE` or `LATTE_CANCELLED`, and the next deadline on the shared clock in
 `*deadline_ns` (0 for none). Only the owner may call it; an overlapping call returns
 `LATTE_ERR_BUSY`.

 # Safety
 `instance` must come from `latte_take_instance`. `deadline_ns` may be null.
 */
int latte_pump(struct LatteInstance *instance, uint64_t now_ns, uint64_t *deadline_ns);

/*
 Frees an instance after its last pump. Called by the owner.

 # Safety
 `instance` must come from `latte_take_instance` and not be used afterwards.
 */
void latte_instance_free(struct LatteInstance *instance);

/*
 Ends the current phase. Returns `LATTE_OK`, or 1 when the phase failed or the run was
 cancelled. Call it from the thread that drives the run.

 # Safety
 `run` must come from `latte_main`.
 */
int latte_end_phase(struct LatteRun *run);

/*
 Prints the report, frees the run and returns the process exit code. Call it from the
 thread that drives the run, after every instance is freed.

 # Safety
 `run` must come from `latte_main` and not be used afterwards.
 */
int latte_finish(struct LatteRun *run);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* LATTE_H */
