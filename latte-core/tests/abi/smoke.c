/* Loads the latte-core shared library with dlopen and runs a whole `latte run` with a null
 * driver that completes every op at once. CQL: every query succeeds with no rows, and
 * `prepare` returns one bigint marker. Alternator: every request gets an empty JSON object. */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "latte.h"

#define SYM(name) name##_t name##_p = (name##_t)dlsym(lib, #name); \
    if (!name##_p) { fprintf(stderr, "missing %s\n", #name); return 2; }

typedef uint32_t (*latte_abi_version_t)(void);
typedef uint64_t (*latte_clock_ns_t)(void);
typedef LatteRun *(*latte_main_t)(int, const LatteStr *, int *);
typedef LatteStr (*latte_connection_json_t)(const LatteRun *);
typedef int (*latte_set_driver_settings_t)(LatteRun *, LatteStr);
typedef int (*latte_begin_phase_t)(LatteRun *);
typedef LatteInstance *(*latte_take_instance_t)(LatteRun *, int);
typedef LatteRing *(*latte_ops_t)(LatteInstance *);
typedef LatteRing *(*latte_completions_t)(LatteInstance *);
typedef int (*latte_pump_t)(LatteInstance *, uint64_t, uint64_t *);
typedef void (*latte_instance_free_t)(LatteInstance *);
typedef int (*latte_end_phase_t)(LatteRun *);
typedef int (*latte_finish_t)(LatteRun *);

static LatteStr str_of(const char *s) {
    LatteStr out = {(const uint8_t *)s, strlen(s)};
    return out;
}

static void put_u16(uint8_t *buf, uint32_t *len, uint16_t v) {
    buf[(*len)++] = (uint8_t)(v >> 8);
    buf[(*len)++] = (uint8_t)v;
}

static unsigned long ddb_ops = 0;

/* Answers one op the way a driver would, without a database. */
static void answer(const LatteRecord *op, LatteRing *out, uint64_t now) {
    LatteRecord *rec = &out->records[out->len++];
    memset(rec, 0, sizeof *rec);
    rec->id = op->id;
    rec->kind = LATTE_STATUS_OK;
    rec->start_ns = now;
    rec->end_ns = now + 1000;
    rec->data_off = out->arena_len;
    uint8_t *data = out->arena + out->arena_len;
    uint32_t len = 0;
    if (op->kind >= LATTE_OP_DDB_CREATE_TABLE) {
        rec->flags = LATTE_RESP_JSON;
        data[len++] = '{';
        data[len++] = '}';
        ddb_ops++;
    } else if (op->kind == LATTE_OP_PREPARE) {
        /* One bind marker named "id" of type bigint (0x0002). */
        rec->flags = LATTE_RESP_PREPARED;
        put_u16(data, &len, 1);
        put_u16(data, &len, 2);
        data[len++] = 'i';
        data[len++] = 'd';
        put_u16(data, &len, 0x0002);
    } else if (op->kind == LATTE_OP_QUERY || op->kind == LATTE_OP_EXECUTE) {
        rec->flags = LATTE_RESP_PAGE;
        rec->rows = 0;
    } else if (op->kind == LATTE_OP_DATACENTERS) {
        rec->flags = LATTE_RESP_DATACENTERS;
        put_u16(data, &len, 0);
    } else {
        rec->flags = LATTE_RESP_DONE;
    }
    rec->data_len = len;
    out->arena_len += len;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: %s <liblatte_core> <latte|latte-alternator> <workload.rn> [args...]\n", argv[0]);
        return 2;
    }
    void *lib = dlopen(argv[1], RTLD_NOW);
    if (!lib) {
        fprintf(stderr, "dlopen: %s\n", dlerror());
        return 2;
    }
    SYM(latte_abi_version) SYM(latte_clock_ns) SYM(latte_main) SYM(latte_connection_json)
    SYM(latte_set_driver_settings) SYM(latte_begin_phase) SYM(latte_take_instance)
    SYM(latte_ops) SYM(latte_completions) SYM(latte_pump) SYM(latte_instance_free)
    SYM(latte_end_phase) SYM(latte_finish)

    printf("abi_version=%u\n", latte_abi_version_p());
    if (latte_abi_version_p() != LATTE_ABI_VERSION) return 3;

    /* The arguments go in as UTF-8 slices, with no NUL terminator needed. */
    LatteStr args[32];
    int nargs = 0;
    args[nargs++] = str_of(argv[2]);
    args[nargs++] = str_of("run");
    args[nargs++] = str_of(argv[3]);
    for (int i = 4; i < argc && nargs < 31; i++) args[nargs++] = str_of(argv[i]);
    int exit_code = -1;
    LatteRun *run = latte_main_p(nargs, args, &exit_code);
    if (!run) {
        fprintf(stderr, "latte_main returned no run, exit %d\n", exit_code);
        return 4;
    }
    LatteStr conn = latte_connection_json_p(run);
    printf("connection=%.*s...\n", conn.len < 60 ? (int)conn.len : 60, (const char *)conn.ptr);
    if (latte_set_driver_settings_p(run, str_of("{\"driver\":\"null\"}")) != LATTE_OK)
        return 6;

    unsigned long ops_seen = 0;
    int n;
    while ((n = latte_begin_phase_p(run)) > 0) {
        for (int i = 0; i < n; i++) {
            LatteInstance *inst = latte_take_instance_p(run, i);
            LatteRing *ops = latte_ops_p(inst);
            LatteRing *done = latte_completions_p(inst);
            for (;;) {
                uint64_t deadline = 0;
                int rc = latte_pump_p(inst, latte_clock_ns_p(), &deadline);
                if (rc < 0) {
                    fprintf(stderr, "pump failed: %d\n", rc);
                    return 5;
                }
                for (uint32_t k = 0; k < ops->len; k++) {
                    uint64_t now = latte_clock_ns_p();
                    if (ops->records[k].send_at_ns > now) {
                        fprintf(stderr, "op %llu released before its send time\n",
                                (unsigned long long)ops->records[k].id);
                        return 7;
                    }
                    if (ops->records[k].deadline_ns <= ops->records[k].send_at_ns) {
                        fprintf(stderr, "op %llu has no deadline\n",
                                (unsigned long long)ops->records[k].id);
                        return 8;
                    }
                    answer(&ops->records[k], done, now);
                    ops_seen++;
                }
                if (rc == LATTE_DONE || rc == LATTE_CANCELLED) break;
            }
            latte_instance_free_p(inst);
        }
        if (latte_end_phase_p(run) != 0) break;
    }
    int code = latte_finish_p(run);
    printf("ops=%lu dynamodb_ops=%lu exit=%d\n", ops_seen, ddb_ops, code);
    return code;
}
