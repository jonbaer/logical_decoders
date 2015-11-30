#include "connect.h"

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_REPLICATION_SLOT "bottledwater"
#define APP_NAME "bottledwater"

/* The name of the logical decoding output plugin with which the replication
 * slot is created. This must match the name of the Postgres extension. */
#define OUTPUT_PLUGIN "bottledwater"

#define check(err, call) { err = call; if (err) return err; }

#define ensure(context, call) { \
    if (call) { \
        fprintf(stderr, "%s: %s\n", progname, context->error); \
        exit_nicely(context); \
    } \
}

static char *progname;

void usage(void);
void parse_options(client_context_t context, int argc, char **argv);
static int print_begin_txn(void *context, uint64_t wal_pos, uint32_t xid);
static int print_commit_txn(void *context, uint64_t wal_pos, uint32_t xid);
static int print_table_schema(void *context, uint64_t wal_pos, Oid relid,
        const char *key_schema_json, size_t key_schema_len, avro_schema_t key_schema,
        const char *row_schema_json, size_t row_schema_len, avro_schema_t row_schema);
static int print_insert_row(void *context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val);
static int print_update_row(void *context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val);
static int print_delete_row(void *context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val);
void checkpoint(void *context, uint64_t wal_pos);
client_context_t init_client(void);
void exit_nicely(client_context_t context);


void usage() {
    fprintf(stderr,
            "Exports a snapshot of a PostgreSQL database, followed by a stream of changes.\n\n"
            "Usage:\n  %s [OPTION]...\n\nOptions:\n"
            "  -d, --postgres=postgres://user:pass@host:port/dbname    (required)\n"
            "                          Connection string or URI of the PostgreSQL server.\n"
            "  -s, --slot=slotname     Name of replication slot to use (default: %s)\n"
            "                          The slot is automatically created on first use.\n"
            "  -u, --allow-unkeyed     Allow export of tables that don't have a primary key.\n"
            "                          This is disallowed by default, because updates and\n"
            "                          deletes need a primary key to identify their row.\n",
            progname, DEFAULT_REPLICATION_SLOT);
    exit(1);
}

void parse_options(client_context_t context, int argc, char **argv) {
    static struct option options[] = {
        {"postgres",      required_argument, NULL, 'd'},
        {"slot",          required_argument, NULL, 's'},
        {"allow-unkeyed", no_argument,       NULL, 'u'},
        {NULL,            0,                 NULL,  0 }
    };

    progname = argv[0];

    int option_index;
    while (true) {
        int c = getopt_long(argc, argv, "d:s:u", options, &option_index);
        if (c == -1) break;

        switch (c) {
            case 'd':
                context->conninfo = strdup(optarg);
                break;
            case 's':
                context->repl.slot_name = strdup(optarg);
                break;
            case 'u':
                context->allow_unkeyed = true;
                break;
            default:
                usage();
        }
    }

    if (!context->conninfo || optind < argc) usage();
}

static int print_begin_txn(void *_context, uint64_t wal_pos, uint32_t xid) {
    client_context_t context = (client_context_t) _context;
    if (xid == 0) {
        fprintf(stderr, "Created replication slot \"%s\", capturing consistent snapshot \"%s\".\n",
                context->repl.slot_name, context->repl.snapshot_name);
    } else {
        printf("begin xid=%u wal_pos=%X/%X\n", xid, (uint32) (wal_pos >> 32), (uint32) wal_pos);
        checkpoint(context, wal_pos);
    }
    return 0;
}

static int print_commit_txn(void *_context, uint64_t wal_pos, uint32_t xid) {
    client_context_t context = (client_context_t) _context;
    if (xid == 0) {
        fprintf(stderr, "Snapshot complete, streaming changes from %X/%X.\n",
                (uint32) (wal_pos >> 32), (uint32) wal_pos);
        context->taking_snapshot = false;
    } else {
        printf("commit xid=%u wal_pos=%X/%X\n", xid, (uint32) (wal_pos >> 32), (uint32) wal_pos);
        checkpoint(context, wal_pos);
    }
    return 0;
}

static int print_table_schema(void *context, uint64_t wal_pos, Oid relid,
        const char *key_schema_json, size_t key_schema_len, avro_schema_t key_schema,
        const char *row_schema_json, size_t row_schema_len, avro_schema_t row_schema) {
    printf("new schema for relid=%u\n\tkey = %.*s\n\trow = %.*s\n", relid,
            (int) key_schema_len, key_schema_json,
            (int) row_schema_len, row_schema_json);
    return 0;
}

static int print_insert_row(void *context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val) {
    int err = 0;
    char *key_json, *new_json;
    const char *table_name = avro_schema_name(avro_value_get_schema(new_val));
    check(err, avro_value_to_json(new_val, 1, &new_json));

    if (key_val) {
        check(err, avro_value_to_json(key_val, 1, &key_json));
        printf("insert to %s: %s --> %s\n", table_name, key_json, new_json);
        free(key_json);
    } else {
        printf("insert to %s: %s\n", table_name, new_json);
    }

    free(new_json);
    if (err == 0) checkpoint(context, wal_pos);
    return err;
}

static int print_update_row(void *context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val) {
    int err = 0;
    char *key_json = NULL, *old_json = NULL, *new_json = NULL;
    const char *table_name = avro_schema_name(avro_value_get_schema(new_val));
    check(err, avro_value_to_json(new_val, 1, &new_json));

    if (old_val) check(err, avro_value_to_json(old_val, 1, &old_json));
    if (key_val) check(err, avro_value_to_json(key_val, 1, &key_json));

    if (key_json && old_json) {
        printf("update to %s: key %s: %s --> %s\n", table_name, key_json, old_json, new_json);
    } else if (old_json) {
        printf("update to %s: %s --> %s\n", table_name, old_json, new_json);
    } else if (key_json) {
        printf("update to %s: key %s: %s\n", table_name, key_json, new_json);
    } else {
        printf("update to %s: (?) --> %s\n", table_name, new_json);
    }

    if (key_json) free(key_json);
    if (old_json) free(old_json);
    free(new_json);
    if (err == 0) checkpoint(context, wal_pos);
    return err;
}

static int print_delete_row(void *context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val) {
    int err = 0;
    char *key_json = NULL, *old_json = NULL;
    const char *table_name = NULL;

    if (key_val) check(err, avro_value_to_json(key_val, 1, &key_json));
    if (old_val) {
        check(err, avro_value_to_json(old_val, 1, &old_json));
        table_name = avro_schema_name(avro_value_get_schema(old_val));
    }

    if (key_json && old_json) {
        printf("delete from %s: %s (was: %s)\n", table_name, key_json, old_json);
    } else if (old_json) {
        printf("delete from %s: %s\n", table_name, old_json);
    } else if (key_json) {
        printf("delete from relid %u: %s\n", relid, key_json);
    } else {
        printf("delete to relid %u (?)\n", relid);
    }

    if (key_json) free(key_json);
    if (old_json) free(old_json);
    if (err == 0) checkpoint(context, wal_pos);
    return err;
}

void checkpoint(void *context, uint64_t wal_pos) {
    replication_stream_t stream = &((client_context_t) context)->repl;
    stream->fsync_lsn = Max(wal_pos, stream->fsync_lsn);
}

client_context_t init_client() {
    frame_reader_t frame_reader = frame_reader_new();
    frame_reader->on_begin_txn    = print_begin_txn;
    frame_reader->on_commit_txn   = print_commit_txn;
    frame_reader->on_table_schema = print_table_schema;
    frame_reader->on_insert_row   = print_insert_row;
    frame_reader->on_update_row   = print_update_row;
    frame_reader->on_delete_row   = print_delete_row;

    client_context_t context = db_client_new();
    context->app_name = APP_NAME;
    context->allow_unkeyed = false;
    context->repl.slot_name = DEFAULT_REPLICATION_SLOT;
    context->repl.output_plugin = OUTPUT_PLUGIN;
    context->repl.frame_reader = frame_reader;
    frame_reader->cb_context = context;
    return context;
}

void exit_nicely(client_context_t context) {
    // If a snapshot was in progress and not yet complete, and an error occurred, try to
    // drop the replication slot, so that the snapshot is retried when the user tries again.
    if (context->taking_snapshot) {
        fprintf(stderr, "Dropping replication slot since the snapshot did not complete successfully.\n");
        if (replication_slot_drop(&context->repl) != 0) {
            fprintf(stderr, "%s: %s\n", progname, context->repl.error);
        }
    }

    frame_reader_free(context->repl.frame_reader);
    db_client_free(context);
    exit(1);
}

int main(int argc, char **argv) {
    client_context_t context = init_client();
    parse_options(context, argc, argv);
    ensure(context, db_client_start(context));

    if (!context->taking_snapshot) {
        fprintf(stderr, "Replication slot \"%s\" exists, streaming changes from %X/%X.\n",
                context->repl.slot_name,
                (uint32) (context->repl.start_lsn >> 32), (uint32) context->repl.start_lsn);
    }

    while (context->status >= 0) { /* TODO install signal handler for graceful shutdown */
        ensure(context, db_client_poll(context));

        if (context->status == 0) {
            ensure(context, db_client_wait(context));
        }
    }

    frame_reader_free(context->repl.frame_reader);
    db_client_free(context);
    return 0;
}
