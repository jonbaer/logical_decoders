#include "connect.h"
#include "replication.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <internal/pqexpbuffer.h>

/* Wrap around a function call to bail on error. */
#define check(err, call) { err = call; if (err) return err; }

/* Similar to the check() macro, but for calls to functions in the replication stream
 * module. Since those functions have their own error message buffer, if an error
 * occurs, we need to copy the message to our own context's buffer. */
#define checkRepl(err, context, call) { \
    err = call; \
    if (err) { \
        strncpy((context)->error, (context)->repl.error, CLIENT_CONTEXT_ERROR_LEN); \
        return err; \
    } \
}

void client_error(client_context_t context, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));
int exec_sql(client_context_t context, char *query);
int client_connect(client_context_t context);
int replication_slot_exists(client_context_t context, bool *exists);
int snapshot_start(client_context_t context);
int snapshot_poll(client_context_t context);
int snapshot_tuple(client_context_t context, PGresult *res, int row_number);


/* Allocates a client_context struct. After this is done and before
 * db_client_start() is called, various fields in the struct need to be
 * initialized. */
client_context_t db_client_new() {
    client_context_t context = malloc(sizeof(client_context));
    memset(context, 0, sizeof(client_context));
    return context;
}


/* Closes any network connections, if applicable, and frees the client_context struct. */
void db_client_free(client_context_t context) {
    if (context->sql_conn) PQfinish(context->sql_conn);
    if (context->repl.conn) PQfinish(context->repl.conn);
    free(context);
}


/* Connects to the Postgres server (using context->conninfo for server info and
 * context->app_name as client name), and checks whether replication slot
 * context->repl.slot_name already exists. If yes, sets up the context to start
 * receiving the stream of changes from that slot. If no, creates the slot, and
 * initiates the consistent snapshot. */
int db_client_start(client_context_t context) {
    int err = 0;
    bool slot_exists;

    check(err, client_connect(context));
    checkRepl(err, context, replication_stream_check(&context->repl));
    check(err, replication_slot_exists(context, &slot_exists));

    if (slot_exists) {
        PQfinish(context->sql_conn);
        context->sql_conn = NULL;
        context->taking_snapshot = false;

        checkRepl(err, context, replication_stream_start(&context->repl));
        return err;

    } else {
        context->taking_snapshot = true;
        checkRepl(err, context, replication_slot_create(&context->repl));
        check(err, snapshot_start(context));
        return err;
    }
}


/* Checks whether new data has arrived from the server (on either the snapshot
 * connection or the replication connection, as appropriate). If yes, it is
 * processed, and context->status is set to 1. If no data is available, this
 * function does not block, but returns immediately, and context->status is set
 * to 0. If the data stream has ended, context->status is set to -1. */
int db_client_poll(client_context_t context) {
    int err = 0;

    if (context->sql_conn) {
        /* To make PQgetResult() non-blocking, check PQisBusy() first */
        if (PQisBusy(context->sql_conn)) {
            context->status = 0;
            return err;
        }

        check(err, snapshot_poll(context));
        context->status = 1;

        /* If the snapshot is finished, switch over to the replication stream */
        if (!context->sql_conn) {
            checkRepl(err, context, replication_stream_start(&context->repl));
        }
        return err;

    } else {
        checkRepl(err, context, replication_stream_poll(&context->repl));
        context->status = context->repl.status;
        return err;
    }
}


/* Blocks until more data is received from the server. You don't have to use
 * this if you have your own select loop. */
int db_client_wait(client_context_t context) {
    fd_set input_mask;
    FD_ZERO(&input_mask);

    int rep_fd = PQsocket(context->repl.conn);
    int max_fd = rep_fd;
    FD_SET(rep_fd, &input_mask);

    if (context->sql_conn) {
        int sql_fd = PQsocket(context->sql_conn);
        if (sql_fd > max_fd) max_fd = sql_fd;
        FD_SET(sql_fd, &input_mask);
    }

    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;

    int ret = select(max_fd + 1, &input_mask, NULL, NULL, &timeout);

    if (ret == 0 || (ret < 0 && errno == EINTR)) {
        return 0; /* timeout or signal */
    }
    if (ret < 0) {
        client_error(context, "select() failed: %s", strerror(errno));
        return errno;
    }

    /* Data has arrived on the socket */
    if (!PQconsumeInput(context->repl.conn)) {
        client_error(context, "Could not receive replication data: %s",
                PQerrorMessage(context->repl.conn));
        return EIO;
    }
    if (context->sql_conn && !PQconsumeInput(context->sql_conn)) {
        client_error(context, "Could not receive snapshot data: %s",
                PQerrorMessage(context->sql_conn));
        return EIO;
    }
    return 0;
}


/* Updates the context's statically allocated error buffer with a message. */
void client_error(client_context_t context, char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(context->error, CLIENT_CONTEXT_ERROR_LEN, fmt, args);
    va_end(args);
}


/* Executes a SQL command that returns no results. */
int exec_sql(client_context_t context, char *query) {
    PGresult *res = PQexec(context->sql_conn, query);
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
        return 0;
    } else {
        client_error(context, "Query failed: %s: %s", query, PQerrorMessage(context->sql_conn));
        PQclear(res);
        return EIO;
    }
}


/* Establishes two network connections to a Postgres server: one for SQL, and one
 * for replication. context->conninfo contains the connection string or URL to connect
 * to, and context->app_name is the client name (which appears, for example, in
 * pg_stat_activity). Returns 0 on success. */
int client_connect(client_context_t context) {
    if (!context->conninfo || context->conninfo[0] == '\0') {
        client_error(context, "conninfo must be set in client context");
        return EINVAL;
    }
    if (!context->app_name || context->app_name[0] == '\0') {
        client_error(context, "app_name must be set in client context");
        return EINVAL;
    }

    context->sql_conn = PQconnectdb(context->conninfo);
    if (PQstatus(context->sql_conn) != CONNECTION_OK) {
        client_error(context, "Connection to database failed: %s", PQerrorMessage(context->sql_conn));
        return EIO;
    }

    /* Parse the connection string into key-value pairs */
    char *error = NULL;
    PQconninfoOption *parsed_opts = PQconninfoParse(context->conninfo, &error);
    if (!parsed_opts) {
        client_error(context, "Replication connection info: %s", error);
        PQfreemem(error);
        return EIO;
    }

    /* Copy the key-value pairs into a new structure with added replication options */
    PQconninfoOption *option;
    int optcount = 2; /* replication, fallback_application_name */
    for (option = parsed_opts; option->keyword != NULL; option++) {
        if (option->val != NULL && option->val[0] != '\0') optcount++;
    }

    const char **keys = malloc((optcount + 1) * sizeof(char *));
    const char **values = malloc((optcount + 1) * sizeof(char *));
    int i = 0;

    for (option = parsed_opts; option->keyword != NULL; option++) {
        if (option->val != NULL && option->val[0] != '\0') {
            keys[i] = option->keyword;
            values[i] = option->val;
            i++;
        }
    }

    keys[i] = "replication";               values[i] = "database";        i++;
    keys[i] = "fallback_application_name"; values[i] = context->app_name; i++;
    keys[i] = NULL;                        values[i] = NULL;

    int err = 0;
    context->repl.conn = PQconnectdbParams(keys, values, true);
    if (PQstatus(context->repl.conn) != CONNECTION_OK) {
        client_error(context, "Replication connection failed: %s", PQerrorMessage(context->repl.conn));
        err = EIO;
    }

    free(keys);
    free(values);
    PQconninfoFree(parsed_opts);
    return err;
}


/* Sets *exists to true if a replication slot with the name context->repl.slot_name
 * already exists, and false if not. In addition, if the slot already exists,
 * context->repl.start_lsn is filled in with the LSN at which the client should
 * restart streaming. */
int replication_slot_exists(client_context_t context, bool *exists) {
    if (!context->repl.slot_name || context->repl.slot_name[0] == '\0') {
        client_error(context, "repl.slot_name must be set in client context");
        return EINVAL;
    }

    int err = 0;
    Oid argtypes[] = { 19 }; // 19 == NAMEOID
    const char *args[] = { context->repl.slot_name };

    PGresult *res = PQexecParams(context->sql_conn,
            "SELECT restart_lsn FROM pg_replication_slots where slot_name = $1",
            1, argtypes, args, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        client_error(context, "Could not check for existing replication slot: %s",
                PQerrorMessage(context->sql_conn));
        err = EIO;
        goto done;
    }

    *exists = (PQntuples(res) > 0 && !PQgetisnull(res, 0, 0));

    if (*exists) {
        uint32 h32, l32;
        if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &h32, &l32) != 2) {
            client_error(context, "Could not parse restart LSN: \"%s\"", PQgetvalue(res, 0, 0));
            err = EIO;
            goto done;
        } else {
            context->repl.start_lsn = ((uint64) h32) << 32 | l32;
        }
    }

done:
    PQclear(res);
    return err;
}


/* Initiates the non-blocking capture of a consistent snapshot of the database,
 * using the exported snapshot context->repl.snapshot_name. */
int snapshot_start(client_context_t context) {
    if (!context->repl.snapshot_name || context->repl.snapshot_name[0] == '\0') {
        client_error(context, "snapshot_name must be set in client context");
        return EINVAL;
    }

    int err = 0;
    check(err, exec_sql(context, "BEGIN"));
    check(err, exec_sql(context, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"));

    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query, "SET TRANSACTION SNAPSHOT '%s'", context->repl.snapshot_name);
    check(err, exec_sql(context, query->data));
    destroyPQExpBuffer(query);

    Oid argtypes[] = { 25, 16 }; // 25 == TEXTOID, 16 == BOOLOID
    const char *args[] = { "%", context->allow_unkeyed ? "t" : "f" };

    if (!PQsendQueryParams(context->sql_conn,
                "SELECT bottledwater_export(table_pattern := $1, allow_unkeyed := $2)",
                2, argtypes, args, NULL, NULL, 1)) { // The final 1 requests results in binary format
        client_error(context, "Could not dispatch snapshot fetch: %s",
                PQerrorMessage(context->sql_conn));
        return EIO;
    }

    if (!PQsetSingleRowMode(context->sql_conn)) {
        client_error(context, "Could not activate single-row mode");
        return EIO;
    }

    // Invoke the begin-transaction callback with xid==0 to indicate start of snapshot
    begin_txn_cb begin_txn = context->repl.frame_reader->on_begin_txn;
    void *cb_context = context->repl.frame_reader->cb_context;
    if (begin_txn) {
        check(err, begin_txn(cb_context, context->repl.start_lsn, 0));
    }
    return 0;
}

/* Reads the next result row from the snapshot query, parses and processes it.
 * Blocks until a new row is available, if necessary. */
int snapshot_poll(client_context_t context) {
    int err = 0;
    PGresult *res = PQgetResult(context->sql_conn);

    /* null result indicates that there are no more rows */
    if (!res) {
        check(err, exec_sql(context, "COMMIT"));
        PQfinish(context->sql_conn);
        context->sql_conn = NULL;

        // Invoke the commit callback with xid==0 to indicate end of snapshot
        commit_txn_cb on_commit = context->repl.frame_reader->on_commit_txn;
        void *cb_context = context->repl.frame_reader->cb_context;
        if (on_commit) {
            check(err, on_commit(cb_context, context->repl.start_lsn, 0));
        }
        return 0;
    }

    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_SINGLE_TUPLE && status != PGRES_TUPLES_OK) {
        client_error(context, "While reading snapshot: %s: %s",
                PQresStatus(PQresultStatus(res)),
                PQresultErrorMessage(res));
        PQclear(res);
        return EIO;
    }

    int tuples = PQntuples(res);
    for (int tuple = 0; tuple < tuples; tuple++) {
        check(err, snapshot_tuple(context, res, tuple));
    }
    PQclear(res);
    return err;
}

/* Processes one tuple of the snapshot query result set. */
int snapshot_tuple(client_context_t context, PGresult *res, int row_number) {
    if (PQnfields(res) != 1) {
        client_error(context, "Unexpected response with %d fields", PQnfields(res));
        return EIO;
    }
    if (PQgetisnull(res, row_number, 0)) {
        client_error(context, "Unexpected null response value");
        return EIO;
    }
    if (PQfformat(res, 0) != 1) { /* format 1 == binary */
        client_error(context, "Unexpected response format: %d", PQfformat(res, 0));
        return EIO;
    }

    /* wal_pos == 0 == InvalidXLogRecPtr */
    int err = parse_frame(context->repl.frame_reader, 0, PQgetvalue(res, row_number, 0),
            PQgetlength(res, row_number, 0));
    if (err) {
        client_error(context, "Error parsing frame data: %s", avro_strerror());
    }
    return err;
}
