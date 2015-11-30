/* A client for the Postgres logical replication protocol, comparable to
 * pg_recvlogical but adapted to our needs. The protocol is documented here:
 * http://www.postgresql.org/docs/9.4/static/protocol-replication.html */

#include "replication.h"

#include <stdarg.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>

#include <datatype/timestamp.h>
#include <internal/pqexpbuffer.h>

#define CHECKPOINT_INTERVAL_SEC 10

// #define DEBUG 1

int replication_stream_finish(replication_stream_t stream);
int parse_keepalive_message(replication_stream_t stream, char *buf, int buflen);
int parse_xlogdata_message(replication_stream_t stream, char *buf, int buflen);
int send_checkpoint(replication_stream_t stream, int64 now);
void repl_error(replication_stream_t stream, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));
int64 current_time(void);
void sendint64(int64 i64, char *buf);
int64 recvint64(char *buf);


/* Send a CREATE_REPLICATION_SLOT ... LOGICAL command to the server. This is similar to
 * the pg_create_logical_replication_slot() function you can call from SQL, but with a
 * crucial addition: it exports a consistent snapshot which we can use to dump a copy
 * of the database contents at the start of the replication slot. Note that the snapshot
 * is deleted when the next command is sent to the server on this replication connection,
 * so the snapshot name should be used immediately after the replication slot has been
 * created.
 *
 * The server response to the CREATE_REPLICATION_SLOT doesn't seem to be documented
 * anywhere, but based on reading the code, it's a tuple with the following fields:
 *
 *   1. "slot_name": name of the slot that was created, as requested
 *   2. "consistent_point": LSN at which we became consistent
 *   3. "snapshot_name": exported snapshot's name
 *   4. "output_plugin": name of the output plugin, as requested
 */
int replication_slot_create(replication_stream_t stream) {
    if (!stream->slot_name || stream->slot_name[0] == '\0') {
        repl_error(stream, "slot_name must be set in replication stream");
        return EINVAL;
    }
    if (!stream->output_plugin || stream->output_plugin[0] == '\0') {
        repl_error(stream, "output_plugin must be set in replication stream");
        return EINVAL;
    }

    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL \"%s\"",
            stream->slot_name, stream->output_plugin);

    PGresult *res = PQexec(stream->conn, query->data);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        repl_error(stream, "Command failed: %s: %s", query->data, PQerrorMessage(stream->conn));
        goto error;
    }

    if (PQntuples(res) != 1 || PQnfields(res) != 4) {
        repl_error(stream, "Unexpected CREATE_REPLICATION_SLOT result (%d rows, %d fields)",
                PQntuples(res), PQnfields(res));
        goto error;
    }

    if (PQgetisnull(res, 0, 1) || PQgetisnull(res, 0, 2)) {
        repl_error(stream, "Unexpected null value in CREATE_REPLICATION_SLOT response");
        goto error;
    }

    uint32 h32, l32;
    if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &h32, &l32) != 2) {
        repl_error(stream, "Could not parse LSN: \"%s\"", PQgetvalue(res, 0, 1));
        goto error;
    }

    stream->start_lsn = ((uint64) h32) << 32 | l32;
    stream->snapshot_name = strdup(PQgetvalue(res, 0, 2));

    destroyPQExpBuffer(query);
    PQclear(res);
    return 0;

error:
    destroyPQExpBuffer(query);
    PQclear(res);
    return EIO;
}


/* Drops the replication slot, like the SQL function pg_drop_replication_slot(). */
int replication_slot_drop(replication_stream_t stream) {
    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query, "DROP_REPLICATION_SLOT \"%s\"", stream->slot_name);

    PGresult *res = PQexec(stream->conn, query->data);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        repl_error(stream, "Command failed: %s: %s", query->data, PQerrorMessage(stream->conn));
        destroyPQExpBuffer(query);
        PQclear(res);
        return EIO;
    }

    destroyPQExpBuffer(query);
    PQclear(res);
    return 0;
}


/* Checks that the connection to the database server supports logical replication. */
int replication_stream_check(replication_stream_t stream) {
    PGresult *res = PQexec(stream->conn, "IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        repl_error(stream, "IDENTIFY_SYSTEM failed: %s", PQerrorMessage(stream->conn));
        PQclear(res);
        return EIO;
    }

    if (PQntuples(res) != 1 || PQnfields(res) < 4) {
        repl_error(stream, "Unexpected IDENTIFY_SYSTEM result (%d rows, %d fields).",
                PQntuples(res), PQnfields(res));
        PQclear(res);
        return EIO;
    }

    /* Check that the database name (fourth column of the result tuple) is non-null,
     * implying a database-specific connection. */
    if (PQgetisnull(res, 0, 3)) {
        repl_error(stream, "Not using a database-specific replication connection.");
        PQclear(res);
        return EIO;
    }

    PQclear(res);
    return 0;
}


/* Starts streaming logical changes from replication slot stream->slot_name,
 * starting from position stream->start_lsn. */
int replication_stream_start(replication_stream_t stream) {
    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
            stream->slot_name,
            (uint32) (stream->start_lsn >> 32), (uint32) stream->start_lsn);

    PGresult *res = PQexec(stream->conn, query->data);

    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        repl_error(stream, "Could not send replication command \"%s\": %s",
                query->data, PQresultErrorMessage(res));
        PQclear(res);
        destroyPQExpBuffer(query);
        return EIO;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    return 0;
}


/* Finish off after the server stopped sending us COPY data. */
int replication_stream_finish(replication_stream_t stream) {
    PGresult *res = PQgetResult(stream->conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        repl_error(stream, "Replication stream was unexpectedly terminated: %s",
                PQresultErrorMessage(res));
        return EIO;
    }
    PQclear(res);
    return 0;
}


/* Tries to read and process one message from a replication stream, using async I/O.
 * Updates stream->status to 1 if a message was processed, 0 if there is no data
 * available right now, or -1 if the stream has ended. Does not block. */
int replication_stream_poll(replication_stream_t stream) {
    char *buf = NULL;
    int ret = PQgetCopyData(stream->conn, &buf, 1);
    int err = 0;

    if (ret < 0) {
        if (ret == -1) {
            err = replication_stream_finish(stream);
        } else {
            repl_error(stream, "Could not read from replication stream: %s",
                    PQerrorMessage(stream->conn));
            err = EIO;
        }
        if (buf) PQfreemem(buf);
        stream->status = ret;
        return err;
    }

    if (ret > 0) {
        stream->status = 1;
        switch (buf[0]) {
            case 'k':
                err = parse_keepalive_message(stream, buf, ret);
                break;
            case 'w':
                err = parse_xlogdata_message(stream, buf, ret);
                break;
            default:
                repl_error(stream, "Unknown streaming message type: \"%c\"", buf[0]);
                err = EIO;
        }
    } else {
        stream->status = 0;
    }

    /* Periodically let the server know up to which point we've consumed the stream. */
    if (!err) err = replication_stream_keepalive(stream);

    if (buf) PQfreemem(buf);
    return err;
}


/* Periodically sends a checkpoint ("Standby status update") message to the server.
 * This is required, as the server will otherwise consider the client dead and
 * close the connection. */
int replication_stream_keepalive(replication_stream_t stream) {
    int err = 0;
    if (stream->recvd_lsn != InvalidXLogRecPtr) {
        int64 now = current_time();
        if (now - stream->last_checkpoint > CHECKPOINT_INTERVAL_SEC * USECS_PER_SEC) {
            err = send_checkpoint(stream, now);
        }
    }
    return err;
}


/* Parses a "Primary keepalive message" received from the server. It is packed binary
 * with the following structure:
 *
 *   - Byte1('k'): Identifies the message as a sender keepalive.
 *   - Int64: The current end of WAL on the server.
 *   - Int64: The server's system clock at the time of transmission, as microseconds
 *            since midnight on 2000-01-01.
 *   - Byte1: 1 means that the client should reply to this message as soon as possible,
 *            to avoid a timeout disconnect. 0 otherwise.
 */
int parse_keepalive_message(replication_stream_t stream, char *buf, int buflen) {
    if (buflen < 1 + 8 + 8 + 1) {
        repl_error(stream, "Keepalive message too small: %d bytes", buflen);
        return EIO;
    }

    int offset = 1; // start with 1 to skip the initial 'k' byte

    XLogRecPtr wal_pos = recvint64(&buf[offset]); offset += 8;
    /* skip server clock timestamp */             offset += 8;
    bool reply_requested = buf[offset];           offset += 1;

    /* Not 100% sure whether it's semantically correct to update our LSN position here --
     * the keepalive message indicates the latest position on the server, which might not
     * necessarily correspond to the latest position on the client. But this is what
     * pg_recvlogical does, so it's probably ok. */
    stream->recvd_lsn = Max(wal_pos, stream->recvd_lsn);

#ifdef DEBUG
    fprintf(stderr, "Keepalive: wal_pos %X/%X, reply_requested %d\n",
            (uint32) (wal_pos >> 32), (uint32) wal_pos, reply_requested);
#endif

    if (reply_requested) {
        return send_checkpoint(stream, current_time());
    }
    return 0;
}


/* Parses a XLogData message received from the server. It is packed binary with the
 * following structure:
 *
 *   - Byte1('w'): Identifies the message as replication data.
 *   - Int64: The starting point of the WAL data in this message.
 *   - Int64: The current end of WAL on the server.
 *   - Int64: The server's system clock at the time of transmission, as microseconds
 *            since midnight on 2000-01-01.
 *   - Byte(n): The output from the logical replication output plugin.
 */
int parse_xlogdata_message(replication_stream_t stream, char *buf, int buflen) {
    int hdrlen = 1 + 8 + 8 + 8;

    if (buflen < hdrlen + 1) {
        repl_error(stream, "XLogData header too small: %d bytes", buflen);
        return EIO;
    }

    XLogRecPtr wal_pos = recvint64(&buf[1]);

#ifdef DEBUG
    fprintf(stderr, "XLogData: wal_pos %X/%X\n", (uint32) (wal_pos >> 32), (uint32) wal_pos);
#endif

    int err = parse_frame(stream->frame_reader, wal_pos, buf + hdrlen, buflen - hdrlen);
    if (err) {
        repl_error(stream, "Error parsing frame data: %s", avro_strerror());
    }

    stream->recvd_lsn = Max(wal_pos, stream->recvd_lsn);
    return err;
}


/* Send a "Standby status update" message to server, indicating the LSN up to which we
 * have received logs. This message is packed binary with the following structure:
 *
 *   - Byte1('r'): Identifies the message as a receiver status update.
 *   - Int64: The location of the last WAL byte + 1 received by the client.
 *   - Int64: The location of the last WAL byte + 1 stored durably by the client.
 *   - Int64: The location of the last WAL byte + 1 applied to the client DB.
 *   - Int64: The client's system clock, as microseconds since midnight on 2000-01-01.
 *   - Byte1: If 1, the client requests the server to reply to this message immediately.
 */
int send_checkpoint(replication_stream_t stream, int64 now) {
    char buf[1 + 8 + 8 + 8 + 8 + 1];
    int offset = 0;

    buf[offset] = 'r';                          offset += 1;
    sendint64(stream->recvd_lsn, &buf[offset]); offset += 8;
    sendint64(stream->fsync_lsn, &buf[offset]); offset += 8;
    sendint64(InvalidXLogRecPtr, &buf[offset]); offset += 8; // only used by physical replication
    sendint64(now,               &buf[offset]); offset += 8;
    buf[offset] = 0;                            offset += 1;

    if (PQputCopyData(stream->conn, buf, offset) <= 0 || PQflush(stream->conn)) {
        repl_error(stream, "Could not send checkpoint to server: %s",
                PQerrorMessage(stream->conn));
        return EIO;
    }

#ifdef DEBUG
    fprintf(stderr, "Checkpoint: recvd_lsn %X/%X, fsync_lsn %X/%X\n",
            (uint32) (stream->recvd_lsn >> 32), (uint32) stream->recvd_lsn,
            (uint32) (stream->fsync_lsn >> 32), (uint32) stream->fsync_lsn);
#endif

    stream->last_checkpoint = now;
    return 0;
}


/* Updates the stream's statically allocated error buffer with a message. */
void repl_error(replication_stream_t stream, char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(stream->error, REPLICATION_STREAM_ERROR_LEN, fmt, args);
    va_end(args);
}

/* Returns the current date and time (according to the local system clock) in the
 * representation used by Postgres: microseconds since midnight on 2000-01-01. */
int64 current_time() {
    int64 timestamp;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    timestamp = (int64) tv.tv_sec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
    timestamp = (timestamp * USECS_PER_SEC) + tv.tv_usec;

    return timestamp;
}

/* Converts an int64 to network byte order. */
void sendint64(int64 i64, char *buf) {
    uint32 i32 = htonl((uint32) (i64 >> 32));
    memcpy(&buf[0], &i32, 4);

    i32 = htonl((uint32) i64);
    memcpy(&buf[4], &i32, 4);
}

/* Converts an int64 from network byte order to native format.  */
int64 recvint64(char *buf) {
    uint32 h32, l32;

    memcpy(&h32, buf, 4);
    memcpy(&l32, buf + 4, 4);

    int64 result = ntohl(h32);
    result <<= 32;
    result |= ntohl(l32);
    return result;
}
