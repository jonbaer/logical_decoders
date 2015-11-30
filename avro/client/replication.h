#ifndef REPLICATION_H
#define REPLICATION_H

#include "protocol_client.h"
#include <avro.h>
#include <libpq-fe.h>
#include <postgres_fe.h>
#include <access/xlogdefs.h>

#define REPLICATION_STREAM_ERROR_LEN 512

typedef struct {
    char *slot_name, *output_plugin, *snapshot_name;
    PGconn *conn;
    XLogRecPtr start_lsn;
    XLogRecPtr recvd_lsn;
    XLogRecPtr fsync_lsn;
    int64 last_checkpoint;
    frame_reader_t frame_reader;
    int status; /* 1 = message was processed on last poll; 0 = no data available right now; -1 = stream ended */
    char error[REPLICATION_STREAM_ERROR_LEN];
} replication_stream;

typedef replication_stream *replication_stream_t;

int replication_slot_create(replication_stream_t stream);
int replication_slot_drop(replication_stream_t stream);
int replication_stream_check(replication_stream_t stream);
int replication_stream_start(replication_stream_t stream);
int replication_stream_poll(replication_stream_t stream);
int replication_stream_keepalive(replication_stream_t stream);

#endif /* REPLICATION_H */
