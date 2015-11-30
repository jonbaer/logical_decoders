#ifndef CONNECT_H
#define CONNECT_H

#include "replication.h"

#define CLIENT_CONTEXT_ERROR_LEN 512

typedef struct {
    char *conninfo, *app_name;
    PGconn *sql_conn;
    replication_stream repl;
    bool allow_unkeyed;
    bool taking_snapshot;
    int status; /* 1 = message was processed on last poll; 0 = no data available right now; -1 = stream ended */
    char error[CLIENT_CONTEXT_ERROR_LEN];
} client_context;

typedef client_context *client_context_t;

client_context_t db_client_new(void);
void db_client_free(client_context_t context);
int db_client_start(client_context_t context);
int db_client_poll(client_context_t context);
int db_client_wait(client_context_t context);

#endif /* CONNECT_H */
