#include "connect.h"
#include "registry.h"

#include <librdkafka/rdkafka.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#define DEFAULT_REPLICATION_SLOT "bottledwater"
#define APP_NAME "bottledwater"

/* The name of the logical decoding output plugin with which the replication
 * slot is created. This must match the name of the Postgres extension. */
#define OUTPUT_PLUGIN "bottledwater"

#define DEFAULT_BROKER_LIST "localhost:9092"
#define DEFAULT_SCHEMA_REGISTRY "http://localhost:8081"

#define check(err, call) { err = call; if (err) return err; }

#define ensure(context, call) { \
    if (call) { \
        fprintf(stderr, "%s: %s\n", progname, (context)->client->error); \
        exit_nicely(context, 1); \
    } \
}

#define PRODUCER_CONTEXT_ERROR_LEN 512
#define MAX_IN_FLIGHT_TRANSACTIONS 1000

typedef struct {
    uint32_t xid;         /* Postgres transaction identifier */
    int recvd_events;     /* Number of row-level events received so far for this transaction */
    int pending_events;   /* Number of row-level events waiting to be acknowledged by Kafka */
    uint64_t commit_lsn;  /* WAL position of the transaction's commit event */
} transaction_info;

typedef struct {
    client_context_t client;            /* The connection to Postgres */
    schema_registry_t registry;         /* Submits Avro schemas to schema registry */
    char *brokers;                      /* Comma-separated list of host:port for Kafka brokers */
    transaction_info xact_list[MAX_IN_FLIGHT_TRANSACTIONS]; /* Circular buffer */
    int xact_head;                      /* Index into xact_list currently being received from PG */
    int xact_tail;                      /* Oldest index in xact_list not yet acknowledged by Kafka */
    rd_kafka_conf_t *kafka_conf;
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_t *kafka;
    char error[PRODUCER_CONTEXT_ERROR_LEN];
} producer_context;

typedef producer_context *producer_context_t;

#define xact_list_full(context) \
    (((context)->xact_head + 1) % MAX_IN_FLIGHT_TRANSACTIONS == (context)->xact_tail)

typedef struct {
    producer_context_t context;
    uint64_t wal_pos;
    Oid relid;
    transaction_info *xact;
} msg_envelope;

typedef msg_envelope *msg_envelope_t;

static char *progname;
static bool received_sigint = false;

void usage(void);
void parse_options(producer_context_t context, int argc, char **argv);
char *parse_config_option(char *option);
void set_kafka_config(producer_context_t context, char *property, char *value);
void set_topic_config(producer_context_t context, char *property, char *value);
static int on_begin_txn(void *_context, uint64_t wal_pos, uint32_t xid);
static int on_commit_txn(void *_context, uint64_t wal_pos, uint32_t xid);
static int on_table_schema(void *_context, uint64_t wal_pos, Oid relid,
        const char *key_schema_json, size_t key_schema_len, avro_schema_t key_schema,
        const char *row_schema_json, size_t row_schema_len, avro_schema_t row_schema);
static int on_insert_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val);
static int on_update_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val);
static int on_delete_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val);
int send_kafka_msg(producer_context_t context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len,
        const void *val_bin, size_t val_len);
static void on_deliver_msg(rd_kafka_t *kafka, const rd_kafka_message_t *msg, void *envelope);
void maybe_checkpoint(producer_context_t context);
void backpressure(producer_context_t context);
client_context_t init_client(void);
producer_context_t init_producer(client_context_t client);
void start_producer(producer_context_t context);
void exit_nicely(producer_context_t context, int status);


void usage() {
    fprintf(stderr,
            "Exports a snapshot of a PostgreSQL database, followed by a stream of changes,\n"
            "and sends the data to a Kafka cluster.\n\n"
            "Usage:\n  %s [OPTION]...\n\nOptions:\n"
            "  -d, --postgres=postgres://user:pass@host:port/dbname   (required)\n"
            "                          Connection string or URI of the PostgreSQL server.\n"
            "  -s, --slot=slotname     Name of replication slot   (default: %s)\n"
            "                          The slot is automatically created on first use.\n"
            "  -b, --broker=host1[:port1],host2[:port2]...   (default: %s)\n"
            "                          Comma-separated list of Kafka broker hosts/ports.\n"
            "  -r, --schema-registry=http://hostname:port   (default: %s)\n"
            "                          URL of the service where Avro schemas are registered.\n"
            "  -u, --allow-unkeyed     Allow export of tables that don't have a primary key.\n"
            "                          This is disallowed by default, because updates and\n"
            "                          deletes need a primary key to identify their row.\n"
            "  -C, --kafka-config property=value\n"
            "                          Set global configuration property for Kafka producer\n"
            "                          (see --config-help for list of properties).\n"
            "  -T, --topic-config property=value\n"
            "                          Set topic configuration property for Kafka producer.\n"
            "  --config-help           Print the list of configuration properties. See also:\n"
            "            https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md\n",
            progname, DEFAULT_REPLICATION_SLOT, DEFAULT_BROKER_LIST, DEFAULT_SCHEMA_REGISTRY);
    exit(1);
}

/* Parse command-line options */
void parse_options(producer_context_t context, int argc, char **argv) {

    static struct option options[] = {
        {"postgres",        required_argument, NULL, 'd'},
        {"slot",            required_argument, NULL, 's'},
        {"broker",          required_argument, NULL, 'b'},
        {"schema-registry", required_argument, NULL, 'r'},
        {"allow-unkeyed",   no_argument,       NULL, 'u'},
        {"kafka-config",    required_argument, NULL, 'C'},
        {"topic-config",    required_argument, NULL, 'T'},
        {"config-help",     no_argument,       NULL,  1 },
        {NULL,              0,                 NULL,  0 }
    };

    progname = argv[0];

    int option_index;
    while (true) {
        int c = getopt_long(argc, argv, "d:s:b:r:uC:T:", options, &option_index);
        if (c == -1) break;

        switch (c) {
            case 'd':
                context->client->conninfo = strdup(optarg);
                break;
            case 's':
                context->client->repl.slot_name = strdup(optarg);
                break;
            case 'b':
                context->brokers = strdup(optarg);
                break;
            case 'r':
                schema_registry_set_url(context->registry, optarg);
                break;
            case 'u':
                context->client->allow_unkeyed = true;
                break;
            case 'C':
                set_kafka_config(context, optarg, parse_config_option(optarg));
                break;
            case 'T':
                set_topic_config(context, optarg, parse_config_option(optarg));
                break;
            case 1:
                rd_kafka_conf_properties_show(stderr);
                exit(1);
                break;
            default:
                usage();
        }
    }

    if (!context->client->conninfo || optind < argc) usage();
}

/* Splits an option string by equals sign. Modifies the option argument to be
 * only the part before the equals sign, and returns a pointer to the part after
 * the equals sign. */
char *parse_config_option(char *option) {
    char *equals = strchr(option, '=');
    if (!equals) {
        fprintf(stderr, "%s: Expected configuration in the form property=value, not \"%s\"\n",
                progname, option);
        exit(1);
    }

    // Overwrite equals sign with null, to split key and value into two strings
    *equals = '\0';
    return equals + 1;
}

void set_kafka_config(producer_context_t context, char *property, char *value) {
    if (rd_kafka_conf_set(context->kafka_conf, property, value,
                context->error, PRODUCER_CONTEXT_ERROR_LEN) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s: %s\n", progname, context->error);
        exit(1);
    }
}

void set_topic_config(producer_context_t context, char *property, char *value) {
    if (rd_kafka_topic_conf_set(context->topic_conf, property, value,
                context->error, PRODUCER_CONTEXT_ERROR_LEN) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s: %s\n", progname, context->error);
        exit(1);
    }
}


static int on_begin_txn(void *_context, uint64_t wal_pos, uint32_t xid) {
    producer_context_t context = (producer_context_t) _context;
    replication_stream_t stream = &context->client->repl;

    if (xid == 0) {
        if (context->xact_head != 0 || context->xact_tail != 0) {
            fprintf(stderr, "%s: Expected snapshot to be the first transaction.\n", progname);
            exit_nicely(context, 1);
        }

        fprintf(stderr, "Created replication slot \"%s\", capturing consistent snapshot \"%s\".\n",
                stream->slot_name, stream->snapshot_name);
        return 0;
    }

    // If the circular buffer is full, we have to block and wait for some transactions
    // to be delivered to Kafka and acknowledged for the broker.
    while (xact_list_full(context)) backpressure(context);

    context->xact_head = (context->xact_head + 1) % MAX_IN_FLIGHT_TRANSACTIONS;
    transaction_info *xact = &context->xact_list[context->xact_head];
    xact->xid = xid;
    xact->recvd_events = 0;
    xact->pending_events = 0;
    xact->commit_lsn = 0;

    return 0;
}

static int on_commit_txn(void *_context, uint64_t wal_pos, uint32_t xid) {
    producer_context_t context = (producer_context_t) _context;
    transaction_info *xact = &context->xact_list[context->xact_head];

    if (xid == 0) {
        fprintf(stderr, "Snapshot complete, streaming changes from %X/%X.\n",
                (uint32) (wal_pos >> 32), (uint32) wal_pos);
    }

    if (xid != xact->xid) {
        fprintf(stderr, "%s: Mismatched begin/commit events (xid %u in flight, "
                "xid %u committed)\n", progname, xact->xid, xid);
        exit_nicely(context, 1);
    }

    xact->commit_lsn = wal_pos;
    maybe_checkpoint(context);
    return 0;
}


static int on_table_schema(void *_context, uint64_t wal_pos, Oid relid,
        const char *key_schema_json, size_t key_schema_len, avro_schema_t key_schema,
        const char *row_schema_json, size_t row_schema_len, avro_schema_t row_schema) {
    producer_context_t context = (producer_context_t) _context;
    const char *topic_name = avro_schema_name(row_schema);

    topic_list_entry_t entry = schema_registry_update(context->registry, relid, topic_name,
            key_schema_json, key_schema_len, row_schema_json, row_schema_len);

    if (!entry) {
        fprintf(stderr, "%s: %s\n", progname, context->registry->error);
        exit_nicely(context, 1);
    }

    if (!entry->topic) {
        entry->topic = rd_kafka_topic_new(context->kafka, topic_name,
                rd_kafka_topic_conf_dup(context->topic_conf));
        if (!entry->topic) {
            fprintf(stderr, "%s: Cannot open Kafka topic %s: %s\n", progname, topic_name,
                    rd_kafka_err2str(rd_kafka_errno2err(errno)));
            exit_nicely(context, 1);
        }
    }
    return 0;
}


static int on_insert_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val) {
    producer_context_t context = (producer_context_t) _context;
    return send_kafka_msg(context, wal_pos, relid, key_bin, key_len, new_bin, new_len);
}

static int on_update_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val) {
    producer_context_t context = (producer_context_t) _context;
    return send_kafka_msg(context, wal_pos, relid, key_bin, key_len, new_bin, new_len);
}

static int on_delete_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val) {
    producer_context_t context = (producer_context_t) _context;
    if (key_bin)
        return send_kafka_msg(context, wal_pos, relid, key_bin, key_len, NULL, 0);
    else
        return 0; // delete on unkeyed table --> can't do anything
}


int send_kafka_msg(producer_context_t context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len,
        const void *val_bin, size_t val_len) {

    transaction_info *xact = &context->xact_list[context->xact_head];
    xact->recvd_events++;
    xact->pending_events++;

    msg_envelope_t envelope = malloc(sizeof(msg_envelope));
    memset(envelope, 0, sizeof(msg_envelope));
    envelope->context = context;
    envelope->wal_pos = wal_pos;
    envelope->relid = relid;
    envelope->xact = xact;

    void *key = NULL, *val = NULL;
    topic_list_entry_t entry = schema_registry_encode_msg(context->registry, relid,
            key_bin, key_len, &key, val_bin, val_len, &val);

    if (!entry) {
        fprintf(stderr, "%s: %s\n", progname, context->registry->error);
        exit_nicely(context, 1);
    }

    bool enqueued = false;
    while (!enqueued) {
        int err = rd_kafka_produce(entry->topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE,
                val, val == NULL ? 0 : val_len + SCHEMA_REGISTRY_MESSAGE_PREFIX_LEN,
                key, key == NULL ? 0 : key_len + SCHEMA_REGISTRY_MESSAGE_PREFIX_LEN,
                envelope);
        enqueued = (err == 0);

        // If data from Postgres is coming in faster than we can send it on to Kafka, we
        // create backpressure by blocking until the producer's queue has drained a bit.
        if (rd_kafka_errno2err(errno) == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            backpressure(context);

        } else if (err != 0) {
            fprintf(stderr, "%s: Failed to produce to Kafka: %s\n",
                progname, rd_kafka_err2str(rd_kafka_errno2err(errno)));
            exit_nicely(context, 1);
        }
    }

    return 0;
}


/* Called by Kafka producer once per message sent, to report the delivery status
 * (whether success or failure). */
static void on_deliver_msg(rd_kafka_t *kafka, const rd_kafka_message_t *msg, void *opaque) {
    // The pointer that is the last argument to rd_kafka_produce is passed back
    // to us in the _private field in the struct. Seems a bit risky to rely on
    // a field called _private, but it seems to be the only way?
    msg_envelope_t envelope = (msg_envelope_t) msg->_private;

    if (msg->err) {
        fprintf(stderr, "%s: Message delivery failed: %s\n", progname, rd_kafka_message_errstr(msg));
        exit_nicely(envelope->context, 1);
    } else {
        // Message successfully delivered to Kafka
        envelope->xact->pending_events--;
        maybe_checkpoint(envelope->context);
    }
    free(envelope);
}


/* When a Postgres transaction has been durably written to Kafka (i.e. we've seen the
 * commit event from Postgres, so we know the transaction is complete, and the Kafka
 * broker has acknowledged all messages in the transaction), we checkpoint it. This
 * allows the WAL for that transaction to be cleaned up in Postgres. */
void maybe_checkpoint(producer_context_t context) {
    transaction_info *xact = &context->xact_list[context->xact_tail];

    while (xact->pending_events == 0 && (xact->commit_lsn > 0 || xact->xid == 0)) {

        // Set the replication stream's "fsync LSN" (i.e. the WAL position up to which
        // the data has been durably written). This will be sent back to Postgres in the
        // next keepalive message, and used as the restart position if this client dies.
        // This should ensure that no data is lost (although messages may be duplicated).
        replication_stream_t stream = &context->client->repl;

        if (stream->fsync_lsn > xact->commit_lsn) {
            fprintf(stderr, "%s: WARNING: Commits not in WAL order! "
                    "Checkpoint LSN is %X/%X, commit LSN is %X/%X.\n", progname,
                    (uint32) (stream->fsync_lsn >> 32), (uint32) stream->fsync_lsn,
                    (uint32) (xact->commit_lsn  >> 32), (uint32) xact->commit_lsn);
        }

#ifdef DEBUG
        if (stream->fsync_lsn < xact->commit_lsn) {
            fprintf(stderr, "Checkpointing %d events for xid %u, WAL position %X/%X.\n",
                    xact->recvd_events, xact->xid,
                    (uint32) (xact->commit_lsn >> 32), (uint32) xact->commit_lsn);
        }
#endif

        stream->fsync_lsn = xact->commit_lsn;

        // xid==0 is the initial snapshot transaction. Clear the flag when it's complete.
        if (xact->xid == 0 && xact->commit_lsn > 0) {
            context->client->taking_snapshot = false;
        }

        if (context->xact_tail == context->xact_head) break;

        context->xact_tail = (context->xact_tail + 1) % MAX_IN_FLIGHT_TRANSACTIONS;
        xact = &context->xact_list[context->xact_tail];
    }
}


/* If the producing of messages to Kafka can't keep up with the consuming of messages from
 * Postgres, this function applies backpressure. It blocks for a little while, until a
 * timeout or until some network activity occurs in the Kafka client. At the same time, it
 * keeps the Postgres connection alive (without consuming any more data from it). This
 * function can be called in a loop until the buffer has drained. */
void backpressure(producer_context_t context) {
    rd_kafka_poll(context->kafka, 200);

    if (received_sigint) {
        fprintf(stderr, "Received interrupt during backpressure. Shutting down...\n");
        exit_nicely(context, 0);
    }

    // Keep the replication connection alive, even if we're not consuming data from it.
    int err = replication_stream_keepalive(&context->client->repl);
    if (err) {
        fprintf(stderr, "%s: While sending standby status update for keepalive: %s\n",
                progname, context->client->repl.error);
        exit_nicely(context, 1);
    }
}


/* Initializes the client context, which holds everything we need to know about
 * our connection to Postgres. */
client_context_t init_client() {
    frame_reader_t frame_reader = frame_reader_new();
    frame_reader->on_begin_txn    = on_begin_txn;
    frame_reader->on_commit_txn   = on_commit_txn;
    frame_reader->on_table_schema = on_table_schema;
    frame_reader->on_insert_row   = on_insert_row;
    frame_reader->on_update_row   = on_update_row;
    frame_reader->on_delete_row   = on_delete_row;

    client_context_t client = db_client_new();
    client->app_name = APP_NAME;
    client->allow_unkeyed = false;
    client->repl.slot_name = DEFAULT_REPLICATION_SLOT;
    client->repl.output_plugin = OUTPUT_PLUGIN;
    client->repl.frame_reader = frame_reader;
    return client;
}

/* Initializes the producer context, which holds everything we need to know about
 * our connection to Kafka. */
producer_context_t init_producer(client_context_t client) {
    producer_context_t context = malloc(sizeof(producer_context));
    memset(context, 0, sizeof(producer_context));
    client->repl.frame_reader->cb_context = context;

    context->client = client;
    context->registry = schema_registry_new(DEFAULT_SCHEMA_REGISTRY);
    context->brokers = DEFAULT_BROKER_LIST;
    context->kafka_conf = rd_kafka_conf_new();
    context->topic_conf = rd_kafka_topic_conf_new();
    // xact_head, xact_tail and xact_list are set to zero by memset() above

    set_topic_config(context, "produce.offset.report", "true");
    rd_kafka_conf_set_dr_msg_cb(context->kafka_conf, on_deliver_msg);
    return context;
}

/* Connects to Kafka. This should be done before connecting to Postgres, as it
 * simply calls exit(1) on failure. */
void start_producer(producer_context_t context) {
    context->kafka = rd_kafka_new(RD_KAFKA_PRODUCER, context->kafka_conf,
            context->error, PRODUCER_CONTEXT_ERROR_LEN);
    if (!context->kafka) {
        fprintf(stderr, "%s: Could not create Kafka producer: %s\n", progname, context->error);
        exit(1);
    }

    if (rd_kafka_brokers_add(context->kafka, context->brokers) == 0) {
        fprintf(stderr, "%s: No valid Kafka brokers specified\n", progname);
        exit(1);
    }
}

/* Shuts everything down and exits the process. */
void exit_nicely(producer_context_t context, int status) {
    // If a snapshot was in progress and not yet complete, and an error occurred, try to
    // drop the replication slot, so that the snapshot is retried when the user tries again.
    if (context->client->taking_snapshot && status != 0) {
        fprintf(stderr, "Dropping replication slot since the snapshot did not complete successfully.\n");
        if (replication_slot_drop(&context->client->repl) != 0) {
            fprintf(stderr, "%s: %s\n", progname, context->client->repl.error);
        }
    }

    schema_registry_free(context->registry);
    frame_reader_free(context->client->repl.frame_reader);
    db_client_free(context->client);
    if (context->kafka) rd_kafka_destroy(context->kafka);
    curl_global_cleanup();
    rd_kafka_wait_destroyed(2000);
    exit(status);
}

static void handle_sigint(int sig) {
    received_sigint = true;
}


int main(int argc, char **argv) {
    curl_global_init(CURL_GLOBAL_ALL);
    signal(SIGINT, handle_sigint);

    producer_context_t context = init_producer(init_client());
    parse_options(context, argc, argv);
    start_producer(context);
    ensure(context, db_client_start(context->client));

    replication_stream_t stream = &context->client->repl;

    if (!context->client->taking_snapshot) {
        fprintf(stderr, "Replication slot \"%s\" exists, streaming changes from %X/%X.\n",
                stream->slot_name,
                (uint32) (stream->start_lsn >> 32), (uint32) stream->start_lsn);
    }

    while (context->client->status >= 0 && !received_sigint) {
        ensure(context, db_client_poll(context->client));

        if (context->client->status == 0) {
            ensure(context, db_client_wait(context->client));
        }

        rd_kafka_poll(context->kafka, 0);
    }

    if (received_sigint) {
        fprintf(stderr, "Interrupted, shutting down...\n");
    }

    exit_nicely(context, 0);
    return 0;
}
