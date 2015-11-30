#ifndef REGISTRY_H
#define REGISTRY_H

#include <librdkafka/rdkafka.h>
#include <curl/curl.h>

/* 5 bytes prefix is added by schema_registry_encode_msg(). */
#define SCHEMA_REGISTRY_MESSAGE_PREFIX_LEN 5

#define SCHEMA_REGISTRY_ERROR_LEN 512

typedef struct {
    uint64_t relid;             /* Uniquely identifies a table, even when it is renamed */
    char *topic_name;           /* Derived from schema record name, in turn derived from table name */
    int key_schema_id;          /* Identifier for the current key schema, assigned by the registry */
    int row_schema_id;          /* Identifier for the current row schema, assigned by the registry */
    rd_kafka_topic_t *topic;    /* Kafka topic to which messages are produced */
} topic_list_entry;

typedef topic_list_entry *topic_list_entry_t;

typedef struct {
    CURL *curl;                            /* HTTP client for making requests to schema registry */
    struct curl_slist *curl_headers;       /* HTTP headers for requests to schema registry */
    char curl_error[CURL_ERROR_SIZE];      /* Buffer for libcurl error messages */
    char error[SCHEMA_REGISTRY_ERROR_LEN]; /* Buffer for general error messages */
    char *registry_url;                    /* URL of server (set with schema_registry_set_url()) */
    int num_topics;                        /* Number of topics in use */
    int capacity;                          /* Allocated size of topics array */
    topic_list_entry **topics;             /* Array of pointers to schema_list_entry structs */
} schema_registry;

typedef schema_registry *schema_registry_t;

schema_registry_t schema_registry_new(char *url);
void schema_registry_set_url(schema_registry_t registry, char *url);
topic_list_entry_t schema_registry_encode_msg(schema_registry_t registry, int64_t relid,
        const void *key_bin, size_t key_len, void **key_out,
        const void *row_bin, size_t row_len, void **row_out);
topic_list_entry_t schema_registry_update(schema_registry_t registry,
        int64_t relid, const char *topic_name,
        const char *key_schema_json, size_t key_schema_len,
        const char *row_schema_json, size_t row_schema_len);
void schema_registry_free(schema_registry_t reader);

#endif /* REGISTRY_H */
