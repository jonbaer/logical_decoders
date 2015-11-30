/* Implements a client for Confluent's Avro schema registry, documented here:
 * http://confluent.io/docs/current/schema-registry/docs/index.html
 * Whenever the Postgres extension notifies us about a new schema, we push that
 * schema to the registry and obtain a schema ID (a 32-bit number).
 *
 * Every message sent to Kafka is Avro-encoded, and prefixed with five bytes:
 *   - The first byte is always 0, and reserved for future use.
 *   - The next four bytes are the schema ID in big-endian byte order.
 *
 * Anyone who wants to consume the messages can look up the schema ID in the
 * schema registry to obtain the schema, and thus decode the message. */

#include "registry.h"

#include <avro.h>
#include <jansson.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <string.h>

#define CONTENT_TYPE "application/vnd.schemaregistry.v1+json"

void *add_schema_prefix(int schema_id, const void *avro_bin, size_t avro_len);
int registry_request(schema_registry_t registry, topic_list_entry_t entry, int is_key,
        const char *schema_json, size_t schema_len);
static size_t registry_response_cb(void *data, size_t size, size_t nmemb, void *writer);
int registry_parse_response(schema_registry_t registry, CURLcode result, char *resp_body,
        int resp_len, int *schema_id_out);
topic_list_entry_t topic_list_lookup(schema_registry_t registry, int64_t relid);
topic_list_entry_t topic_list_replace(schema_registry_t registry, int64_t relid);
topic_list_entry_t topic_list_entry_new(schema_registry_t registry);
void registry_error(schema_registry_t registry, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));

/* Allocates and initializes the schema registry struct. */
schema_registry_t schema_registry_new(char *url) {
    schema_registry_t registry = malloc(sizeof(schema_registry));
    memset(registry, 0, sizeof(schema_registry));

    registry->curl = curl_easy_init();
    registry->curl_headers = curl_slist_append(NULL, "Content-Type: " CONTENT_TYPE);
    registry->curl_headers = curl_slist_append(registry->curl_headers, "Accept: " CONTENT_TYPE);
    registry->num_topics = 0;
    registry->capacity = 16;
    registry->topics = malloc(registry->capacity * sizeof(void*));

    schema_registry_set_url(registry, url);
    return registry;
}


/* Configures the URL for the schema registry. The argument is copied. */
void schema_registry_set_url(schema_registry_t registry, char *url) {
    if (registry->registry_url) {
        free(registry->registry_url);
    }
    registry->registry_url = strdup(url);

    // Strip trailing slash
    size_t len = strlen(url);
    if (registry->registry_url[len] == '/') {
        registry->registry_url[len] = '\0';
    }
}


/* Prefixes Avro-encoded key and row records with IDs of the schema used for encoding. Sets
 * key_out and row_out to malloc'ed arrays that are SCHEMA_REGISTRY_MESSAGE_PREFIX_LEN bytes
 * longer than the key_len and row_len bytes that were passed in, respectively. The caller is
 * responsible for freeing key_out and row_out. Returns the topic list entry on success,
 * or NULL on error. */
topic_list_entry_t schema_registry_encode_msg(schema_registry_t registry, int64_t relid,
        const void *key_bin, size_t key_len, void **key_out,
        const void *row_bin, size_t row_len, void **row_out) {

    topic_list_entry_t entry = topic_list_lookup(registry, relid);
    if (!entry) {
        registry_error(registry, "relid %" PRIu64 " has no registered schema", relid);
        return NULL;
    }

    *key_out = add_schema_prefix(entry->key_schema_id, key_bin, key_len);
    *row_out = add_schema_prefix(entry->row_schema_id, row_bin, row_len);
    return entry;
}


/* Adds a 5-byte schema ID prefix to a byte array. */
void *add_schema_prefix(int schema_id, const void *avro_bin, size_t avro_len) {
    if (!avro_bin) return NULL;

    uint32_t schema_id_big_endian = htonl(schema_id);

    char *msg = malloc(avro_len + SCHEMA_REGISTRY_MESSAGE_PREFIX_LEN);
    msg[0] = '\0';
    memcpy(msg + 1, &schema_id_big_endian, 4);
    memcpy(msg + SCHEMA_REGISTRY_MESSAGE_PREFIX_LEN, avro_bin, avro_len);

    return msg;
}


/* Submits a new or updated schema to the registry. Re-registering a previously
 * registered schema is idempotent -- indeed, this is how we find out the schema
 * ID for an existing schema. Returns the topic list entry on success, or NULL
 * on failure. Consult registry->error for error message on failure. */
topic_list_entry_t schema_registry_update(schema_registry_t registry,
        int64_t relid, const char *topic_name,
        const char *key_schema_json, size_t key_schema_len,
        const char *row_schema_json, size_t row_schema_len) {

    topic_list_entry_t entry = topic_list_replace(registry, relid);
    entry->relid = relid;
    entry->topic_name = strdup(topic_name);

    if (registry_request(registry, entry, 1, key_schema_json, key_schema_len)) return NULL;
    if (registry_request(registry, entry, 0, row_schema_json, row_schema_len)) return NULL;
    return entry;
}


/* Submits a schema to the registry. If is_key == 1, it's a key schema, and if is_key == 0,
 * it's a row schema. Returns 0 on success. */
int registry_request(schema_registry_t registry, topic_list_entry_t entry, int is_key,
        const char *schema_json, size_t schema_len) {
    if (!schema_json || schema_len == 0) return 0; // Nothing to do

    char url[512];
    int url_len = snprintf(url, sizeof(url), "%s/subjects/%s-%s/versions",
                registry->registry_url, entry->topic_name, is_key ? "key" : "value");

    if (url_len >= sizeof(url)) {
        registry_error(registry, "Schema registry URL is too long: %s", url);
        return EINVAL;
    }

    json_t *req_json = json_pack("{s:s}", "schema", schema_json);
    char *req_body = json_dumps(req_json, JSON_COMPACT);
    if (!req_body) {
        registry_error(registry, "Could not encode JSON request for schema registry");
        return EINVAL;
    }

    char resp_body[1024];
    avro_writer_t resp_writer = avro_writer_memory(resp_body, sizeof(resp_body));

    curl_easy_setopt(registry->curl, CURLOPT_URL, url);
    curl_easy_setopt(registry->curl, CURLOPT_POSTFIELDS, req_body);
    curl_easy_setopt(registry->curl, CURLOPT_HTTPHEADER, registry->curl_headers);
    curl_easy_setopt(registry->curl, CURLOPT_WRITEFUNCTION, registry_response_cb);
    curl_easy_setopt(registry->curl, CURLOPT_WRITEDATA, resp_writer);
    curl_easy_setopt(registry->curl, CURLOPT_ERRORBUFFER, registry->curl_error);

    CURLcode result = curl_easy_perform(registry->curl);

    int schema_id = 0;
    int resp_len = avro_writer_tell(resp_writer);
    int err = registry_parse_response(registry, result, resp_body, resp_len, &schema_id);

    if (!err) {
        if (is_key) {
            entry->key_schema_id = schema_id;
            fprintf(stderr, "Registered key schema for topic \"%s\" with ID %d\n",
                    entry->topic_name, schema_id);
        } else {
            entry->row_schema_id = schema_id;
            fprintf(stderr, "Registered value schema for topic \"%s\" with ID %d\n",
                    entry->topic_name, schema_id);
        }
    }

    avro_writer_free(resp_writer);
    free(req_body);
    json_decref(req_json);
    return err;
}


/* Called by cURL when bytes of response are received from the schema registry.
 * Appends them to a buffer, so that we can parse the response when finished. */
static size_t registry_response_cb(void *data, size_t size, size_t nmemb, void *writer) {
    size_t bytes = size * nmemb;
    int err = avro_write((avro_writer_t) writer, data, bytes);
    if (err == ENOSPC) {
        fprintf(stderr, "Response from schema registry is too large\n");
    }
    return (err == 0) ? bytes : 0;
}


/* Handles the response from a schema-publishing request to the schema registry.
 * On failure, sets an error message and returns non-zero. On success, returns zero
 * and assigns the schema ID to *schema_id_out. */
int registry_parse_response(schema_registry_t registry, CURLcode result, char *resp_body,
        int resp_len, int *schema_id_out) {
    if (result != CURLE_OK) {
        registry_error(registry, "Could not send schema to registry: %s", registry->curl_error);
        return EIO;
    }

    long resp_code = 0;
    curl_easy_getinfo(registry->curl, CURLINFO_RESPONSE_CODE, &resp_code);

    json_error_t parse_err;
    json_t *resp_json = json_loadb(resp_body, resp_len, 0, &parse_err);

    if (!resp_json) {
        if (resp_code == 200) {
            registry_error(registry, "Could not parse schema registry response: %s\n\tResponse text: %.*s",
                    parse_err.text, resp_len, resp_body);
        } else {
            registry_error(registry, "Schema registry returned HTTP status %ld", resp_code);
        }
        return EIO;
    }

    if (resp_code != 200) {
        json_t *message = NULL;
        if (json_is_object(resp_json)) {
            message = json_object_get(resp_json, "message");
        }

        if (message && json_is_string(message)) {
            registry_error(registry, "Schema registry returned HTTP status %ld: %s",
                    resp_code, json_string_value(message));
        } else {
            registry_error(registry, "Schema registry returned HTTP status %ld", resp_code);
        }

        json_decref(resp_json);
        return EIO;
    }

    json_t *schema_id = NULL;
    if (json_is_object(resp_json)) {
        schema_id = json_object_get(resp_json, "id");
    }

    if (!schema_id || !json_is_integer(schema_id)) {
        registry_error(registry, "Missing id field in schema registry response: %.*s",
                resp_len, resp_body);
        json_decref(resp_json);
        return EIO;
    }

    *schema_id_out = (int) json_integer_value(schema_id);
    json_decref(resp_json);
    return 0;
}


/* Obtains the topic list entry for the given relid, and returns null if there is
 * no matching entry. */
topic_list_entry_t topic_list_lookup(schema_registry_t registry, int64_t relid) {
    for (int i = 0; i < registry->num_topics; i++) {
        topic_list_entry_t entry = registry->topics[i];
        if (entry->relid == relid) return entry;
    }
    return NULL;
}


/* If there is an existing list entry for the given relid, it is cleared (the memory
 * it references is freed) and then returned. If there is no existing list entry, a
 * new blank entry is returned. */
topic_list_entry_t topic_list_replace(schema_registry_t registry, int64_t relid) {
    topic_list_entry_t entry = topic_list_lookup(registry, relid);
    if (entry) {
        free(entry->topic_name);
        return entry;
    } else {
        return topic_list_entry_new(registry);
    }
}


/* Allocates a new topic list entry. */
topic_list_entry_t topic_list_entry_new(schema_registry_t registry) {
    if (registry->num_topics == registry->capacity) {
        registry->capacity *= 4;
        registry->topics = realloc(registry->topics, registry->capacity * sizeof(void*));
    }

    topic_list_entry_t new_entry = malloc(sizeof(topic_list_entry));
    memset(new_entry, 0, sizeof(topic_list_entry));
    registry->topics[registry->num_topics] = new_entry;
    registry->num_topics++;

    return new_entry;
}


/* Frees all the memory structures associated with a schema registry. */
void schema_registry_free(schema_registry_t registry) {
    for (int i = 0; i < registry->num_topics; i++) {
        topic_list_entry_t entry = registry->topics[i];
        if (entry->topic) rd_kafka_topic_destroy(entry->topic);
        free(entry->topic_name);
        free(entry);
    }

    curl_slist_free_all(registry->curl_headers);
    curl_easy_cleanup(registry->curl);
    free(registry->topics);
    free(registry->registry_url);
    free(registry);
}


/* Updates the registry's statically allocated error buffer with a message. */
void registry_error(schema_registry_t registry, char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(registry->error, SCHEMA_REGISTRY_ERROR_LEN, fmt, args);
    va_end(args);
}
