/* Conversion of Postgres server-side structures into the wire protocol, which
 * is emitted by the output plugin and consumed by the client. */

#include "protocol_server.h"
#include "io_util.h"
#include "oid2avro.h"

#include <stdarg.h>
#include <string.h>
#include "access/heapam.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"

int extract_tuple_key(schema_cache_entry *entry, Relation rel, TupleDesc tupdesc, HeapTuple tuple, bytea **key_out);
int update_frame_with_table_schema(avro_value_t *frame_val, schema_cache_entry *entry);
int update_frame_with_insert_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *new_bin);
int update_frame_with_update_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *old_bin, bytea *new_bin);
int update_frame_with_delete_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *old_bin);
int schema_cache_lookup(schema_cache_t cache, Relation rel, schema_cache_entry **entry_out);
schema_cache_entry *schema_cache_entry_new(schema_cache_t cache);
void schema_cache_entry_update(schema_cache_entry *entry, Relation rel);
void schema_cache_entry_decrefs(schema_cache_entry *entry);
uint64 fnv_hash(uint64 base, char *str, int len);
uint64 fnv_format(uint64 base, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));
uint64 schema_hash_for_relation(Relation rel);
void tupdesc_debug_info(StringInfo msg, TupleDesc tupdesc);

/* http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-param */
#define FNV_HASH_BASE UINT64CONST(0xcbf29ce484222325)
#define FNV_HASH_PRIME UINT64CONST(0x100000001b3)
#define FNV_HASH_BUFSIZE 256

/* Populates a wire protocol message for a "begin transaction" event. */
int update_frame_with_begin_txn(avro_value_t *frame_val, ReorderBufferTXN *txn) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, xid_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_BEGIN_TXN, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    return err;
}

/* Populates a wire protocol message for a "commit transaction" event. */
int update_frame_with_commit_txn(avro_value_t *frame_val, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, xid_val, lsn_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_COMMIT_TXN, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &lsn_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    check(err, avro_value_set_long(&lsn_val, commit_lsn));
    return err;
}

/* If we're using a primary key/replica identity index for a given table, this
 * function extracts that index' columns from a row tuple, and encodes the values
 * as an Avro string using the table's key schema. */
int extract_tuple_key(schema_cache_entry *entry, Relation rel, TupleDesc tupdesc, HeapTuple tuple, bytea **key_out) {
    int err = 0;
    if (entry->key_schema) {
        check(err, avro_value_reset(&entry->key_value));
        check(err, tuple_to_avro_key(&entry->key_value, tupdesc, tuple, rel, entry->key_index));
        check(err, try_writing(key_out, &write_avro_binary, &entry->key_value));
    }
    return err;
}

/* Updates the given frame value with a tuple inserted into a table. The table
 * schema is automatically included in the frame if it's not in the cache. This
 * function is used both during snapshot and during stream replication.
 *
 * The TupleDesc parameter is not redundant. During stream replication, it is just
 * RelationGetDescr(rel), but during snapshot it is taken from the result set.
 * The difference is that the result set tuple has dropped (logically invisible)
 * columns omitted. */
int update_frame_with_insert(avro_value_t *frame_val, schema_cache_t cache, Relation rel, TupleDesc tupdesc, HeapTuple newtuple) {
    int err = 0;
    schema_cache_entry *entry;
    bytea *key_bin = NULL, *new_bin = NULL;

    int changed = schema_cache_lookup(cache, rel, &entry);
    if (changed) {
        check(err, update_frame_with_table_schema(frame_val, entry));
    }

    check(err, extract_tuple_key(entry, rel, tupdesc, newtuple, &key_bin));
    check(err, avro_value_reset(&entry->row_value));
    check(err, tuple_to_avro_row(&entry->row_value, tupdesc, newtuple));
    check(err, try_writing(&new_bin, &write_avro_binary, &entry->row_value));
    check(err, update_frame_with_insert_raw(frame_val, RelationGetRelid(rel), key_bin, new_bin));

    if (key_bin) pfree(key_bin);
    pfree(new_bin);
    return err;
}

/* Updates the given frame with information about a table row that was modified.
 * This is used only during stream replication. */
int update_frame_with_update(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple oldtuple, HeapTuple newtuple) {
    int err = 0;
    schema_cache_entry *entry;
    bytea *old_bin = NULL, *new_bin = NULL, *old_key_bin = NULL, *new_key_bin = NULL;

    int changed = schema_cache_lookup(cache, rel, &entry);
    if (changed) {
        check(err, update_frame_with_table_schema(frame_val, entry));
    }

    /* oldtuple is non-NULL when replident = FULL, or when replident = DEFAULT and there is no
     * primary key, or replident = DEFAULT and the primary key was not modified by the update. */
    if (oldtuple) {
        check(err, extract_tuple_key(entry, rel, RelationGetDescr(rel), oldtuple, &old_key_bin));
        check(err, avro_value_reset(&entry->row_value));
        check(err, tuple_to_avro_row(&entry->row_value, RelationGetDescr(rel), oldtuple));
        check(err, try_writing(&old_bin, &write_avro_binary, &entry->row_value));
    }

    check(err, extract_tuple_key(entry, rel, RelationGetDescr(rel), newtuple, &new_key_bin));
    check(err, avro_value_reset(&entry->row_value));
    check(err, tuple_to_avro_row(&entry->row_value, RelationGetDescr(rel), newtuple));
    check(err, try_writing(&new_bin, &write_avro_binary, &entry->row_value));

    if (old_key_bin != NULL && (VARSIZE(old_key_bin) != VARSIZE(new_key_bin) ||
            memcmp(VARDATA(old_key_bin), VARDATA(new_key_bin), VARSIZE(new_key_bin) - VARHDRSZ) != 0)) {
        /* If the primary key changed, turn the update into a delete and an insert. */
        check(err, update_frame_with_delete_raw(frame_val, RelationGetRelid(rel), old_key_bin, old_bin));
        check(err, update_frame_with_insert_raw(frame_val, RelationGetRelid(rel), new_key_bin, new_bin));
    } else {
        check(err, update_frame_with_update_raw(frame_val, RelationGetRelid(rel), new_key_bin, old_bin, new_bin));
    }

    if (old_key_bin) pfree(old_key_bin);
    if (new_key_bin) pfree(new_key_bin);
    if (old_bin) pfree(old_bin);
    pfree(new_bin);
    return err;
}

/* Updates the given frame with information about a table row that was deleted.
 * This is used only during stream replication. */
int update_frame_with_delete(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple oldtuple) {
    int err = 0;
    schema_cache_entry *entry;
    bytea *key_bin = NULL, *old_bin = NULL;

    int changed = schema_cache_lookup(cache, rel, &entry);
    if (changed) {
        check(err, update_frame_with_table_schema(frame_val, entry));
    }

    if (oldtuple) {
        check(err, extract_tuple_key(entry, rel, RelationGetDescr(rel), oldtuple, &key_bin));
        check(err, avro_value_reset(&entry->row_value));
        check(err, tuple_to_avro_row(&entry->row_value, RelationGetDescr(rel), oldtuple));
        check(err, try_writing(&old_bin, &write_avro_binary, &entry->row_value));
    }

    check(err, update_frame_with_delete_raw(frame_val, RelationGetRelid(rel), key_bin, old_bin));

    if (key_bin) pfree(key_bin);
    if (old_bin) pfree(old_bin);
    return err;
}

/* Sends Avro schemas for a table to the client. This is called the first time we send
 * row-level events for a table, as well as every time the schema changes. All subsequent
 * inserts/updates/deletes are assumed to be encoded with this schema. */
int update_frame_with_table_schema(avro_value_t *frame_val, schema_cache_entry *entry) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, hash_val, key_schema_val,
                 row_schema_val, branch_val;
    bytea *key_schema_json = NULL, *row_schema_json = NULL;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_TABLE_SCHEMA, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,      NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &hash_val,       NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &key_schema_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 3, &row_schema_val, NULL));
    check(err, avro_value_set_long(&relid_val, entry->relid));
    check(err, avro_value_set_fixed(&hash_val, &entry->hash, 8));

    if (entry->key_schema) {
        check(err, try_writing(&key_schema_json, &write_schema_json, entry->key_schema));
        check(err, avro_value_set_branch(&key_schema_val, 1, &branch_val));
        check(err, avro_value_set_string_len(&branch_val, VARDATA(key_schema_json),
                    VARSIZE(key_schema_json) - VARHDRSZ + 1));
        pfree(key_schema_json);
    } else {
        check(err, avro_value_set_branch(&key_schema_val, 0, NULL));
    }

    check(err, try_writing(&row_schema_json, &write_schema_json, entry->row_schema));
    check(err, avro_value_set_string_len(&row_schema_val, VARDATA(row_schema_json),
                VARSIZE(row_schema_json) - VARHDRSZ + 1));
    pfree(row_schema_json);
    return err;
}

/* Populates a wire protocol message for an insert event. */
int update_frame_with_insert_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *new_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, key_val, newrow_val, branch_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_INSERT, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &key_val,    NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &newrow_val, NULL));
    check(err, avro_value_set_long(&relid_val, relid));
    check(err, avro_value_set_bytes(&newrow_val, VARDATA(new_bin), VARSIZE(new_bin) - VARHDRSZ));

    if (key_bin) {
        check(err, avro_value_set_branch(&key_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(key_bin), VARSIZE(key_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&key_val, 0, NULL));
    }
    return err;
}

/* Populates a wire protocol message for an update event. */
int update_frame_with_update_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin,
        bytea *old_bin, bytea *new_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, key_val, oldrow_val, newrow_val, branch_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_UPDATE, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &key_val,    NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &oldrow_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 3, &newrow_val, NULL));
    check(err, avro_value_set_long(&relid_val, relid));
    check(err, avro_value_set_bytes(&newrow_val, VARDATA(new_bin), VARSIZE(new_bin) - VARHDRSZ));

    if (key_bin) {
        check(err, avro_value_set_branch(&key_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(key_bin), VARSIZE(key_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&key_val, 0, NULL));
    }

    if (old_bin) {
        check(err, avro_value_set_branch(&oldrow_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(old_bin), VARSIZE(old_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&oldrow_val, 0, NULL));
    }
    return err;
}

/* Populates a wire protocol message for a delete event. */
int update_frame_with_delete_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *old_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, key_val, oldrow_val, branch_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_DELETE, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,   NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &key_val,     NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &oldrow_val,  NULL));
    check(err, avro_value_set_long(&relid_val, relid));

    if (key_bin) {
        check(err, avro_value_set_branch(&key_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(key_bin), VARSIZE(key_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&key_val, 0, NULL));
    }

    if (old_bin) {
        check(err, avro_value_set_branch(&oldrow_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(old_bin), VARSIZE(old_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&oldrow_val, 0, NULL));
    }
    return err;
}

/* Creates a new schema cache. All palloc allocations for this cache will be
 * performed in the given memory context. */
schema_cache_t schema_cache_new(MemoryContext context) {
    MemoryContext oldctx = MemoryContextSwitchTo(context);
    schema_cache_t cache = palloc0(sizeof(schema_cache));
    cache->context = context;
    cache->num_entries = 0;
    cache->capacity = 16;
    cache->entries = palloc0(cache->capacity * sizeof(void*));
    MemoryContextSwitchTo(oldctx);
    return cache;
}

/* Obtains the schema cache entry for the given relation, creating or updating it if necessary.
 * If the schema hasn't changed since the last invocation, a cached value is used and 0 is returned.
 * If the schema has changed, 1 is returned. If the schema has not been seen before, 2 is returned. */
int schema_cache_lookup(schema_cache_t cache, Relation rel, schema_cache_entry **entry_out) {
    Oid relid = RelationGetRelid(rel);
    schema_cache_entry *entry;

    for (int i = 0; i < cache->num_entries; i++) {
        uint64 hash;
        entry = cache->entries[i];
        if (entry->relid != relid) continue;

        hash = schema_hash_for_relation(rel);
        if (entry->hash == hash) {
            /* Schema has not changed */
            *entry_out = entry;
            return 0;

        } else {
            /* Schema has changed since we last saw it -- update the cache */
            schema_cache_entry_decrefs(entry);
            schema_cache_entry_update(entry, rel);
            *entry_out = entry;
            return 1;
        }
    }

    /* Schema not previously seen -- create a new cache entry */
    entry = schema_cache_entry_new(cache);
    schema_cache_entry_update(entry, rel);
    *entry_out = entry;
    return 2;
}

/* Adds a new entry to the cache, allocated within the cache's memory context.
 * Returns a pointer to the new entry. */
schema_cache_entry *schema_cache_entry_new(schema_cache_t cache) {
    schema_cache_entry *new_entry;
    MemoryContext oldctx = MemoryContextSwitchTo(cache->context);

    if (cache->num_entries == cache->capacity) {
        cache->capacity *= 4;
        cache->entries = repalloc(cache->entries, cache->capacity * sizeof(void*));
    }

    new_entry = palloc0(sizeof(schema_cache_entry));
    cache->entries[cache->num_entries] = new_entry;
    cache->num_entries++;

    MemoryContextSwitchTo(oldctx);
    return new_entry;
}

/* Populates a schema cache entry with the information from a given table. */
void schema_cache_entry_update(schema_cache_entry *entry, Relation rel) {
    entry->relid = RelationGetRelid(rel);
    entry->hash = schema_hash_for_relation(rel);
    entry->key_schema = schema_for_table_key(rel, &entry->key_index);
    entry->row_schema = schema_for_table_row(rel);
    entry->row_iface = avro_generic_class_from_schema(entry->row_schema);
    avro_generic_value_new(entry->row_iface, &entry->row_value);

    if (entry->key_schema) {
        entry->key_iface = avro_generic_class_from_schema(entry->key_schema);
        avro_generic_value_new(entry->key_iface, &entry->key_value);
    }
}

/* Decrements the reference counts for a schema cache entry. */
void schema_cache_entry_decrefs(schema_cache_entry *entry) {
    avro_value_decref(&entry->row_value);
    avro_value_iface_decref(entry->row_iface);
    avro_schema_decref(entry->row_schema);

    if (entry->key_schema) {
        avro_value_decref(&entry->key_value);
        avro_value_iface_decref(entry->key_iface);
        avro_schema_decref(entry->key_schema);
    }
}

/* Frees all the memory structures associated with a schema cache. */
void schema_cache_free(schema_cache_t cache) {
    MemoryContext oldctx = MemoryContextSwitchTo(cache->context);

    for (int i = 0; i < cache->num_entries; i++) {
        schema_cache_entry *entry = cache->entries[i];
        schema_cache_entry_decrefs(entry);
        pfree(entry);
    }

    pfree(cache->entries);
    pfree(cache);
    MemoryContextSwitchTo(oldctx);
}

/* FNV-1a hash algorithm. Can be called incrementally for chunks of data, by using
 * the return value of one call as 'base' argument to the next call. For the first
 * call, use FNV_HASH_BASE as base. */
uint64 fnv_hash(uint64 base, char *str, int len) {
    uint64 hash = base;
    for (int i = 0; i < len; i++) {
        hash = (hash ^ str[i]) * FNV_HASH_PRIME;
    }
    return hash;
}

/* Evaluates a format string with arguments, and then hashes it. */
uint64 fnv_format(uint64 base, char *fmt, ...) {
    static char str[FNV_HASH_BUFSIZE];
    va_list args;
    int len;

    va_start(args, fmt);
    len = vsnprintf(str, FNV_HASH_BUFSIZE, fmt, args);
    va_end(args);

    if (len >= FNV_HASH_BUFSIZE) {
        elog(WARNING, "fnv_format: FNV_HASH_BUFSIZE is too small (must be at least %d)", len + 1);
        len = FNV_HASH_BUFSIZE - 1;
    }

    return fnv_hash(base, str, len);
}

/* Computes a hash over the definition of a relation. This is used to efficiently detect
 * schema changes: if the hash is unchanged, the schema is (very likely) unchanged, but any
 * change in the table definition will cause a different value to be returned. */
uint64 schema_hash_for_relation(Relation rel) {
    uint64 hash = fnv_format(FNV_HASH_BASE, "oid=%u name=%s ns=%s\n",
            RelationGetRelid(rel),
            RelationGetRelationName(rel),
            get_namespace_name(RelationGetNamespace(rel)));

    TupleDesc tupdesc = RelationGetDescr(rel);
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        if (attr->attisdropped) continue; /* skip dropped columns */

        hash = fnv_format(hash, "attname=%s typid=%u\n", NameStr(attr->attname), attr->atttypid);
    }

    if (RelationGetForm(rel)->relkind == RELKIND_RELATION) {
        Relation index_rel = table_key_index(rel);
        if (index_rel) {
            hash = (hash * FNV_HASH_PRIME) ^ schema_hash_for_relation(index_rel);
            relation_close(index_rel, AccessShareLock);
        }
    }

    return hash;
}

/* Append debug information about table columns to a string buffer. */
void tupdesc_debug_info(StringInfo msg, TupleDesc tupdesc) {
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        appendStringInfo(msg, "\n\t%4d. attrelid = %u, attname = %s, atttypid = %u, attlen = %d, "
                "attnum = %d, attndims = %d, atttypmod = %d, attnotnull = %d, "
                "atthasdef = %d, attisdropped = %d, attcollation = %u",
                i, attr->attrelid, NameStr(attr->attname), attr->atttypid, attr->attlen,
                attr->attnum, attr->attndims, attr->atttypmod, attr->attnotnull,
                attr->atthasdef, attr->attisdropped, attr->attcollation);
    }
}

/* Returns a palloc'ed string with information about a table schema, for debugging. */
char *schema_debug_info(Relation rel, TupleDesc tupdesc) {
    StringInfoData msg;
    initStringInfo(&msg);
    appendStringInfo(&msg, "relation oid=%u name=%s ns=%s relkind=%c",
            RelationGetRelid(rel),
            RelationGetRelationName(rel),
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetForm(rel)->relkind);

    if (!tupdesc) tupdesc = RelationGetDescr(rel);
    tupdesc_debug_info(&msg, tupdesc);

    if (RelationGetForm(rel)->relkind == RELKIND_RELATION) {
        Relation index_rel = table_key_index(rel);
        if (index_rel) {
            appendStringInfo(&msg, "\nreplica identity index: oid=%u name=%s ns=%s",
                    RelationGetRelid(index_rel),
                    RelationGetRelationName(index_rel),
                    get_namespace_name(RelationGetNamespace(index_rel)));
            tupdesc_debug_info(&msg, RelationGetDescr(index_rel));
            relation_close(index_rel, AccessShareLock);
        }
    }

    return msg.data;
}
