/* Definition of the wire protocol between the output plugin (running as an extension
 * in the PostgreSQL server) and the client (which connects to the replication slot).
 * This file is linked into both server and client. */

#include "protocol.h"
#include <assert.h>

avro_schema_t schema_for_begin_txn(void);
avro_schema_t schema_for_commit_txn(void);
avro_schema_t schema_for_table_schema(void);
avro_schema_t schema_for_insert(void);
avro_schema_t schema_for_update(void);
avro_schema_t schema_for_delete(void);
avro_schema_t nullable_schema(avro_schema_t value_schema);

avro_schema_t schema_for_frame() {
    avro_schema_t union_schema, branch_schema, array_schema, record_schema;
    union_schema = avro_schema_union();

    assert(avro_schema_union_size(union_schema) == PROTOCOL_MSG_BEGIN_TXN);
    branch_schema = schema_for_begin_txn();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    assert(avro_schema_union_size(union_schema) == PROTOCOL_MSG_COMMIT_TXN);
    branch_schema = schema_for_commit_txn();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    assert(avro_schema_union_size(union_schema) == PROTOCOL_MSG_TABLE_SCHEMA);
    branch_schema = schema_for_table_schema();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    assert(avro_schema_union_size(union_schema) == PROTOCOL_MSG_INSERT);
    branch_schema = schema_for_insert();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    assert(avro_schema_union_size(union_schema) == PROTOCOL_MSG_UPDATE);
    branch_schema = schema_for_update();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    assert(avro_schema_union_size(union_schema) == PROTOCOL_MSG_DELETE);
    branch_schema = schema_for_delete();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    array_schema = avro_schema_array(union_schema);
    avro_schema_decref(union_schema);

    record_schema = avro_schema_record("Frame", PROTOCOL_SCHEMA_NAMESPACE);
    avro_schema_record_field_append(record_schema, "msg", array_schema);
    avro_schema_decref(array_schema);
    return record_schema;
}

avro_schema_t schema_for_begin_txn() {
    avro_schema_t record_schema = avro_schema_record("BeginTxn", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "xid", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
}

avro_schema_t schema_for_commit_txn() {
    avro_schema_t record_schema = avro_schema_record("CommitTxn", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "xid", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "lsn", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
}

avro_schema_t schema_for_table_schema() {
    avro_schema_t record_schema = avro_schema_record("TableSchema", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "relid", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_fixed("SchemaHash", 8);
    avro_schema_record_field_append(record_schema, "hash", field_schema);
    avro_schema_decref(field_schema);

    field_schema = nullable_schema(avro_schema_string());
    avro_schema_record_field_append(record_schema, "keySchema", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_string();
    avro_schema_record_field_append(record_schema, "rowSchema", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
}

avro_schema_t schema_for_insert() {
    avro_schema_t record_schema = avro_schema_record("Insert", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "relid", field_schema);
    avro_schema_decref(field_schema);

    field_schema = nullable_schema(avro_schema_bytes());
    avro_schema_record_field_append(record_schema, "key", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_bytes();
    avro_schema_record_field_append(record_schema, "newRow", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
}

avro_schema_t schema_for_update() {
    avro_schema_t record_schema = avro_schema_record("Update", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "relid", field_schema);
    avro_schema_decref(field_schema);

    field_schema = nullable_schema(avro_schema_bytes());
    avro_schema_record_field_append(record_schema, "key", field_schema);
    avro_schema_decref(field_schema);

    field_schema = nullable_schema(avro_schema_bytes());
    avro_schema_record_field_append(record_schema, "oldRow", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_bytes();
    avro_schema_record_field_append(record_schema, "newRow", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
}

avro_schema_t schema_for_delete() {
    avro_schema_t record_schema = avro_schema_record("Delete", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "relid", field_schema);
    avro_schema_decref(field_schema);

    field_schema = nullable_schema(avro_schema_bytes());
    avro_schema_record_field_append(record_schema, "key", field_schema);
    avro_schema_decref(field_schema);

    field_schema = nullable_schema(avro_schema_bytes());
    avro_schema_record_field_append(record_schema, "oldRow", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
}

avro_schema_t nullable_schema(avro_schema_t value_schema) {
    avro_schema_t null_schema = avro_schema_null();
    avro_schema_t union_schema = avro_schema_union();
    avro_schema_union_append(union_schema, null_schema);
    avro_schema_union_append(union_schema, value_schema);
    avro_schema_decref(null_schema);
    avro_schema_decref(value_schema);
    return union_schema;
}
