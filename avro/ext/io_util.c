#include "io_util.h"

#define INIT_BUFFER_LENGTH 16384
#define MAX_BUFFER_LENGTH 1048576


/* Allocates a fixed-length buffer and tries to write something to it using the Avro writer API.
 * If it doesn't fit, increases the buffer size and tries again. The actual writing operation
 * is given as a callback; the context argument is passed to the callback. On success (return
 * value 0), output is set to a palloc'ed byte array of the right size. The VARSIZE of the
 * output array does not include a terminating null byte, but we guarantee that the following
 * byte is indeed 0, so it's safe to increment VARSIZE if you need the null byte included. */
int try_writing(bytea **output, try_writing_cb cb, void *context) {
    int size = INIT_BUFFER_LENGTH, err = ENOSPC;
    avro_writer_t writer;

    while (err == ENOSPC && size <= MAX_BUFFER_LENGTH) {
        *output = (bytea *) palloc(size);
        writer = avro_writer_memory(VARDATA(*output), size - VARHDRSZ);
        err = (*cb)(writer, context);

        if (err == 0) {
            SET_VARSIZE(*output, avro_writer_tell(writer) + VARHDRSZ);
            err = avro_write(writer, "\x00", 1);
        }

        if (err == ENOSPC) {
            size *= 4;
            pfree(*output);
        }
        avro_writer_free(writer);
    }

    return err;
}

/* try_writing_cb function that encodes an Avro schema as a JSON string. */
int write_schema_json(avro_writer_t writer, void *context) {
    return avro_schema_to_json((avro_schema_t) context, writer);
}

/* try_writing_cb function that encodes a value using Avro binary encoding. */
int write_avro_binary(avro_writer_t writer, void *context) {
    return avro_value_write(writer, (avro_value_t *) context);
}
