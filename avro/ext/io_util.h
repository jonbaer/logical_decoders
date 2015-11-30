#ifndef IO_UTIL_H
#define IO_UTIL_H

#include "avro.h"
#include "postgres.h"

#define check(err, call) { err = call; if (err) return err; }

/* Function that writes something using the Avro writer that is passed to it. Must return 0
 * on success, ENOSPC if the buffer was too small (the operation will be retried), and any
 * other value to indicate any other error (the operation will not be retried). */
typedef int (*try_writing_cb)(avro_writer_t, void *);

int try_writing(bytea **output, try_writing_cb cb, void *context);
int write_schema_json(avro_writer_t writer, void *context);
int write_avro_binary(avro_writer_t writer, void *context);

#endif /* IO_UTIL_H */
