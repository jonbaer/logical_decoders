#!/bin/bash
# This script customizes the configuration of Postgres in a Docker image.
# It is called by the entrypoint script:
# https://github.com/docker-library/postgres/blob/master/9.4/docker-entrypoint.sh

sed -i.old \
    -e 's/#* *wal_level *= *[a-z]*/wal_level = logical/' \
    -e 's/#* *max_wal_senders *= *[0-9]*/max_wal_senders = 8/' \
    -e 's/#* *wal_keep_segments *= *[0-9]*/wal_keep_segments = 4/' \
    -e 's/#* *max_replication_slots *= *[0-9]*/max_replication_slots = 4/' \
    "${PGDATA}/postgresql.conf"

# TODO authenticate the user
echo "host replication all 0.0.0.0/0 trust" >> "${PGDATA}/pg_hba.conf"
