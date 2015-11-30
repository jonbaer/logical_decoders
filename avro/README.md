Bottled Water for PostgreSQL
============================

How do you export water from your country? Well, you first go to your reservoir, pump out all the
water, and fill it into bottles. You then go to the streams of clear mountain water flowing into
the reservoir, and tap into them, filling the fresh water into bottles as it arrives. Then you
ship those bottles all around the world.

How do you export data from your database? Well, you first take a consistent snapshot of your
entire database, and encode it in a language-independent format. You then look at the stream of
transactions writing to your database, and parse the transaction log, encoding the
inserts/updates/deletes into the same language-independent format as they happen. Then you take
that data and ship it to your other systems: build search indexes, update caches, load it into
a data warehouse, calculate analytics, monitor it for fraud, and so on.

* [Blog post explaining the background](http://blog.confluent.io/2015/04/23/bottled-water-real-time-integration-of-postgresql-and-kafka/)
* [Watch a short demo!](http://showterm.io/fde6260d684ee3a6ee692)


How it works
------------

Bottled Water uses the [logical decoding](http://www.postgresql.org/docs/9.4/static/logicaldecoding.html)
feature (introduced in PostgreSQL 9.4) to extract a consistent snapshot and a continuous stream
of change events from a database. The data is extracted at a row level, and encoded using
[Avro](http://avro.apache.org/). A client program connects to your database, extracts this data,
and relays it to [Kafka](http://kafka.apache.org/) (you could also integrate it with other systems
if you wish, but Kafka is pretty awesome).

Key features of Bottled Water are:

* Works with any PostgreSQL database (version 9.4 or later). There are no restrictions on your
  database schema.
* No schema changes are required, no triggers or additional tables. (However, you do need to be
  able to install a PostgreSQL extension on the database server. More on this below.)
* Negligible impact on database performance.
* Transactionally consistent output. That means: writes appear only when they are committed to the
  database (writes by aborted transactions are discarded), writes appear in the same order as they
  were committed (no race conditions).
* Fault-tolerant: does not lose data, even if processes crash, machines die, the network is
  interrupted, etc.


Quickstart
----------

There are several possible ways of installing and trying Bottled Water:

* [Running in Docker](#running-in-docker) is the fastest way of getting started, but currently
  only recommended for development environments.
* [Building from source](#building-from-source) is the most flexible, but also a bit fiddly.
* There are also [Ubuntu packages](https://launchpad.net/~stub/+archive/ubuntu/bottledwater),
  built by Stuart Bishop (Canonical).


Running in Docker
-----------------

The easiest way to try Bottled Water is to use the [Docker](https://www.docker.com/) images we have
prepared. You need at least 2GB of memory to run this demo, so if you're running inside a virtual
machine (such as [Boot2docker](http://boot2docker.io/) on a Mac), please check that it is big
enough.

Once you have [installed Docker](https://docs.docker.com/installation/), you can start up
Postgres, Kafka, Zookeeper (required by Kafka) and the
[Confluent schema registry](http://confluent.io/docs/current/schema-registry/docs/intro.html)
as follows:

    $ docker run -d --name zookeeper --hostname zookeeper confluent/zookeeper
    $ docker run -d --name kafka --hostname kafka --link zookeeper:zookeeper \
        --env KAFKA_LOG_CLEANUP_POLICY=compact confluent/kafka
    $ docker run -d --name schema-registry --hostname schema-registry \
        --link zookeeper:zookeeper --link kafka:kafka \
        --env SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL=none confluent/schema-registry
    $ docker run -d --name postgres --hostname postgres confluent/postgres-bw:0.1

The `postgres-bw` image extends the
[official Postgres docker image](https://registry.hub.docker.com/_/postgres/) and adds
Bottled Water support. However, before Bottled Water can be used, it first needs to be
enabled. To do this, start a `psql` shell for the Postgres database:

    $ docker run -it --rm --link postgres:postgres postgres:9.4 sh -c \
        'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U postgres'

When the prompt appears, enable the `bottledwater` extension, and create a database with
some test data, for example:

    create extension bottledwater;
    create table test (id serial primary key, value text);
    insert into test (value) values('hello world!');

You can keep the psql terminal open, and run the following in a new terminal.

The next step is to start the Bottled Water client, which relays data from Postgres to Kafka.
You start it like this:

    $ docker run -d --name bottledwater --hostname bottledwater --link postgres:postgres \
        --link kafka:kafka --link schema-registry:schema-registry confluent/bottledwater:0.1

You can run `docker logs bottledwater` to see what it's doing. Now Bottled Water has taken
the snapshot, and continues to watch Postgres for any data changes. You can see the data
that has been extracted from Postgres by consuming from Kafka (the topic name `test` must
match up with the name of the table you created earlier):

    $ docker run -it --rm --link zookeeper:zookeeper --link kafka:kafka \
        --link schema-registry:schema-registry confluent/tools \
        kafka-avro-console-consumer --property print.key=true --topic test --from-beginning

This should print out the contents of the `test` table in JSON format (key/value separated
by tab). Now go back to the `psql` terminal, and change some data — insert, update or delete
some rows in the `test` table. You should see the changes swiftly appear in the Kafka
consumer terminal.


Building from source
--------------------

To compile Bottled Water is just a matter of:

    make && make install

For that to work, you need the following dependencies installed:

* [PostgreSQL 9.4](http://www.postgresql.org/) development libraries (PGXS and libpq).
  (Homebrew: `brew install postgresql`;
  Ubuntu: `sudo apt-get install postgresql-server-dev-9.4 libpq-dev`)
* [libsnappy](https://code.google.com/p/snappy/), a dependency of Avro.
  (Homebrew: `brew install snappy`; Ubuntu: `sudo apt-get install libsnappy-dev`)
* [avro-c](http://avro.apache.org/), the C implementation of Avro.
  (Homebrew: `brew install avro-c`; others: build from source)
* [Jansson](http://www.digip.org/jansson/), a JSON parser.
  (Homebrew: `brew install jansson`; Ubuntu: `sudo apt-get install libjansson-dev`)
* [libcurl](http://curl.haxx.se/libcurl/), a HTTP client.
  (Homebrew: `brew install curl`; Ubuntu: `sudo apt-get install libcurl4-openssl-dev`)
* [librdkafka](https://github.com/edenhill/librdkafka) (0.8.4 or later), a Kafka client.
  (Ubuntu universe: `sudo apt-get install librdkafka-dev`; others: build from source)

You can see the Dockerfile for
[building the quickstart images](https://github.com/ept/bottledwater-pg/blob/master/build/Dockerfile.build)
as an example of building Bottled Water and its dependencies on Debian.

If you get errors about *Package libsnappy was not found in the pkg-config search path*,
and you have Snappy installed, you may need to create `/usr/local/lib/pkgconfig/libsnappy.pc`
with contents something like the following (be sure to check which version of _libsnappy_
is installed in your system):

    Name: libsnappy
    Description: Snappy is a compression library
    Version: 1.1.2
    URL: https://google.github.io/snappy/
    Libs: -L/usr/local/lib -lsnappy
    Cflags: -I/usr/local/include


Configuration
-------------

The `make install` command above installs an extension into the Postgres installation on
your machine, which does all the work of encoding change data into Avro. There's then a
separate client program which connects to Postgres, fetches the data, and pushes it to Kafka.

To configure Bottled Water, you need to set the following in `postgresql.conf`: (If you're
using Homebrew, you can probably find it in `/usr/local/var/postgres`. On Linux, it's
probably in `/etc/postgres`.)

    wal_level = logical
    max_wal_senders = 8
    wal_keep_segments = 4
    max_replication_slots = 4

You'll also need to give yourself the replication privileges for the database. You can do
this by adding the following to `pg_hba.conf` (in the same directory, replacing `<user>`
with your login username):

    local   replication     <user>                 trust
    host    replication     <user>  127.0.0.1/32   trust
    host    replication     <user>  ::1/128        trust

Restart Postgres for the changes to take effect. Next, enable the Postgres extension that
`make install` installed previously. Start `psql -h localhost` and run:

    create extension bottledwater;

That should be all the setup on the Postgres side. Next, make sure you're running Kafka
and the [Confluent schema registry](http://confluent.io/docs/current/schema-registry/docs/index.html),
for example by following the [quickstart](http://confluent.io/docs/current/quickstart.html).

Assuming that everything is running on the default ports on localhost, you can start
Bottled Water as follows:

    ./kafka/bottledwater --postgres=postgres://localhost

The first time this runs, it will create a replication slot called `bottedwater`,
take a consistent snapshot of your database, and send it to Kafka. (You can change the
name of the replication slot with a command line flag.) When the snapshot is complete,
it switches to consuming the replication stream.

If the slot already exists, the tool assumes that no snapshot is needed, and simply
resumes the replication stream where it last left off.

When you no longer want to run Bottled Water, you have to drop its replication slot
(otherwise you'll eventually run out of disk space, as the open replication slot
prevents the WAL from getting garbage-collected). You can do this by opening `psql`
again and running:

    select pg_drop_replication_slot('bottledwater');


Consuming data
--------------

Bottled Water creates one Kafka topic per database table, with the same name as the
table. The messages in the topic use the table's primary key (or replica identity
index, if set) as key, and the entire table row as value. With inserts and updates,
the message value is the new contents of the row. With deletes, the message value
is null, which allows Kafka's [log compaction](http://kafka.apache.org/documentation.html#compaction)
to garbage-collect deleted values.

If a table doesn't have a primary key or replica identity index, Bottled Water will
complain and refuse to start. You can override this with the `--allow-unkeyed` option.
Any inserts and updates to tables without primary key or replica identity will be
sent to Kafka as messages without a key. Deletes to such tables are not sent to Kafka.

Messages are written to Kafka in a binary Avro encoding, which is efficient, but not
human-readable. To view the contents of a Kafka topic, you can use the Avro console
consumer:

    ./bin/kafka-avro-console-consumer --topic test --zookeeper localhost:2181 \
        --property print.key=true


Status
------

This is early alpha-quality software. It will probably break. See
[Github issues](https://github.com/confluentinc/bottledwater-pg/issues)
for a list of known issues.

Bug reports and pull requests welcome.

Note that Bottled Water has nothing to do with
[Sparkling Water](https://github.com/h2oai/sparkling-water), a machine learning
engine for Spark.


License
-------

Copyright 2015 Confluent, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
software except in compliance with the License in the enclosed file called `LICENSE`.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
