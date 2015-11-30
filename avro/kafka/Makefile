SOURCES=bottledwater.c registry.c
EXECUTABLE=bottledwater
STATICLIB=../client/libbottledwater.a

PG_CFLAGS = -I$(shell pg_config --includedir) -I$(shell pg_config --includedir-server)
PG_LDFLAGS = -L$(shell pg_config --libdir) -lpq
KAFKA_CFLAGS =
KAFKA_LDFLAGS = -lrdkafka -lz -lpthread # TODO -lrt needed on Linux for clock_gettime, but not on OSX?
AVRO_CFLAGS = $(shell pkg-config --cflags avro-c)
AVRO_LDFLAGS = $(shell pkg-config --libs avro-c)
CURL_CFLAGS = $(shell curl-config --cflags)
CURL_LDFLAGS = $(shell curl-config --libs)
JSON_CFLAGS = $(shell pkg-config --cflags jansson)
JSON_LDFLAGS = $(shell pkg-config --libs jansson)

WARNINGS=-Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security
# _POSIX_C_SOURCE=200809L enables strdup
CFLAGS=-c -std=c99 -D_POSIX_C_SOURCE=200809L -I../client $(PG_CFLAGS) $(KAFKA_CFLAGS) $(AVRO_CFLAGS) $(CURL_CFLAGS) $(JSON_CFLAGS) $(WARNINGS)
LDFLAGS= $(PG_LDFLAGS) $(KAFKA_LDFLAGS) $(AVRO_LDFLAGS) $(CURL_LDFLAGS) $(JSON_LDFLAGS)
CC=gcc
OBJECTS=$(SOURCES:.c=.o)

.PHONY: all clean

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $^ $(STATICLIB) -o $@ $(LDFLAGS)

.c.o:
	$(CC) $< $(CFLAGS) -o $@

clean:
	rm -f $(OBJECTS) $(EXECUTABLE)
