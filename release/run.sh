#!/usr/bin/env bash
export CLASSPATH='target/Movement-1.0.0-SNAPSHOT.jar:'

rm -rf /tmp/generate

java com.aerospike.graph.move.CLI \
  -c conf/generate-csv-sample.properties \
  -o emitter.schemaFile=src/test/resources/new_schema.yaml \
  -o output.directory=/tmp/generate \
  -o emitter.rootVertexIdEnd=20 \
  -o output.entriesPerFile=100
