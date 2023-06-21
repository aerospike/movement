#!/usr/bin/env bash
echo will clear /tmp/generate
rm -rf /tmp/generate
echo running generate in /tmp/generate
java -cp ./lib/graph-generator-1.0.0-SNAPSHOT.jar:\
  com.aerospike.graph.generator.CLI \
  -c conf/write-csv-100m.properties
