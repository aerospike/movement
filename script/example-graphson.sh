#!/usr/bin/env bash
java -cp core/target/core-1.0.0-SNAPSHOT.jar:extensions/tinkerpop/target/tinkerpop-1.0.0-SNAPSHOT.jar:cli/target/cli-1.0.0-SNAPSHOT.jar \
  com.aerospike.movement.cli.CLI \
  task=Generate \
  -d \
  -c conf/generator/simplest.properties \
  -s traversalSource.host=localhost \
  -s traversalSource.port=8182 \
  -s generator.schema.graphschema.graphson.file=conf/generator/simplest_schema.json \
  -s generator.scaleFactor=3
