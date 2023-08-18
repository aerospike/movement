#!/usr/bin/env bash
java -cp core/target/core-1.0.0-SNAPSHOT.jar:extensions/tinkerpop/target/tinkerpop-1.0.0-SNAPSHOT.jar:cli/target/cli-1.0.0-SNAPSHOT.jar \
  com.aerospike.movement.cli.CLI \
  task=Generate \
  -d \
  -c conf/generator/example_gdemo.properties \
  -s generator.schema.yaml.path=conf/generator/gdemo_schema.yaml