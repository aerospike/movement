FIREFLY_VERSION='1.0.0-SNAPSHOT'

java -cp target/graph-generator-1.0.0-SNAPSHOT.jar:$(realpath ../firefly/aerospike-graph-gremlin/target/aerospike-graph-gremlin-$FIREFLY_VERSION.jar) com.aerospike.graph.generator.CLI -c conf/graph-writer.properties
