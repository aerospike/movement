$ mvn clean package
$ time java -cp target/graph-generator-1.0.0-SNAPSHOT.jar com.aerospike.graph.generator.CLI -c conf/generator-sample.properties
Aerospike Graph Data Generator.

SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
3,380,732 vertices and 2,958,190 edges written to 12 outputs at 1,267,784 elements per second
7,919,837 vertices and 6,929,872 edges written to 12 outputs at 1,484,970 elements per second
7,999,968 vertices and 6,999,972 edges written to 12 outputs at 1,499,994 elements per second

real    0m10.784s
user    1m37.050s
sys     0m5.265s
