package com.aerospike.graph.generator.schema;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import com.aerospike.graph.generator.process.ValueGenerator;
import org.junit.Test;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ValueGeneratorTest extends AbstractGeneratorTest {
    @Test
    public void generateValue() {
        final VertexSchema vertexSchema = testVertexSchema();
        final ValueGenerator gen = ValueGenerator.getGenerator(vertexSchema.properties.iterator().next().valueGenerator);

    }

}
