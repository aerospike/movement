package com.aerospike.graph.move.schema;

import com.aerospike.graph.move.AbstractGeneratorTest;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.process.ValueGenerator;
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
