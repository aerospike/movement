package com.aerospike.graph.move.schema;

import com.aerospike.graph.move.AbstractMovementTest;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.emitter.generator.ValueGenerator;
import org.junit.Test;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ValueGeneratorTest extends AbstractMovementTest {
    @Test
    public void generateValue() {
        final VertexSchema vertexSchema = testVertexSchema();
        final ValueGenerator gen = ValueGenerator.getGenerator(vertexSchema.properties.iterator().next().valueGenerator);

    }

}