package com.aerospike.graph.move.schema;

import com.aerospike.graph.move.TestUtil;
import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ParserTest {

    @Test
    public void testLoadYamlFromResources() {
        final String yamlFileFromResources = "basic_schema.yaml";
        final String yamlText = TestUtil.readFromResources(yamlFileFromResources);
        assertTrue(yamlText.contains("edgeTypes"));
    }

    @Test
    public void parseYamlSimple() {
        final String yamlFileFromResources = "basic_schema.yaml";
        final String yamlText = TestUtil.readFromResources(yamlFileFromResources);
        final GraphSchema schema = SchemaParser.parse(yamlText);
        assertTrue(schema.edgeTypes.size() > 0);
    }

    @Test
    public void parseYaml() {
        final String yamlFileFromResources = "basic_schema.yaml";
        final String yamlText = TestUtil.readFromResources(yamlFileFromResources);
        final GraphSchema schema = SchemaParser.parse(yamlText);
        assertTrue(schema.edgeTypes.size() > 0);
        assertTrue(schema.vertexTypes.size() > 0);
        assertEquals("account", schema.entrypointVertexType);
        assertEquals(10, schema.edgeTypes.stream()
                .filter(edgeSchema -> edgeSchema.name.equals("Holds")).iterator().next()
                .properties.get(0).valueGenerator.args.get("length"));
    }
}
