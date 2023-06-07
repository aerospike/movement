package com.aerospike.graph.generator;

import com.aerospike.graph.generator.emitter.generated.schema.Parser;
import com.aerospike.graph.generator.emitter.generated.schema.def.GraphSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Before;

import java.util.HashMap;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class AbstractGeneratorTest {

    public GraphSchema schema;

    @Before
    public void getTestSchema() {
        final String yamlFileFromResources = "basic_schema.yaml";
        final String yamlText = TestUtil.readFromResources(yamlFileFromResources);
        this.schema = Parser.parse(yamlText);
    }
    public Configuration emptyConfiguration(){
        return new MapConfiguration(new HashMap<>());
    }
    public GraphSchema testGraphSchema() {
        return this.schema;
    }

    public String testGraphSchemaLocationRelativeToModule() {
        return "src/test/resources/basic_schema.yaml";
    }

    public String testGeneratorPropertiesLocationRelativeToProject() {
        return "conf/generator-sample.properties";
    }

    public String testGeneratorTraversalPropertiesLocationRelativeToProject() {
        return "conf/generator-sample-traversal.properties";
    }
    public String newGraphSchemaLocationRelativeToModule() {
        return "src/test/resources/new_schema.yaml";
    }

    public VertexSchema testVertexSchema() {
        return this.schema.vertexTypes.stream()
                .filter(vertexSchema -> vertexSchema.name.equals(TestUtil.Schema.Vertex.ACCOUNT))
                .iterator().next();
    }

}
