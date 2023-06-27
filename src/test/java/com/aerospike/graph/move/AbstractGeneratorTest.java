package com.aerospike.graph.move;

import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
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
        this.schema = SchemaParser.parse(yamlText);
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

    public String sampleConfigurationLocationRelativeToModule() {
        return "conf/generate-csv-sample.properties";
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
