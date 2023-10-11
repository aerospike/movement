package com.aerospike.movement.emitter.generator;

import com.aerospike.movement.emitter.generator.schema.TinkerPopSchemaParser;
import com.aerospike.movement.emitter.generator.schema.YAMLParser;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.util.core.IOUtil;
import com.aerospike.movement.util.tinkerpop.SchemaGraphUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/*
  Created by Grant Haywood grant@iowntheinter.net
  7/18/23
*/
public class SchemaGraphTest extends AbstractMovementTest {
    @Test
    public void testYamlYamlEquality() {
        final File schemaFile = IOUtil.copyFromResourcesIntoNewTempFile("example_schema.yaml");
        SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
        final Configuration config = new MapConfiguration(new HashMap<>() {{
            put(YAMLParser.Config.Keys.YAML_FILE_PATH, schemaFile.getAbsolutePath());
            put(TinkerPopSchemaParser.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
        }});

        //parse the yaml file to a GraphSchema
        GraphSchema fromYaml = YAMLParser.open(config).parse();

        //write that loaded schema into a graph instance
        SchemaGraphUtil.writeToGraph(SharedEmptyTinkerGraphGraphProvider.getInstance(), fromYaml);

        //parse the graph instance back into a GraphSchema
        GraphSchema fromYaml2 = YAMLParser.open(config).parse();

        //compare the two GraphSchema instances, deep equality is implemented by each schema def class
        assertTrue(fromYaml2.equals(fromYaml));
    }

    @Test
    public void testGraphYamlEquality() {
        final File schemaFile = IOUtil.copyFromResourcesIntoNewTempFile("example_schema.yaml");
        SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
        final Configuration config = new MapConfiguration(new HashMap<>() {{
            put(YAMLParser.Config.Keys.YAML_FILE_PATH, schemaFile.getAbsolutePath());
            put(TinkerPopSchemaParser.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
        }});

        //parse the yaml file to a GraphSchema
        GraphSchema fromYaml = YAMLParser.open(config).parse();

        //write that loaded schema into a graph instance
        SchemaGraphUtil.writeToGraph(SharedEmptyTinkerGraphGraphProvider.getInstance(), fromYaml);

        //parse the graph instance back into a GraphSchema
        GraphSchema fromGraph = TinkerPopSchemaParser.open(config).parse();

        //compare the two GraphSchema instances, deep equality is implemented by each schema def class
        assertTrue(fromGraph.equals(fromYaml));
    }

    @Test
    public void testGraphSONSeralization() {
        final File schemaFile = IOUtil.copyFromResourcesIntoNewTempFile("example_schema.yaml");
        SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
        final Configuration config = new MapConfiguration(new HashMap<>() {{
            put(YAMLParser.Config.Keys.YAML_FILE_PATH, schemaFile.getAbsolutePath());
            put(TinkerPopSchemaParser.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
        }});

        //parse the yaml file to a GraphSchema
        GraphSchema fromYaml = YAMLParser.open(config).parse();

        //write that loaded schema into a graph instance
        SchemaGraphUtil.writeToGraph(SharedEmptyTinkerGraphGraphProvider.getInstance(), fromYaml);
        final Graph graph = SharedEmptyTinkerGraphGraphProvider.getInstance();
        graph.traversal().io("target/example_schema.json").write().iterate();
        graph.traversal().V().drop().iterate();
        graph.traversal().io("target/example_schema.json").read().iterate();
        final Configuration GraphSONConfig = new MapConfiguration(new HashMap<>() {{
            put(TinkerPopSchemaParser.Config.Keys.GRAPHSON_FILE, "target/example_schema.json");
        }});


        //parse the graph instance back into a GraphSchema
        GraphSchema fromGraph = TinkerPopSchemaParser.open(GraphSONConfig).parse();

        //compare the two GraphSchema instances, deep equality is implemented by each schema def class
        assertTrue(fromGraph.equals(fromYaml));
    }

}