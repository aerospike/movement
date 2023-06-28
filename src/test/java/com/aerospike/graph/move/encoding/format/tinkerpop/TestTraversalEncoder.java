package com.aerospike.graph.move.encoding.format.tinkerpop;

import com.aerospike.graph.move.AbstractMovementTest;
import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.generator.GeneratedVertex;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.emitter.generator.VertexContext;
import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.OutEdgeSpec;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.output.tinkerpop.GraphOutput;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import junit.framework.TestCase;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

import static com.aerospike.graph.move.config.ConfigurationBase.configurationToPropertiesFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestTraversalEncoder extends AbstractMovementTest {

    @Test
    @Ignore
    public void testWriteVertex() {
        throw ErrorUtil.unimplemented();
    }

    @Test
    @Ignore
    public void testIterateRootVertex() {
        throw ErrorUtil.unimplemented();
    }

}
