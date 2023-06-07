package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.emitter.Emitter;
import com.aerospike.graph.generator.emitter.generated.GeneratedVertex;
import com.aerospike.graph.generator.emitter.generated.StitchMemory;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class LocalSequentialStreamRuntime implements Runtime {
    private final StitchMemory memory;
    private final Configuration config;
    final Output output;
    final Emitter emitter;

    public LocalSequentialStreamRuntime(final Configuration config, StitchMemory memory, Optional<Output> output, Optional<Emitter> emitter) {
        this.memory = memory;
        this.config = config;
        this.emitter = emitter.orElse(RuntimeUtil.loadEmitter(config));
        this.output = output.orElse(RuntimeUtil.loadOutput(config));
    }

    public void processVertexStream() {

        emitter.vertexStream().forEach(generatedVertex -> {
            Optional<CapturedError> result;
            try {
                IteratorUtils.iterate(RuntimeUtil.walk(generatedVertex.emit(output), output));
            } catch (Exception e) {
                result = Optional.of(new CapturedError(e, new GeneratedVertex.GeneratedVertexId(generatedVertex.id().getId())));
            }
            result = Optional.empty();
        });
    }

    @Override
    public void processEdgeStream() {
        emitter.edgeStream().map(emittedEdge -> {
            Optional<CapturedError> result;
            try {
                IteratorUtils.iterate(RuntimeUtil.walk(emittedEdge.emit(output), output));
            } catch (Exception e) {
                result = Optional.of(new CapturedError(e, new GeneratedVertex.GeneratedVertexId(-1L)));
                return result;
            }
            result = Optional.empty();
            return result;
        }).filter(Optional::isPresent).map(Optional::get);
    }

    public Long getOutputEdgeMetric() {
        return output.getEdgeMetric();
    }

    public Long getOutputVertexMetric() {
        return output.getEdgeMetric();
    }
}
