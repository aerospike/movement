package com.aerospike.movement.runtime.core.local;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.CheckedNotThreadSafe;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

class Pipeline extends CheckedNotThreadSafe implements AutoCloseable {
    public static String PIPELINE_ID = "internal.pipeline.id";
    private final Runtime.PHASE phase;
    private final Emitter emitter;
    private final Output output;
    private final long id;
    private AtomicBoolean started = new AtomicBoolean(false);
    final WorkChunkDriver driver;

    private Pipeline(final long id, final WorkChunkDriver driver, final Emitter emitter, final Output output, final Runtime.PHASE phase) {
        this.driver = driver;
        this.phase = phase;
        this.emitter = emitter;
        this.output = output;
        this.id = id;
    }

    public static Pipeline create(final int id, final Runtime.PHASE phase, final Configuration config) {
        final WorkChunkDriver driver = (WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config);
        final Emitter emitter = RuntimeUtil.loadEmitter(ConfigurationUtil.configurationWithOverrides(config, Map.of(PIPELINE_ID, id)));
        final Output output = RuntimeUtil.loadOutput(ConfigurationUtil.configurationWithOverrides(config, Map.of(PIPELINE_ID, id)));
        return new Pipeline(id, driver, emitter, output, phase);
    }

    public long getId() {
        return id;
    }

    public Stream<Emitable> start() {
        checkThreadAssignment();
        if (started.compareAndSet(false, true)) {
            return getEmitter().stream(driver, phase);
        } else {
            throw RuntimeUtil.getErrorHandler(this).handleError(new IllegalStateException(String.format("%s already started", this)));
        }
    }

    public Output getOutput() {
        return output;
    }

    public Emitter getEmitter() {
        return emitter;
    }

    @Override
    public String toString() {
        return String.format("Pipeline [%d] with Output [%s] and Emitter [%s]", id, output, emitter);
    }

    @Override
    public void close() throws Exception {
        ((Loadable) emitter).close();
        output.close();
    }
}
