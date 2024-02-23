/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core;

import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.CheckedNotThreadSafe;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;

public class Pipeline extends CheckedNotThreadSafe implements AutoCloseable {
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
        final Emitter emitter = (Emitter) RuntimeUtil.lookupOrLoad(Emitter.class,ConfigUtil.withOverrides(config, Map.of(PIPELINE_ID, id)));
        final WorkChunkDriver driver;
        if (Emitter.SelfDriving.class.isAssignableFrom(emitter.getClass()))
            driver = ((Emitter.SelfDriving) emitter).driver(config);
        else
            driver = (WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config);
        final Output output = RuntimeUtil.loadOutput(ConfigUtil.withOverrides(config, Map.of(PIPELINE_ID, id)));
        return new Pipeline(id, driver, emitter, output, phase);
    }

    public long getId() {
        return id;
    }


    public Output getOutput() {
        return output;
    }

    public Emitter getEmitter() {
        return emitter;
    }

    public Iterator<Map<String, Object>> status() {
        return IteratorUtils.of(new HashMap<>() {{
            put("PIPELINE_ID", id);
            put(OUTPUT, output.toString());
            put(EMITTER, emitter.toString());
            put(ENCODER, output.getEncoder().map(Object::toString).orElse("null"));
        }});
    }

    @Override
    public String toString() {
        return IOUtil.formatStruct(status().next());
    }

    @Override
    public void close() throws Exception {
        ((Loadable) emitter).close();
        output.close();
    }
}
