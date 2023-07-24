package com.aerospike.movement.process.tasks.generator;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.generator.Generator;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.OneShotSupplier;
import com.aerospike.movement.util.core.iterator.PrimitiveIteratorWrap;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Generate extends Task {
    static {
        RuntimeUtil.registerTaskAlias(Generate.class.getSimpleName(), Generate.class);
    }

    @Override
    public void init(final Configuration config) {
        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> {
            final long scale = Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.SCALE_FACTOR, config));
            return PrimitiveIteratorWrap.wrap(LongStream.range(0, scale).iterator());
        }));
    }

    @Override
    public void close() throws Exception {

    }

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return new HashMap<>() {{
                put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
                //alias driver range to generator scale factor
                put(SuppliedWorkChunkDriver.Config.Keys.RANGE_TOP, Generator.Config.INSTANCE.getOrDefault(Generator.Config.Keys.SCALE_FACTOR, config));
            }};
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Keys.class);
        }

        public static class Keys {
            public static final String EMITTER = "emitter";
            public static final String OUTPUT_ID_DRIVER = "output.idDriver";
            public static final String WORK_CHUNK_DRIVER = "emitter.workChunkDriver";
            public static final String SCALE_FACTOR = Generator.Config.Keys.SCALE_FACTOR;
        }

    }

    private Generate(final Configuration config) {
        super(Config.INSTANCE, config);
    }


    public static Generate open(final Configuration config) {
        return new Generate(config);
    }


    @Override
    public Map<String, Object> getMetrics() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return false;
    }

    @Override
    public List<Runtime.PHASE> getPhases() {
        return List.of(Runtime.PHASE.ONE);
    }

//    public static Generate
}
