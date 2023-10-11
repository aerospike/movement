package com.aerospike.movement.performance.plugin.script;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ConfigurationUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ScriptPlugin should call a script that prints a JsonString and returns 0 on success.
 * any other return status will be treated as an Error
 */
public abstract class ScriptPlugin extends Loadable {

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(MockOutput.Config.Keys.class);
        }


        public static class Keys {

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    protected ScriptPlugin(final ConfigurationBase configurationMeta, final Configuration config) {
        super(Config.INSTANCE, config);
    }

    protected static class ScriptResults {
        private final int returnCode;
        private final String stdout;
        private final String stderr;

        private ScriptResults(final int returnCode, final String stdout, final String stderr) {
            this.returnCode = returnCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        public static ScriptResults of(final int returnCode, final String stdout, final String stderr) {
            return new ScriptResults(returnCode, stdout, stderr);
        }
    }

    protected static class ScriptPluginError extends RuntimeException {
        private static String formatError(final int returnCode, final String stdout, final String stderr) {
            return String.format("ScriptPluginError:\n  returnCode: %d\n  stdout %s\n  stderr: %s\n", returnCode, stdout, stderr);
        }

        private ScriptPluginError(final int returnCode, final String stdout, final String stderr) {
            super(formatError(returnCode, stdout, stderr));
        }

        public static ScriptPluginError from(ScriptResults results) {
            return new ScriptPluginError(results.returnCode, results.stdout, results.stderr);
        }
    }


}
