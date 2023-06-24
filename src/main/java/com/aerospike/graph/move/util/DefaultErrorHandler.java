package com.aerospike.graph.move.util;

import org.apache.commons.configuration2.Configuration;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DefaultErrorHandler {


    public static Config CONFIG = new Config();
    private final Configuration config;

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String LOG_OUTPUT_DIR = "errors.output.dir";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.LOG_OUTPUT_DIR, "/tmp/");
        }};
    }

    private final FileOutputStream fileWriteStream;

    public DefaultErrorHandler(Configuration config, String logName) {
        this.config = config;
        final String logFileName = String.format(CONFIG.getOrDefault(config, Config.Keys.LOG_OUTPUT_DIR)+"/error_stream_%s.log", logName);
        try {
            this.fileWriteStream = new FileOutputStream(Path.of(logFileName).toFile());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    public void handle(final Throwable t) {
        System.err.println("Error: " + t.getMessage());
        t.printStackTrace();
        writeLine(t.getMessage());
        writeLine(getStackTrace(t));
    }

    private void writeLine(final String line) {
        try {
            fileWriteStream.write((line + "\n").getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
