package com.aerospike.movement.emitter.generator.schema;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.IOUtil;
import org.apache.commons.configuration2.Configuration;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class YAMLParser implements GraphSchemaParser {

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
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }

        public static class Keys {
            public static final String YAML_FILE_URI = "generator.schema.yaml.URI";
            public static final String YAML_FILE_PATH = "generator.schema.yaml.path";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    private final File file;

    private YAMLParser(final File file) {
        this.file = file;
    }

    public static YAMLParser open(final Configuration config) {
        if (config.containsKey(Config.Keys.YAML_FILE_PATH)) {
            return from(Path.of(config.getString(Config.Keys.YAML_FILE_PATH)));
        } else if (config.containsKey(Config.Keys.YAML_FILE_URI)) {
            return from(URI.create(Config.INSTANCE.getOrDefault(Config.Keys.YAML_FILE_URI, config)));
        } else {
            throw new RuntimeException("must set " + Config.Keys.YAML_FILE_PATH + " or " + Config.Keys.YAML_FILE_URI);
        }
    }

    public static YAMLParser from(final URI yamlSchemaFileURI) {
        if (yamlSchemaFileURI.getScheme().equals("http")) {
            try {
                File tmpFile = Files.createTempFile("schema", ".yaml").toFile();
                IOUtil.downloadFileFromURL(yamlSchemaFileURI.toURL(), tmpFile);
                return new YAMLParser(tmpFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Unsupported URI scheme: " + yamlSchemaFileURI.getScheme());
        }
    }

    public static YAMLParser from(final Path yamlSchemaPath) {
        return new YAMLParser(yamlSchemaPath.toFile());
    }

    public GraphSchema parse() {
        try {
            final String yamlText = Files.readAllLines(file.toPath(), Charset.defaultCharset()).stream()
                    .reduce("", (a, b) -> a + "\n" + b);
            final Yaml yaml = new Yaml(new Constructor(GraphSchema.class, new LoaderOptions()));
            return yaml.load(yamlText);
        } catch (IOException e) {
            throw new RuntimeException("Could not read file: " + file.toPath(), e);
        }
    }
}
