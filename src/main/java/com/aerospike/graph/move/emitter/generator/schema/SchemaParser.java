package com.aerospike.graph.move.emitter.generator.schema;

import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class SchemaParser {
    public static GraphSchema parse(Path yamlPath)  {
        try{
            final String yamlText = Files.readAllLines(yamlPath, Charset.defaultCharset()).stream()
                    .reduce("", (a, b) -> a + "\n" + b);
            final Yaml yaml = new Yaml(new Constructor(GraphSchema.class, new LoaderOptions()));
            return yaml.load(yamlText);
        }catch (IOException e){
            throw new RuntimeException("Could not read file: " + yamlPath, e);
        }
    }

    public static GraphSchema parse(String yamlText) {
        final Yaml yaml = new Yaml(new Constructor(GraphSchema.class, new LoaderOptions()));
        return yaml.load(yamlText);
    }
}
