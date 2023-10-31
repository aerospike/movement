/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.configuration;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigurationUtil {
    public static ConfigurationBase getConfigurationMeta(final Class<? extends Loadable> clazz) {
        return (ConfigurationBase) RuntimeUtil.getStaticFieldValue(
                RuntimeUtil.loadInnerClass(clazz.getName(), "Config"),
                "INSTANCE");
    }

    public static List<String> getKeysFromClass(final Class clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .map(f -> RuntimeUtil.getStaticFieldValue(clazz, f.getName()))
                .map(Object::toString).collect(Collectors.toList());
    }

    public static Configuration fromFile(final File file) {
        try {
            Properties catalogProps = new Properties();
            catalogProps.load(new FileInputStream(file));
            return new MapConfiguration(catalogProps);
        } catch (IOException e) {
            throw RuntimeUtil.getErrorHandler(ConfigurationUtil.class).handleError(new RuntimeException(e));
        }
    }

    public static boolean isURL(final String path) {
        try {
            new URL(path);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static Map<String, Object> toMap(final Configuration config) {
        return IteratorUtils.stream(config.getKeys())
                .map(key -> Map.entry(key, config.getProperty(key)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Configuration load(final String configPath) {
        if (configPath.startsWith("resources:")) {
            return ConfigurationUtil.fromFile(IOUtil.copyFromResourcesIntoNewTempFile(configPath.split("resources:/")[1]));
        }
        if (ConfigurationUtil.isURL(configPath)) {
            return ConfigurationUtil.fromURL(configPath);
        } else {
            return ConfigurationUtil.fromFile(new File(configPath));
        }
    }

    public static String configurationToPropertiesFormat(final Configuration config) {
        final StringBuilder sb = new StringBuilder();
        config.getKeys().forEachRemaining(key -> {
            sb.append(key).append("=").append(config.getProperty(key)).append("\n");
        });
        return sb.toString();
    }

    public static String keyFromPathElements(String... elements) {
        return String.join(ConfigurationBase.Keys.DOT, elements);
    }

    public static List<String> getSubKeys(final Configuration config, final String baseKey, final String... path) {
        final Stream<String> x = IteratorUtils.stream(config.getKeys())
                .filter(it -> it.contains(baseKey));
        final List pathelements = Arrays.asList(path);
        if (pathelements.size() == 0) {
            return x.collect(Collectors.toList());
        } else {
            final String keyPath = keyFromPathElements(path);
            return x.filter(it -> it.contains(keyPath)).collect(Collectors.toList());
        }
    }

    public static Iterator<String> getSubKeys(final String baseKey, final String providedKey) {
        return Arrays.asList(providedKey.substring(baseKey.length()).split(ConfigurationBase.REGEX_TO_SPLIT_ON_DOT)).iterator();
    }

    public static String getLastConfigPathElement(final String dotNotationKey) {
        final String[] x = dotNotationKey.split(ConfigurationBase.REGEX_TO_SPLIT_ON_DOT);
        return x.length == 0 ? "" : x[x.length - 1];
    }

    /**
     * base key may be a.b.c
     * provided key may be a.b.c.d
     */
    public static boolean hasSubKeys(final String baseKey, final String providedKey) {
        return !baseKey.equals(providedKey) && providedKey.startsWith(baseKey);
    }

    public static boolean isSubKey(final String key) {
        return key.contains(".");
    }

    public static String subKey(final String key, final String subKey) {
        return key + "." + subKey;
    }

    Map<String, Object> configurationToMap(Configuration config) {
        return IteratorUtils.stream(config.getKeys())
                .map(key -> Map.entry(key, config.getProperty(key)))
                .collect(Collectors.toMap(t -> t.getKey(), x -> x.getValue()));
    }

    public static Configuration configurationWithOverrides(Configuration config, Configuration overrides) {
        final Map<String, Object> configMap = new HashMap<>();
        config.getKeys().forEachRemaining(key -> configMap.put(key, config.getProperty(key)));
        final Map<String, Object> overridesMap = new HashMap<>();
        overrides.getKeys().forEachRemaining(key -> overridesMap.put(key, overrides.getProperty(key)));
        configMap.putAll(overridesMap);
        return new MapConfiguration(configMap);
    }

    public static Configuration configurationWithOverrides(Configuration config, Map<String, Object> overrides) {
        return configurationWithOverrides(config, new MapConfiguration(overrides));
    }

    public static Configuration fromURL(final String url) {
        try {
            return fromURL(new URL(url));
        } catch (IOException e) {
            throw RuntimeUtil.getErrorHandler(ConfigurationUtil.class).handleError(new RuntimeException(e));
        }
    }

    public static Configuration fromURL(final URL url) {
        final Path tempFile;
        try {
            tempFile = Files.createTempFile("movement", "tmp");
            IOUtil.downloadFileFromURL(url, tempFile.toFile());
        } catch (IOException e) {
            throw RuntimeUtil.getErrorHandler(ConfigurationUtil.class).handleError(new RuntimeException(e));
        }
        return fromFile(tempFile.toFile());
    }

    public static Configuration empty() {
        return new MapConfiguration(new HashMap<>());
    }

}
