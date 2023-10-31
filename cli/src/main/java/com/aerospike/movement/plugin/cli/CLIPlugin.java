/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.plugin.cli;

import com.aerospike.movement.cli.CLI;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.plugin.Plugin;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.stream.Collectors;

public class CLIPlugin extends Plugin {

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
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }


    private final CLI.MovementCLI cli;

    public CLI.MovementCLI getCommandLine() {
        return cli;
    }

    public static Plugin open(final CLI.MovementCLI cli) {
        return new CLIPlugin(cli);
    }

    public Iterator<Object> call() {
        if (cli.listTasks()) {
            return listTasks().stream().map(it -> (Object) it.getName()).collect(Collectors.toList()).iterator();
        }
        if (cli.listComponents()) {
            return listComponents();
        }
        if (cli.taskName().isEmpty()) {
            throw new RuntimeException("No task specified.");
        }
        if (cli.configPath().isEmpty()) {
            throw new RuntimeException("No config file specified.");
        }
        final Configuration config;
        try {
            config = ConfigurationUtil.load(cli.configPath().get());
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config file: " + cli.configPath().get(), e);
        }

        if (cli.overrides().isPresent() && cli.overrides().get().size() > 0) {
            final Map<String, String> overrides = new HashMap<>(cli.overrides().get());
            overrides.forEach(config::setProperty);
        }
        listTasks().forEach(it -> RuntimeUtil.registerTaskAlias(it.getSimpleName(), it));


        return runTask((Task) RuntimeUtil.openClassRef(
                RuntimeUtil.getTaskClassByAlias(cli.taskName().get()).getName(), config), config)
                .orElseThrow(() -> new RuntimeException("Failed to run task: " + cli.taskName().get()));
    }


    protected CLIPlugin(CLI.MovementCLI cli) {
        super(Config.INSTANCE, Config.INSTANCE.defaults());
        this.cli = cli;
    }

    @Override
    public Map<String, List<String>> api() {
        return IteratorUtils.consolidateToMap(Arrays.stream(
                        CLI.MovementCLI.Args.class.getDeclaredFields())
                .map(f -> new AbstractMap.SimpleEntry<>(f.getName(), List.of(String.class.getSimpleName()))));
    }

    @Override
    public void plugInto(final Object system) {

    }

    @Override
    public void init(final Configuration config) {

    }

    @Override
    public void close() throws Exception {

    }
}
