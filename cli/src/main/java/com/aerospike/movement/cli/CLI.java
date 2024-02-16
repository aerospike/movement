/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.cli;

import com.aerospike.movement.plugin.cli.CLIPlugin;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import picocli.CommandLine;

import java.util.*;
import java.util.stream.Collectors;


public class CLI {
    public static void main(String[] args) throws Exception {
        final Optional<CLIPlugin> plugin = parseAndLoadPlugin(args);
        if (plugin.isEmpty()) return;
        final Iterator<?> response = plugin.get().call();
        if (plugin.get().getCommandLine().listComponents || plugin.get().getCommandLine().listTasks || plugin.get().getCommandLine().help) {
            response.forEachRemaining(it -> System.out.println(it));
        } else {
            Object x = response.next();
            if (UUID.class.isAssignableFrom(x.getClass())) {
                UUID taskId = (UUID) x;
                final Task.StatusMonitor taskMonitor = Task.StatusMonitor.from(taskId);
                while (taskMonitor.isRunning()) {
                    System.out.println(taskMonitor.statusMessage());
                    Thread.sleep(1000L);
                }
                RuntimeUtil.waitTask((UUID) taskId);
            }
        }
        if (!plugin.get().getCommandLine().testMode)
            System.exit(0);
    }

    protected static Optional<CLIPlugin> parseAndLoadPlugin(final String[] args) throws Exception {
        final MovementCLI cli;
        try {
            cli = CommandLine.populateCommand(new MovementCLI(), args);
        } catch (CommandLine.ParameterException pxe) {
            CommandLine.usage(new MovementCLI(), System.out);
            System.err.println("Error parsing command line arguments: " + pxe.getMessage());
            return Optional.empty();
        }
        if (cli.help) {
            CommandLine.usage(new MovementCLI(), System.out);
            return Optional.empty();
        }

        return Optional.of((CLIPlugin) CLIPlugin.open(cli));
    }

    private static List<String> apiToHelp(final Map<String, List<String>> api) {
        return api
                .entrySet()
                .stream()
                .map(entry -> String.format("%s: %s", entry.getKey(),
                        entry.getValue().stream().collect(Collectors.joining(" "))))
                .collect(Collectors.toList());
    }

    public static String setEquals(final String key, final String value) {
        return String.format("%s=%s", key, value);
    }


    @CommandLine.Command(name = "Movement", header = "Movement: a parallel dataflow system, by Aerospike.")
    public static class MovementCLI {
        public static class Args {
            public static final String TASK = "task";
            public static final String CONFIG_SHORT = "-c";
            public static final String CONFIG_LONG = "--config";
            public static final String SET_SHORT = "-s";
            public static final String SET_LONG = "--set";
            public static final String HELP_SHORT = "-h";
            public static final String HELP_LONG = "--help";
            public static final String DEBUG_SHORT = "-d";
            public static final String DEBUG_LONG = "--debug";
            public static final String LIST_TASKS = "--list-tasks";
            public static final String LIST_COMPONENTS = "--list-components";
            public static final String TEST_MODE = "--test-mode";


        }

        @CommandLine.Option(names = {Args.HELP_SHORT, Args.HELP_LONG}, description = "Help")
        protected boolean help;

        @CommandLine.Option(names = Args.LIST_TASKS, description = "List available tasks")
        protected boolean listTasks;
        @CommandLine.Option(names = Args.LIST_COMPONENTS, description = "List available components")
        protected boolean listComponents;
        @CommandLine.Option(names = Args.TASK, description = "Task to run", required = true)
        protected String taskName;
        @CommandLine.Option(names = {Args.CONFIG_SHORT, Args.CONFIG_LONG}, description = "Path or URL to the configuration file")
        protected String configPath;
        @CommandLine.Option(names = {Args.SET_SHORT, Args.SET_LONG}, description = "Set or override configuration key")
        protected Map<String, String> overrides;
        @CommandLine.Option(names = {Args.TEST_MODE}, description = "Test Mode")
        protected Boolean testMode = false;

        @CommandLine.Option(names = {Args.DEBUG_SHORT, Args.DEBUG_LONG}, description = "Debug")
        protected Boolean debug = false;

        public boolean help() {
            return Optional.ofNullable(help).orElse(false);
        }

        public boolean listTasks() {
            return Optional.ofNullable(listTasks).orElse(false);
        }

        public boolean listComponents() {
            return Optional.ofNullable(listComponents).orElse(false);
        }

        public Optional<String> taskName() {
            return Optional.ofNullable(taskName);
        }

        public Optional<String> configPath() {
            return Optional.ofNullable(configPath);
        }

        public Optional<Map<String, String>> overrides() {
            return Optional.ofNullable(overrides);
        }

    }


    private static void outputTicker(final Iterator<Map<String, Object>> pluginIterator) {
        try {
            if (pluginIterator.hasNext()) {
                final Map<String, Object> next = pluginIterator.next();
                System.out.println(next);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
