package com.aerospike.movement.cli;

import com.aerospike.movement.plugin.cli.CLIPlugin;
import picocli.CommandLine;

import java.util.*;
import java.util.stream.Collectors;


public class CLI {
    public static void main(String[] args) throws InterruptedException {
        try {
            Optional<CLIPlugin> plugin = parseAndLoadPlugin(args);
            final Iterator<?> results = plugin.orElseThrow(() -> new RuntimeException("Could not load CLI Plugin")).call();
            while (results.hasNext()) {
                results.next();
//                System.out.println(results.next());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected static Optional<CLIPlugin> parseAndLoadPlugin(final String[] args) throws Exception {
        final MovementCLI cli;
        try {
            cli = CommandLine.populateCommand(new MovementCLI(), args);
        } catch (CommandLine.ParameterException pxe) {
            CommandLine.usage(new MovementCLI(), System.out);
            throw new RuntimeException("Error parsing command line arguments", pxe);
        }
        if (cli.help) {
            System.out.println("Movement, by Aerospike.\n");
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

    public static class MovementCLI {
        public static class Args {
            public static final String TASK = "task";
            public static final String CONFIG_SHORT = "-c";
            public static final String CONFIG_LONG = "--config";
            public static final String SET_SHORT = "-s";
            public static final String SET_LONG = "--set";
            public static final String HELP_SHORT = "-h";
            public static final String HELP_LONG = "--help";
            public static final String LIST_TASKS = "--list-tasks";
            public static final String LIST_COMPONENTS = "--list-components";


        }

        @CommandLine.Option(names = {Args.HELP_SHORT, Args.HELP_LONG}, description = "Help")
        protected boolean help;

        @CommandLine.Option(names = Args.LIST_TASKS, description = "List available tasks")
        protected boolean listTasks;
        @CommandLine.Option(names = Args.LIST_COMPONENTS, description = "List available components")
        protected boolean listComponents;
        @CommandLine.Option(names = Args.TASK, description = "Task to run")
        protected String taskName;
        @CommandLine.Option(names = {Args.CONFIG_SHORT, Args.CONFIG_LONG}, description = "Path or URL to the configuration file")
        protected String configPath;
        @CommandLine.Option(names = {Args.SET_SHORT, Args.SET_LONG}, description = "Set or override configuration key")
        protected Map<String, String> overrides;


        public boolean help(){
            return Optional.ofNullable(help).orElse(false);
        }
        public boolean listTasks(){
            return Optional.ofNullable(listTasks).orElse(false);
        }
        public boolean listComponents(){
            return Optional.ofNullable(listComponents).orElse(false);
        }
        public Optional<String> taskName(){
            return Optional.ofNullable(taskName);
        }
        public Optional<String> configPath(){
            return Optional.ofNullable(configPath);
        }
        public Optional<Map<String, String>> overrides(){
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
