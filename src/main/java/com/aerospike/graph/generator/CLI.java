package com.aerospike.graph.generator;

import com.aerospike.graph.generator.emitter.generated.StitchMemory;
import com.aerospike.graph.generator.runtime.LocalParallelStreamRuntime;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.sleep;

public class CLI {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Aerospike Graph Data Generator.\n");
        final GeneratorCLI cli;
        try {
            cli = CommandLine.populateCommand(new GeneratorCLI(), args);
        } catch (CommandLine.ParameterException pxe) {
            System.err.printf("Error parsing command line arguments: %s \n", pxe);
            CommandLine.usage(new GeneratorCLI(), System.out);
            return;
        }
        if (cli.printExampleConfig) {
            final Configuration config = ConfigurationBase.getCSVSampleConfiguration(
                    "/path/to/schema.yaml",
                    "/tmp/output");
            System.out.println(ConfigurationBase.configurationToPropertiesFormat(config));
            return;
        }
        if (cli.configPath == null) {
            System.err.println("Error: No configuration file specified.");
            CommandLine.usage(new GeneratorCLI(), System.out);
            return;
        }
        final Configuration config = RuntimeUtil.loadConfiguration(cli.configPath);
        if (cli.overrides != null && cli.overrides.size() > 0) {
            final Map<String, String> overrides = new HashMap<>(cli.overrides);
            overrides.forEach(config::setProperty);
        }
        run(config);
    }

    private static class GeneratorCLI {
        @Option(names = "-c", description = "Path to the configuration file.")
        private String configPath;
        @Option(names = "-p", description = "Print example configuration")
        private boolean printExampleConfig;

        @Option(names = "-o", description = "Override configuration key")
        private Map<String, String> overrides;

    }

    public static void run(Configuration config) {
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);
        final long startTime = System.currentTimeMillis();
        final AtomicLong lastVertexCount = new AtomicLong(0);
        final AtomicLong lastEdgeCount = new AtomicLong(0);
        final ForkJoinTask<Object> backgroundTicker = new ForkJoinPool(1).submit(() -> {
            while (true) {
                final List<Long> outputVertexMetrics = runtime.getOutputVertexMetrics();
                final List<Long> outputEdgeMetrics = runtime.getOutputEdgeMetrics();
                outputTicker(outputVertexMetrics, outputEdgeMetrics, startTime, lastVertexCount, lastEdgeCount);
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            }
        });
        runtime.processVertexStream();
        runtime.processEdgeStream();
        outputTicker(runtime.getOutputVertexMetrics(), runtime.getOutputEdgeMetrics(), startTime, lastVertexCount, lastEdgeCount);
        backgroundTicker.cancel(true);
        runtime.close();
        if (!config.containsKey("runtime.testMode") || !config.getBoolean("runtime.testMode"))
            System.exit(0);
    }


    private static void outputTicker(final List<Long> outputVertexMetrics,
                                     final List<Long> outputEdgeMetrics,
                                     final long startTime,
                                     final AtomicLong lastVertexCount,
                                     AtomicLong lastEdgeCount) {
        final Logger logger = LoggerFactory.getLogger("outputTicker");
        try {
            if (!(outputVertexMetrics.size() > 0 || outputEdgeMetrics.size() > 0)) {
                logger.debug("no active outputs");
                return;
            }
            long totalOutputVertices = 0;
            long totalOutputEdges = 0;
            for (int i = 0; i < outputVertexMetrics.size(); i++) {
                totalOutputVertices += outputVertexMetrics.get(i);
                totalOutputEdges += outputEdgeMetrics.get(i);
            }
            final long priorOutputVerticies = lastVertexCount.getAndSet(totalOutputVertices);
            final long priorOutputEdges = lastEdgeCount.getAndSet(totalOutputEdges);
            final long vertexTransferredSinceLastTick = totalOutputVertices - priorOutputVerticies;
            final long edgeTransferredSinceLastTick = totalOutputEdges - priorOutputEdges;
            final long vertexTransferredPerSecond = vertexTransferredSinceLastTick / 5;
            final long edgeTransferredPerSecond = edgeTransferredSinceLastTick / 5;
            long totalOutputTime = System.currentTimeMillis() - startTime + 1;
            long elementsPerSecond = 0;
            if (totalOutputVertices + totalOutputEdges > 0)
                elementsPerSecond = (totalOutputVertices + totalOutputEdges) / (1 + totalOutputTime / 1000);
            System.out.printf("|%s|" +
                            "%,d vertices and " +
                            "%,d edges written to " +
                            "%s outputs currently at " +
                            "%,d vertex per second and " +
                            "%,d edge per second averaging " +
                            "%,d elements per second since " +
                            "|%s|\n",
                    new Date(System.currentTimeMillis()),
                    totalOutputVertices,
                    totalOutputEdges,
                    outputVertexMetrics.size(),
                    vertexTransferredPerSecond,
                    edgeTransferredPerSecond,
                    elementsPerSecond,
                    new Date(startTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
