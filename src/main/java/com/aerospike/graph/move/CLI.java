package com.aerospike.graph.move;

import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.process.BatchJob;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.runtime.local.RunningPhase;
import com.aerospike.graph.move.util.IOUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.graph.move.output.file.DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY;


public class CLI {
    public static final String TEST_MODE = "CLI.testMode";
    private static final long ONE_GB_DATA = 2000000L;

    public static void main(String[] args) {
        System.out.println("Movement, by Aerospike.\n");
        final GeneratorCLI cli;
        try {
            cli = CommandLine.populateCommand(new GeneratorCLI(), args);
        } catch (CommandLine.ParameterException pxe) {
            System.err.printf("Error parsing command line arguments: %s \n", pxe);
            CommandLine.usage(new GeneratorCLI(), System.out);
            return;
        }
        if (cli.runBatchJob) {
            runBatch(RuntimeUtil.loadConfiguration(cli.configPath));
            return;
        }
        if (cli.batchConfigPaths != null && cli.batchConfigPaths.size() > 0) {
            final List<Configuration> configs = cli.batchConfigPaths.stream()
                    .map(RuntimeUtil::loadConfiguration)
                    .collect(Collectors.toList());
            final BatchJob batchJob = BatchJob.of(configs.toArray(new Configuration[0]));
            batchJob.run();
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
        @Option(names = "-x", description = "custom batch job")
        private boolean runBatchJob;
        @Option(names = "-b", description = "Batch mode, provide several configurations")
        private List<String> batchConfigPaths;
        @Option(names = "-c", description = "Path to the configuration file")
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

        System.out.println("Phase one start");
        final List<Output> initalPhaseOutputs = new LinkedList<>();
        final ForkJoinTask<Object> initialPhaseTicker = IOUtil.backgroundTicker(.3, () -> {
            outputTicker(initalPhaseOutputs, startTime, lastVertexCount, lastEdgeCount);
        });
        RunningPhase phaseOne = runtime.phaseOne();
        initalPhaseOutputs.addAll(phaseOne.getOutputs());
        phaseOne.get();
        phaseOne.close();
        initialPhaseTicker.cancel(true);
        outputTicker(initalPhaseOutputs, startTime, lastVertexCount, lastEdgeCount);
        System.out.println("Phase one complete");
        lastVertexCount.set(0);
        lastEdgeCount.set(0);


        System.out.println("Phase two start");
        final List<Output> phaseTwoOutputs = new LinkedList<>();
        final ForkJoinTask<Object> phaseTwoTicker = IOUtil.backgroundTicker(.3, () -> {
            outputTicker(phaseTwoOutputs, startTime, lastVertexCount, lastEdgeCount);
        });
        RunningPhase phaseTwo = runtime.phaseTwo();
        phaseTwoOutputs.addAll(phaseTwo.getOutputs());
        phaseTwo.get();
        phaseTwo.close();
        phaseTwoTicker.cancel(true);
        outputTicker(phaseTwoOutputs, startTime, lastVertexCount, lastEdgeCount);
        System.out.println("Phase two complete");

        runtime.close();

        if (!config.containsKey(TEST_MODE) || !config.getBoolean(TEST_MODE))
            System.exit(0);
    }

    public static void runBatch(Configuration config) {
        final String ID = "id";
        final Path baseDir = Path.of(config.getString(OUTPUT_DIRECTORY));
        config.setProperty(TEST_MODE, true);
        BatchJob.of(config).withOverrides(IntStream.of(64, 128, 256, 512).mapToObj(jobSize ->
                new HashMap<String, Object>() {{
                    put(ID, jobSize);
                    put(Generator.Config.Keys.ROOT_VERTEX_ID_END, jobSize * ONE_GB_DATA);
                    put(OUTPUT_DIRECTORY, String.valueOf(baseDir.resolve(String.valueOf(jobSize))));
                }}).collect(Collectors.toMap(it -> String.valueOf(it.get(ID)), it -> it))
        ).run();
    }

    static List<Long> getOutputVertexMetrics(final List<Output> outputs) {
        return outputs.stream().map(Output::getVertexMetric).filter(Objects::nonNull).collect(Collectors.toList());
    }

    static List<Long> getOutputEdgeMetrics(final List<Output> outputs) {
        return outputs.stream().map(Output::getEdgeMetric).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private static void outputTicker(final List<Output> outputs,
                                     final long startTime,
                                     final AtomicLong lastVertexCount,
                                     final AtomicLong lastEdgeCount) {
        List<Long> edgeMetrics = getOutputEdgeMetrics(outputs);
        List<Long> vertexMetrics = getOutputVertexMetrics(outputs);
        final Logger logger = LoggerFactory.getLogger("outputTicker");
        try {
            if (!(vertexMetrics.size() > 0 || edgeMetrics.size() > 0)) {
                logger.debug("no active outputs");
                return;
            }
            long totalOutputVertices = 0;
            long totalOutputEdges = 0;
            for (int i = 0; i < vertexMetrics.size(); i++) {
                totalOutputVertices += vertexMetrics.get(i);
                totalOutputEdges += edgeMetrics.get(i);
            }
            final long priorOutputVertices = lastVertexCount.getAndSet(totalOutputVertices);
            final long priorOutputEdges = lastEdgeCount.getAndSet(totalOutputEdges);
            final long vertexTransferredSinceLastTick = totalOutputVertices - priorOutputVertices;
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
                    getOutputVertexMetrics(outputs).size(),
                    vertexTransferredPerSecond,
                    edgeTransferredPerSecond,
                    elementsPerSecond,
                    new Date(startTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
