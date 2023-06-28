package com.aerospike.graph.move.runtime.distributed;

import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.process.Job;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.runtime.local.RunningPhase;
import com.aerospike.graph.move.util.MovementIteratorUtils;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.graph.move.runtime.distributed.DistributedStreamRuntime.Config.Keys.INTERFACES;

/**
 * send out chunkId: [arrays of ids] to each node
 * keep a map of chunkId and understand when it has completed
 * write this to a log
 * if all outstanding chunks have been completed, it is safe to checkpoint halt
 * if you need to resume, you can use the log to determine which ids have already been loaded
 */
public class DistributedStreamRuntime implements Runtime {
    final ClusterManager clusterManager;

    public static DistributedStreamRuntime.Config CONFIG = new DistributedStreamRuntime.Config();
    private final Vertx vertx;
    private final Configuration config;
    private final Iterator<List<Object>> vertexIterator;
    private final Iterator<List<Object>> edgeIterator;

    private final LocalParallelStreamRuntime subRuntime;
    private final Optional<List<String>> nodeList;
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private AtomicBoolean coordinator = new AtomicBoolean(false);

    public void close() {
        vertx.close();
    }

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String VERTEX = "vertex";
            public static final String EDGE = "edge";

            public static final String ID_LOG_FILE = "ids.log";
            private static final String MEMBER_COUNT = "memberCount";
            private static final String START_CHANNEL = "startChannel";
            private static final String STOP_CHANNEL = "stopChannel";
            private static final String COORDINATOR = "coordinator";
            public static final String ID_CHANNEL = "idChannel";


            public static final String THREADS = "runtime.threads";
            public static final String DROP_OUTPUT = "runtime.dropOutput";
            public static final String OUTPUT_STARTUP_DELAY_MS = "runtime.outputStallTimeMs";
            public static final String MEMBERS_LIST = "runtime.distributed.members";
            public static final String INTERFACES = "runtime.distributed.interfaces";

        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(INTERFACES, "127.0.0.1");
        }};
    }

    private DistributedStreamRuntime(Configuration config, ClusterManager clusterManager, Vertx vertex) {
        this.clusterManager = clusterManager;
        this.vertx = vertex;
        this.config = config;
        this.subRuntime = (LocalParallelStreamRuntime) LocalParallelStreamRuntime.open(config);
        this.nodeList = config.containsKey(Config.Keys.MEMBERS_LIST) ?
                Optional.of(Arrays.stream(CONFIG.getOrDefault(config, Config.Keys.MEMBERS_LIST)
                                .split(","))
                        .collect(Collectors.toList())) :
                Optional.empty();
        this.vertexIterator = new MovementIteratorUtils.IteratorWithFeeder(this, vertx, Config.Keys.VERTEX);
        this.edgeIterator = new MovementIteratorUtils.IteratorWithFeeder(this, vertx, Config.Keys.EDGE);

    }

    public static com.hazelcast.config.Config toHazelcastConfig(Configuration config) {
        final TcpIpConfig tcpIpConfig = new TcpIpConfig();
        if (config.containsKey(Config.Keys.MEMBERS_LIST))
            tcpIpConfig.setMembers(Arrays.stream(CONFIG.getOrDefault(config, Config.Keys.MEMBERS_LIST).split(",")).collect(Collectors.toList()));
        final JoinConfig join = new JoinConfig().setTcpIpConfig(tcpIpConfig);
        final NetworkConfig net = new NetworkConfig()
                .setPort(5701)
                .setInterfaces(new InterfacesConfig()
                        .setInterfaces(Arrays.stream(CONFIG.getOrDefault(config, INTERFACES)
                                .split(",")).collect(Collectors.toList())))
                .setPortAutoIncrement(true)
                .setJoin(join);
        return new com.hazelcast.config.Config()
                .setNetworkConfig(net);
    }

    public static DistributedStreamRuntime open(Configuration config) {
        ClusterManager mgr = new HazelcastClusterManager(toHazelcastConfig(config));

        VertxOptions options = new VertxOptions().setClusterManager(mgr);

        try {
            return new DistributedStreamRuntime(config, mgr, convertToCompletableFuture(Vertx.clusteredVertx(options)).get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> CompletableFuture<T> convertToCompletableFuture(Future<T> vertxFuture) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();

        vertxFuture.onComplete(result -> {
            if (result.succeeded()) {
                completableFuture.complete(result.result());
            } else {
                completableFuture.completeExceptionally(result.cause());
            }
        });

        return completableFuture;
    }

    public CompletableFuture<?> start() {
        Promise promise = Promise.promise();
        vertx.sharedData().getLock(Config.Keys.COORDINATOR, lock -> {
            if (lock.succeeded()) {
                if (coordinator.compareAndSet(false, true)) {
                    startupProcedureAsCoordinator(promise);
                }
            } else {
                startupProcedureAsWorker(promise);
            }
        });
        return convertToCompletableFuture(promise.future());
    }

    private void checkIn() {
        vertx.sharedData().getCounter(Config.Keys.MEMBER_COUNT, counter -> {
            if (counter.succeeded()) {
                counter.result().incrementAndGet(ar -> {
                    if (ar.failed())
                        handleError(ar.cause());
                });
            } else {
                handleError(counter.cause());
            }
        });
    }

    private void coordinatorBroadcastStart() {
        if (isStarted.get()) {
            vertx.eventBus().publish(CONFIG.getOrDefault(config, Config.Keys.START_CHANNEL), "start");
        }
        isStarted.set(true);
    }

    public void handleError(Throwable cause) {
        cause.printStackTrace();
    }

    private Future<Message<Object>> getIdBatchFromCoordinator(String name) {
        return vertx.eventBus().request(Config.Keys.ID_CHANNEL, new JsonObject().put("name", name));
    }

    public Optional<List<Long>> parseIdBatch(Message<Object> message) {
        if (message.body() instanceof List) {
            return Optional.of((List<Long>) message.body());
        } else {
            return Optional.empty();
        }
    }

    public List<Long> syncGetIdBatchFromCoordinator(final String name) {
        return getIdBatchFromCoordinator(name)
                .onFailure(this::handleError)
                .map(this::parseIdBatch)
                .onFailure(this::handleError)
                .map(Optional::get)
                .onFailure(this::handleError)
                .result();
    }

    private void startupProcedureAsWorker(Handler<AsyncResult<?>> handler) {
        listenForStart(handler);
    }

    private void listenForStart(Handler<AsyncResult<?>> handler) {
        vertx.eventBus().consumer(Config.Keys.START_CHANNEL, msg -> {
            if (msg.body().equals("start")) {
                workerStart(handler);
            }
        });
    }

    private void workerStart(Handler<AsyncResult<?>> handler) {
        handler.handle(Future.succeededFuture());
        phaseOne().get();
        phaseTwo().get();
    }

    private void startupProcedureAsCoordinator(Handler<AsyncResult<?>> handler) {
        vertx.sharedData().getCounter(Config.Keys.MEMBER_COUNT, counter -> {
            if (counter.succeeded()) {
                counter.result().get(ar -> {
                    if (ar.failed()) {
                        handleError(ar.cause());
                    } else {
                        if (ar.result() == (nodeList.isPresent() ? nodeList.get().size() : 1)) {
                            coordinatorBroadcastStart();
                        } else {
                            listenForStart(handler);
                        }
                    }
                });
            } else {
                handleError(counter.cause());
            }
        });
    }

    private void reportMetricsToCluster() {
        vertx.setPeriodic(1000, id -> {
            Promise promise = Promise.promise();
            vertx.sharedData().getAsyncMap(clusterManager.getNodeId(), ar -> {
                if (ar.failed()) {
                    handleError(ar.cause());
                } else {
                    AsyncMap<Object, Object> x = ar.result();
                    CompositeFuture f = Future.all(Arrays.asList(
                            x.put("vertex", subRuntime.getOutputVertexMetrics().stream().reduce((a, b) -> a + b).orElse(0L)),
                            x.put("edge", subRuntime.getOutputEdgeMetrics().stream().reduce((a, b) -> a + b).orElse(0L)),
                            x.put("timestamp", System.currentTimeMillis())));
                    promise.complete(f);
                }
            });
        });
    }


    @Override
    public RunningPhase phaseOne() {
        ((MovementIteratorUtils.IteratorWithFeeder) vertexIterator).start();
        subRuntime.phaseOne(vertexIterator);
        return null;
    }

    @Override
    public Map.Entry<ForkJoinTask, List<Output>> phaseOne(Iterator<List<Object>> iterator) {

        return null;
    }


    @Override
    public RunningPhase phaseTwo() {
        ((MovementIteratorUtils.IteratorWithFeeder) edgeIterator).start();
        phaseTwo(edgeIterator).get();
        return null;
    }

    @Override
    public RunningPhase phaseTwo(Iterator<List<Object>> iterator) {
        subRuntime.phaseTwo(iterator).get();
        return null;
    }

    @Override
    public Optional<String> submitJob(Job job) {
        return Optional.empty();
    }

}
