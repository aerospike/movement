package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.util.ConfigurationBase;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.graph.generator.runtime.DistributedStreamRuntime.Config.Keys.INTERFACES;
import static java.lang.Runtime.getRuntime;

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
    private final Iterator<List<Long>> vertexIterator;
    private final Iterator<List<Long>> edgeIterator;

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
            private static final String ID_CHANNEL = "idChannel";


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
        this.vertexIterator = new IteratorWithFeeder(vertx, Config.Keys.VERTEX);
        this.edgeIterator = new IteratorWithFeeder(vertx, Config.Keys.EDGE);

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

    private void handleError(Throwable cause) {
        cause.printStackTrace();
    }

    private Future<Message<Object>> getIdBatchFromCoordinator(String name) {
        return vertx.eventBus().request(Config.Keys.ID_CHANNEL, new JsonObject().put("name", name));
    }

    private Optional<List<Long>> parseIdBatch(Message<Object> message) {
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
        processVertexStream();
        processEdgeStream();
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
    public void processVertexStream() {
        ((IteratorWithFeeder) vertexIterator).start();
        subRuntime.processVertexStream(vertexIterator);
    }


    @Override
    public void processEdgeStream() {
        ((IteratorWithFeeder) edgeIterator).start();
        subRuntime.processEdgeStream(edgeIterator);
    }

    private class IteratorWithFeeder implements Iterator<List<Long>> {
        final BlockingQueue<List<Long>> feeder = new LinkedBlockingQueue<>();
        private final Vertx vertx;
        private final String idType;
        final AtomicBoolean isStarted = new AtomicBoolean(false);

        public IteratorWithFeeder(final Vertx vertx, final String idType) {
            this.vertx = vertx;
            this.idType = idType;
        }

        public void start() {
            if (!isStarted.compareAndSet(false, true)) {
                vertx.setPeriodic(1000, id -> {
                    int fillSize = 3;
                    if (feeder.isEmpty() || feeder.size() < fillSize) {
                        vertx.eventBus().request(Config.Keys.ID_CHANNEL, new JsonObject().put("type", idType), ar -> {
                            if (ar.succeeded()) {
                                Optional<List<Long>> batch = parseIdBatch(ar.result());
                                if (batch.isPresent()) {
                                    feed(batch.get());
                                }
                            } else {
                                handleError(ar.cause());
                            }
                        });
                    }
                });
            }
        }

        public void feed(List<Long> buffer) {
            feeder.add(buffer);
        }

        @Override
        public boolean hasNext() {
            return !feeder.isEmpty();
        }

        @Override
        public List<Long> next() {
            try {
                return feeder.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
