package com.aerospike.movement.runtime.core.local;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JVMGlobalRuntimeMetrics {
    private static final AtomicLong vertexWritePerformanceHighWaterMark = new AtomicLong(0);
    private static final AtomicLong edgeWritePerformanceHighWaterMark = new AtomicLong(0);
    private static final AtomicLong airbrakeFactor = new AtomicLong(0);
    private static final AtomicLong noErrorsSeenBelow = new AtomicLong(0);
    private static final AtomicLong lastTimeAirbrakeIncreased = new AtomicLong(0);
    private static final AtomicLong lastTimeErrorObserved = new AtomicLong(0);
    private static Future<Void> airbrakeAdjustmentTask = null;
    private static final long airbrakeAdjustmentDelayMs = 1;
    private static final AtomicBoolean airbrakeAdjustmentTaskRunning = new AtomicBoolean(false);
    private static final ConcurrentHashMap<String, AtomicLong> errorCounters = new ConcurrentHashMap<>();

    private static final long airBrakeAdjustmentTickTimeMs = 1000;


    public static void reportVertexPerformance(final long vertexRate){
        if(vertexRate > vertexWritePerformanceHighWaterMark.get()){
            vertexWritePerformanceHighWaterMark.set(vertexRate);
        }

    }
    public static void reportEdgePerformance(final long edgeRate){
        if(edgeRate > edgeWritePerformanceHighWaterMark.get()){
            edgeWritePerformanceHighWaterMark.set(edgeRate);
        }
    }

    public static void reportError(final Exception error){
        errorCounters.computeIfAbsent(error.getClass().getName(), k -> new AtomicLong(0)).incrementAndGet();
    }
    public long getAirbrakeFactor(){
        return airbrakeFactor.get();
    }

    public static long computeAirbrakeAdjustment(){
        long now = System.currentTimeMillis();
        long timeSinceLastError = now - lastTimeErrorObserved.get();
        long adjustment = 0;
        if(airbrakeFactor.get() > noErrorsSeenBelow.get() + 2){
            adjustment -= 1;
        }

        if(timeSinceLastError < airBrakeAdjustmentTickTimeMs) {
            adjustment += 1;
        }

        return adjustment;
    }
    public static void airbrakeAdjustmentTick() {
        long now = System.currentTimeMillis();
        long timeSinceLastIncrease = now - lastTimeAirbrakeIncreased.get();
        if(timeSinceLastIncrease > airBrakeAdjustmentTickTimeMs) {
            long adjustment = computeAirbrakeAdjustment();
            if(adjustment > 0) {
                airbrakeFactor.addAndGet(adjustment);
                lastTimeAirbrakeIncreased.set(now);
            }
        }

        }

    public static void startAirbrakeAdjustmentTask() {
        if (airbrakeAdjustmentTaskRunning.compareAndSet(false, true)) {
            airbrakeAdjustmentTask = CompletableFuture.supplyAsync(() -> {
                while (true) {
                    try {
                        Thread.sleep(airBrakeAdjustmentTickTimeMs);
                        airbrakeAdjustmentTick();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

}
