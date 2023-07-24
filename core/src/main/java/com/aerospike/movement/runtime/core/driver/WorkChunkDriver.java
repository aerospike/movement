package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.Handler;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.Threadsafe;
import org.apache.commons.configuration2.Configuration;

import java.util.Iterator;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class WorkChunkDriver extends Loadable implements Iterator<WorkChunk>, Threadsafe {
    private static final AtomicReference<WorkChunkDriver> INSTANCE = new AtomicReference<>();
    protected final Configuration config;
    protected static Queue<UUID> outstanding = new ConcurrentLinkedQueue<>();
    private final AtomicLong chunksEmitted, chunksAcknowledged;

    protected final ErrorHandler errorHandler;

    protected WorkChunkDriver(final ConfigurationBase configurationMeta, final Configuration config) {
        super(configurationMeta, config);
        this.config = config;
        this.chunksEmitted = new AtomicLong(0);
        this.chunksAcknowledged = new AtomicLong(0);
        this.errorHandler = RuntimeUtil.loadErrorHandler(this,config);
    }




    public void acknowledgeComplete(final UUID workChunkId) {
//        if (!outstanding.remove(workChunkId))
//            throw new RuntimeException(String.format("workChunkId %s is not in the outstanding queue", workChunkId));
        chunksAcknowledged.addAndGet(1);
    }


    @Override
    public boolean isThreadsafe() {
        return true;
    }

    public Iterator<WorkChunk> iterator() {
        return this;
    }

    protected abstract AtomicBoolean getInitialized();
    protected void onNext(){
        if(!getInitialized().get()){
            throw new RuntimeException("WorkChunkDriver is not initialized");
        }
    }
    protected void onNextValue(final WorkChunk value){
        outstanding.add(value.getId());
    }
    public void close() throws Exception {
        chunksAcknowledged.set(0);
        getInitialized().set(false);
//        waitOnOutstanding(1000);
        //@todo this may not be necessary. At any rate acknowledging the WorkList has been consumed by the emitter
        // is not the same as the output acknowledging it has processed them all
        INSTANCE.set(null);
    }

    private void waitOnOutstanding(final long maxWait) {
        long waitTime = 0;
        while (!outstanding.isEmpty() && waitTime < maxWait) {
            try {
                Thread.sleep(50);
                waitTime += 50;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (!outstanding.isEmpty()) {
            throw new RuntimeException(String.format("Timeout exceeded WorkChunkDriver has %d outstanding work chunks during phase %s", outstanding.size(), RuntimeUtil.getCurrentPhase(config)));
        }

    }

}
