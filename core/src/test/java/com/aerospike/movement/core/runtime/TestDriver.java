/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.core.runtime;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.OutputId;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkItem;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.util.core.iterator.ConfiguredRangeSupplier;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.OUTPUT_ID_DRIVER;
import static com.aerospike.movement.config.core.ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/*
  Created by Grant Haywood grant@iowntheinter.net
  7/23/23
*/
public class TestDriver {
    @Test
    public void testWorkChunkDriver() {
        GeneratedOutputIdDriver.closeInstance();

        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(BATCH_SIZE, 10);
            put(WORK_CHUNK_DRIVER_PHASE_ONE, SuppliedWorkChunkDriver.class.getName());
            put(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, Runtime.PHASE.ONE.name());
            put(ConfiguredRangeSupplier.Config.Keys.RANGE_BOTTOM, 0);
            put(ConfiguredRangeSupplier.Config.Keys.RANGE_TOP, 100);
            put(SuppliedWorkChunkDriver.Config.Keys.ITERATOR_SUPPLIER_PHASE_ONE, ConfiguredRangeSupplier.class.getName());
        }});
        SuppliedWorkChunkDriver driver = (SuppliedWorkChunkDriver) SuppliedWorkChunkDriver.open(config);
        final WorkChunk chunk = driver.getNext().get();
        assertTrue(chunk.hasNext());
        long ctr = 0;
        while (chunk.hasNext()) {
            final WorkItem id = chunk.next();
            assertEquals(ctr++, id.getId());
        }
        assertEquals(10, ctr);
    }

    @Test
    public void testOutputIDDriver() throws Exception {
        GeneratedOutputIdDriver.closeInstance();

        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(BATCH_SIZE, 10);
            put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
            put(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, Runtime.PHASE.ONE.name());
            put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, 0);
            put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, 100);
        }});
        GeneratedOutputIdDriver driver = (GeneratedOutputIdDriver) GeneratedOutputIdDriver.open(config);
        final OutputId x = driver.getNext().get();
        assertEquals(0L, x.getId());
        long y = (Long) x.getId();
        Optional<OutputId> things;
        while (true) {
            things = driver.getNext();
            if (things.isEmpty()) {
                break;
            }
            assertEquals((Long) y + 1, things.get().getId());
            y++;
        }
        assertEquals(100L, y + 1);
        driver.close();
    }

    @Test
    public void testOutputIdDriverConcurrent() {
        GeneratedOutputIdDriver.closeInstance();
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(BATCH_SIZE, 100);
            put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
            put(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, Runtime.PHASE.ONE.name());
            put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, 0);
            put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, 100_000);
        }});
        GeneratedOutputIdDriver driver = (GeneratedOutputIdDriver) GeneratedOutputIdDriver.open(config);
        final AtomicLong hitCounter = new AtomicLong(0);
        final Set<Long> concurrentUniqueSet = new ConcurrentSkipListSet<>();
        LongStream.range(0, 1000).parallel().forEach(l -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Optional<OutputId> things;
            while (true) {
                things = driver.getNext();
                if (things.isEmpty()) {
                    break;
                }
                final Long newVal = (Long) things.get().getId();
                if (!concurrentUniqueSet.add(newVal)) {
                    throw new RuntimeException("Duplicate value found: " + newVal);
                }
                hitCounter.incrementAndGet();
            }
        });
        assertEquals(100_000L, hitCounter.get());
    }

    @Test
    @Ignore
    @Deprecated
    public void testWorkChunkDriverConcurrent() throws Exception {
        GeneratedOutputIdDriver.closeInstance();
        SuppliedWorkChunkDriver.closeStatic();
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(BATCH_SIZE, 100);
            put(WORK_CHUNK_DRIVER_PHASE_ONE, SuppliedWorkChunkDriver.class.getName());
            put(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, Runtime.PHASE.ONE.name());
            put(ConfiguredRangeSupplier.Config.Keys.RANGE_BOTTOM, 0);
            put(ConfiguredRangeSupplier.Config.Keys.RANGE_TOP, 100_000);
            put(SuppliedWorkChunkDriver.Config.Keys.ITERATOR_SUPPLIER_PHASE_ONE, ConfiguredRangeSupplier.class.getName());
        }});
        SuppliedWorkChunkDriver driver = (SuppliedWorkChunkDriver) SuppliedWorkChunkDriver.open(config);
        final AtomicLong elementHitCounter = new AtomicLong(0);
        final AtomicLong chunkHitCounter = new AtomicLong(0);
        final Set<Long> concurrentUniqueSet = new ConcurrentSkipListSet<>();
        LongStream.range(0, 1000).parallel().forEach(l -> {
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (driver.getNext().isPresent()) {
                final WorkChunk workChunk = driver.getNext().get();
                if(workChunk.hasNext())
                    chunkHitCounter.incrementAndGet();
                while (workChunk.hasNext()) {
                    final WorkItem newVal = workChunk.next();
                    if (!concurrentUniqueSet.add((Long) newVal.getId())) {
                        throw new RuntimeException("Duplicate value found: " + newVal);
                    }
                    elementHitCounter.incrementAndGet();
                }
            }
        });
        assertEquals(1000L, chunkHitCounter.get());
        assertEquals(100_000L, elementHitCounter.get());
    }
}
