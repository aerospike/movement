/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.coordonation;

import java.util.concurrent.CountDownLatch;

public class WaitGroup {
    private final CountDownLatch countDownLatch;

    private WaitGroup(int count) {
        countDownLatch = new CountDownLatch(count);
    }

    public static WaitGroup of(int count) {
        return new WaitGroup(count);
    }

    public void done() {
        countDownLatch.countDown();
    }

    public boolean completed() {
        return countDownLatch.getCount() == 0;
    }

    public void await() throws InterruptedException {
        countDownLatch.await();
    }
}
