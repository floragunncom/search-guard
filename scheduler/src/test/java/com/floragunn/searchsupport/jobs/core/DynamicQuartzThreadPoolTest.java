/*
 * Copyright 2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.searchsupport.jobs.core;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Test;

public class DynamicQuartzThreadPoolTest {

    @Test
    public void basicTest() {
        DynamicQuartzThreadPool threadPool = new DynamicQuartzThreadPool(null, "Test", "", 3, Thread.NORM_PRIORITY, Duration.ofSeconds(4));
        Set<String> completedTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());

        threadPool.setPollingIntervalMs(50);

        Assert.assertEquals(0, threadPool.getCurrentWorkerCount());
        Assert.assertEquals(0, threadPool.getCurrentBusyWorkerCount());
        Assert.assertEquals(0, threadPool.getCurrentAvailableWorkerCount());

        Assert.assertTrue(threadPool.runInThread(() -> {
            sleep(3000);
            completedTasks.add("A");
        }));

        Assert.assertEquals(1, threadPool.getCurrentWorkerCount());
        Assert.assertEquals(1, threadPool.getCurrentBusyWorkerCount());
        Assert.assertEquals(0, threadPool.getCurrentAvailableWorkerCount());

        Assert.assertTrue(threadPool.runInThread(() -> {
            sleep(3000);
            completedTasks.add("B");
        }));

        Assert.assertEquals(2, threadPool.getCurrentWorkerCount());
        Assert.assertEquals(2, threadPool.getCurrentBusyWorkerCount());
        Assert.assertEquals(0, threadPool.getCurrentAvailableWorkerCount());

        Assert.assertTrue(threadPool.runInThread(() -> {
            sleep(1000);
            completedTasks.add("C");
        }));

        Assert.assertEquals(3, threadPool.getCurrentWorkerCount());
        Assert.assertEquals(3, threadPool.getCurrentBusyWorkerCount());
        Assert.assertEquals(0, threadPool.getCurrentAvailableWorkerCount());

        Assert.assertFalse(threadPool.runInThread(() -> {
            sleep(3000);
            completedTasks.add("X");
        }));

        Assert.assertEquals(3, threadPool.getCurrentWorkerCount());
        Assert.assertEquals(3, threadPool.getCurrentBusyWorkerCount());
        Assert.assertEquals(0, threadPool.getCurrentAvailableWorkerCount());

        int available = threadPool.blockForAvailableThreads();

        Assert.assertTrue(available + "", available > 0);

        Assert.assertTrue(threadPool.runInThread(() -> {
            sleep(100);
            completedTasks.add("D");
        }));

        awaitAssert("Busy worker count did not reach 0: " + threadPool + "", () -> threadPool.getCurrentBusyWorkerCount() == 0,
                Duration.ofSeconds(10));

        Assert.assertNotEquals(0, threadPool.getCurrentWorkerCount());
        Assert.assertEquals(threadPool.getCurrentWorkerCount(), threadPool.getCurrentAvailableWorkerCount());

        Assert.assertEquals(ImmutableSet.of("A", "B", "C", "D"), completedTasks);

        awaitAssert("Idle threads have not been timed out: " + threadPool, () -> threadPool.getCurrentWorkerCount() == 0, Duration.ofSeconds(20));

        Assert.assertEquals(0, threadPool.getCurrentWorkerCount());
        Assert.assertEquals(0, threadPool.getCurrentAvailableWorkerCount());

        Assert.assertTrue(threadPool.runInThread(() -> {
            completedTasks.add("E");
        }));

        awaitAssert("Task E finished", () -> completedTasks.contains("E"), Duration.ofSeconds(10));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void awaitAssert(String message, Supplier<Boolean> condition, Duration maxWaitingTime) {
        long timeout = System.currentTimeMillis() + maxWaitingTime.toMillis();
        while (!condition.get() && timeout >= System.currentTimeMillis()) {
            sleep(50);
        }

        if (condition.get()) {
            return;
        } else {
            Assert.fail(message);
        }
    }
}
