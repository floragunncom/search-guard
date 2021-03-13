/*
 * Includes code from the following Apache 2 licensed work:
 * 
 *   https://github.com/quartz-scheduler/quartz/blob/master/quartz-core/src/main/java/org/quartz/simpl/SimpleThreadPool.java 
 * 
 * Reproduction of the original copyright notice below:
 *   
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
 * 
 * Otherwise:
 * 
 * Copyright 2021 floragunn GmbH
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

public class DynamicQuartzThreadPool implements ThreadPool {
    private static final Logger log = LogManager.getLogger(DynamicQuartzThreadPool.class);

    private final String threadPoolName;
    private final int maxThreadCount;
    private final int threadPriority;
    private final Duration threadKeepAlive;

    private final Deque<WorkerThread> availableWorkers;
    private final Set<WorkerThread> busyWorkers = new HashSet<>();
    private final Set<WorkerThread> allWorkers = new HashSet<>();
    private final AtomicInteger threadIdCounter = new AtomicInteger();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workerAvailable = lock.newCondition();

    private boolean isShutdown = false;

    private ThreadGroup threadGroup;

    private long pollingIntervalMs = 1000;

    /**
     * <p>
     * Create a new <code>SimpleThreadPool</code> with the specified number
     * of <code>Thread</code> s that have the given priority.
     * </p>
     * 
     * @param threadCount
     *          the max number of worker <code>Threads</code> in the pool, must
     *          be > 0.
     * @param threadPriority
     *          the thread priority for the worker threads.
     * 
     * @see java.lang.Thread
     */
    public DynamicQuartzThreadPool(String threadPoolName, int maxThreadCount, int threadPriority, Duration threadKeepAlive) {
        this.threadPoolName = threadPoolName;
        this.maxThreadCount = maxThreadCount;
        this.threadPriority = threadPriority;
        this.threadKeepAlive = threadKeepAlive;
        this.availableWorkers = new ArrayDeque<>(maxThreadCount);
    }

    /**
     * <p>
     * Run the given <code>Runnable</code> object in the next available
     * <code>Thread</code>. If while waiting the thread pool is asked to
     * shut down, the Runnable is executed immediately within a new additional
     * thread.
     * </p>
     * 
     * @param runnable
     *          the <code>Runnable</code> to be added.
     */
    @Override
    public boolean runInThread(Runnable runnable) {

        if (runnable == null) {
            return false;
        }

        lock.lock();
        try {

            WorkerThread workerThread = this.availableWorkers.pollLast();

            if (workerThread != null) {
                busyWorkers.add(workerThread);
                if (workerThread.run(runnable)) {
                    return true;
                } else {
                    // Thread seems to have died. Fall through to creation.
                    busyWorkers.remove(workerThread);
                }
            }

            if (this.allWorkers.size() < maxThreadCount) {
                createNewWorker(runnable);
                return true;
            }

            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int blockForAvailableThreads() {
        lock.lock();
        try {
            while (availableWorkers.isEmpty() && busyWorkers.size() >= this.maxThreadCount && !isShutdown) {
                try {
                    workerAvailable.await(pollingIntervalMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.warn("Unexpected InterruptedException", e);
                }
            }

            return this.maxThreadCount - busyWorkers.size();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public int getPoolSize() {
        return maxThreadCount;
    }

    @Override
    public void initialize() throws SchedulerConfigException {

    }

    @Override
    public void shutdown(boolean waitForJobsToComplete) {
        try {

            lock.lock();
            try {
                log.debug("Shutting down " + this);

                isShutdown = true;

                for (WorkerThread availableWorkerThread : availableWorkers) {
                    availableWorkerThread.shutdown();
                    allWorkers.remove(availableWorkerThread);
                }

                availableWorkers.clear();

                for (WorkerThread workerThread : allWorkers) {
                    workerThread.shutdown();
                }

                // Give waiting (wait(1000)) worker threads a chance to shut down.
                // Active worker threads will shut down after finishing their
                // current job.
                workerAvailable.signalAll();
            } finally {
                lock.unlock();
            }

            if (waitForJobsToComplete) {

                boolean interrupted = false;
                try {

                    // Wait until all worker threads are shut down
                    lock.lock();

                    try {
                        while (busyWorkers.size() > 0) {
                            try {
                                log.debug("Waiting for threads " + busyWorkers + " to shut down");

                                // note: with waiting infinite time the
                                // application may appear to 'hang'.
                                workerAvailable.await(1, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                interrupted = true;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }

                    log.debug("All busy workers finished. Waiting for worker threads to terminate: " + allWorkers);

                    for (WorkerThread workerThread : allWorkers) {
                        try {
                            workerThread.join(5000);

                            if (workerThread.isAlive()) {
                                Throwable stackTraceHolder = new Exception();
                                stackTraceHolder.setStackTrace(workerThread.getStackTrace());
                                log.warn("Worker thread did not properly terminate: " + workerThread, stackTraceHolder);
                            }
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }

                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }

                log.debug("No executing jobs remaining, all threads stopped.");
            }

            lock.lock();
            try {
                allWorkers.clear();
            } finally {
                lock.unlock();
            }

            log.debug("Shutdown of threadpool complete.");
        } catch (Exception e) {
            log.error("Encountered error while shutting down", e);
        }
    }

    public synchronized int getCurrentWorkerCount() {
        return this.allWorkers.size();
    }

    public synchronized int getCurrentAvailableWorkerCount() {
        return this.availableWorkers.size();
    }

    public synchronized int getCurrentBusyWorkerCount() {
        return this.busyWorkers.size();
    }

    @Override
    public void setInstanceId(String schedulerInstanceId) {
    }

    @Override
    public void setInstanceName(String schedulerInstanceName) {
    }

    protected WorkerThread createNewWorker(Runnable runnable) {
        WorkerThread result = new WorkerThread(threadGroup, this.threadPoolName + "/worker_" + threadIdCounter.incrementAndGet(), threadPriority,
                true, runnable);

        lock.lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Creating new worker: " + result);
            }

            result.start();

            allWorkers.add(result);

            if (runnable != null) {
                busyWorkers.add(result);
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    protected void makeAvailable(WorkerThread workerThread) {
        if (log.isDebugEnabled()) {
            log.debug("makeAvailable: " + workerThread);
        }

        lock.lock();
        try {
            if (!isShutdown) {
                availableWorkers.add(workerThread);
            }
            busyWorkers.remove(workerThread);
            workerAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    protected void onTermination(WorkerThread workerThread) {

        if (log.isDebugEnabled()) {
            log.debug("onTermination: " + workerThread);
        }

        lock.lock();
        try {
            allWorkers.remove(workerThread);
            busyWorkers.remove(workerThread);
            availableWorkers.remove(workerThread);
        } finally {
            lock.unlock();
        }
    }

    /**
     * A Worker loops, waiting to execute tasks.
     */
    class WorkerThread extends Thread {

        private boolean running = true;
        private Runnable runnable = null;
        private Instant idleSince = Instant.now();

        WorkerThread(ThreadGroup threadGroup, String name, int prio, boolean isDaemon, Runnable runnable) {
            super(threadGroup, name);
            this.runnable = runnable;
            setPriority(prio);
            setDaemon(isDaemon);
        }

        /**
         * Signal the thread that it should terminate.
         */
        synchronized void shutdown() {
            running = false;
            notifyAll();
        }

        public synchronized boolean run(Runnable newRunnable) {
            if (!running) {
                return false;
            }

            if (runnable != null) {
                throw new IllegalStateException("Already running a Runnable!");
            }

            runnable = newRunnable;
            notifyAll();

            return true;

        }

        /**
         * Loop, executing targets as they are received.
         */
        @Override
        public void run() {
            while (running) {
                try {
                    Runnable doRun = null;

                    synchronized (this) {
                        if (runnable != null) {
                            doRun = runnable;
                        } else if (idleSince.plus(threadKeepAlive).isBefore(Instant.now())) {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Retiring " + this + " due to inactivity. Last activity: " + idleSince + "; keep alive: " + threadKeepAlive);
                            }
                            running = false;
                            break;
                        } else {
                            wait(pollingIntervalMs);

                            if (!running) {
                                break;
                            } else {
                                continue;
                            }
                        }
                    }

                    if (doRun != null) {
                        try {
                            runnable.run();
                        } catch (Throwable e) {
                            log.error("Error while executing the Runnable: ", e);
                        } finally {
                            synchronized (this) {
                                runnable = null;
                                idleSince = Instant.now();
                            }
                            makeAvailable(this);
                        }
                    }

                } catch (InterruptedException unblock) {
                    // do nothing (loop will terminate if shutdown() was called
                    log.warn("Worker thread was interrupt()'ed.", unblock);
                } catch (Exception e) {
                    log.error("Unexpected exception in " + this, e);
                }
            }

            onTermination(this);

            try {
                log.debug("WorkerThread is shut down.");
            } catch (Exception e) {
                // ignore to help with a tomcat glitch
            }
        }
    }

    @Override
    public String toString() {
        return "DynamicQuartzThreadPool [threadPoolName=" + threadPoolName + ", maxThreadCount=" + maxThreadCount + ", threadPriority="
                + threadPriority + ", threadKeepAlive=" + threadKeepAlive + "]";
    }

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public void setPollingIntervalMs(long pollingIntervalMs) {
        this.pollingIntervalMs = pollingIntervalMs;
    }

}
