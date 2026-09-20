/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 * Copyright IBM Corp. 2024, 2025
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
 *
 */

package org.quartz.simpl;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;
import org.slf4j.Logger;

/**
 * A {@link ThreadPool} that runs each job on a Java virtual thread, while capping concurrency with
 * a semaphore sized by {@link #setThreadCount(int)}.
 *
 * <p>Use this instead of {@link SimpleThreadPool} when jobs are mostly waiting on I/O. Keep a
 * positive concurrency limit — unbounded virtual threads would break Quartz's acquire/backpressure
 * model ({@link #blockForAvailableThreads()} drives how many triggers are fetched).
 *
 * <p>Configure via:
 *
 * <pre>
 * org.quartz.threadPool.class = org.quartz.simpl.VirtualThreadPool
 * org.quartz.threadPool.threadCount = 10
 * </pre>
 *
 * <p>Thread priority, daemon, and ThreadGroup settings from {@link SimpleThreadPool} do not apply
 * to virtual threads and are ignored if set.
 *
 * @see SimpleThreadPool
 * @see ThreadPool
 */
@Slf4j
public class VirtualThreadPool implements ThreadPool {

  private int maxConcurrency = -1;

  private Semaphore permits;

  private final AtomicInteger activeCount = new AtomicInteger();

  private final AtomicBoolean shutdown = new AtomicBoolean();

  private final Object lock = new Object();

  private final AtomicLong threadNumber = new AtomicLong();

  @Getter @Setter private String threadNamePrefix;

  private String schedulerInstanceName;

  public Logger getLog() {
    return log;
  }

  @Override
  public int getPoolSize() {
    return getThreadCount();
  }

  /**
   * Maximum number of jobs that may run concurrently (semaphore permits). Named {@code threadCount}
   * so {@code org.quartz.threadPool.threadCount} configures this pool the same way as {@link
   * SimpleThreadPool}.
   */
  public void setThreadCount(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
  }

  public int getThreadCount() {
    return maxConcurrency;
  }

  /**
   * No-op: virtual threads do not support custom thread priority. Accepted so property injection
   * from {@code org.quartz.threadPool.threadPriority} does not fail.
   */
  public void setThreadPriority(int priority) {}

  /**
   * No-op: virtual threads are always daemon-like from the carrier's perspective. Accepted so
   * property injection from {@code org.quartz.threadPool.makeThreadsDaemons} does not fail.
   */
  public void setMakeThreadsDaemons(boolean makeThreadsDaemons) {}

  /**
   * No-op: virtual threads do not use ThreadGroup inheritance. Accepted so property injection from
   * {@code org.quartz.threadPool.threadsInheritGroupOfInitializingThread} does not fail.
   */
  public void setThreadsInheritGroupOfInitializingThread(boolean inheritGroup) {}

  @Override
  public void setInstanceId(String schedInstId) {}

  @Override
  public void setInstanceName(String schedName) {
    this.schedulerInstanceName = schedName;
  }

  @Override
  public void initialize() throws SchedulerConfigException {
    if (permits != null) {
      return;
    }
    if (maxConcurrency <= 0) {
      throw new SchedulerConfigException("Thread count must be > 0");
    }
    permits = new Semaphore(maxConcurrency);
    getLog()
        .info(
            "VirtualThreadPool initialized with max concurrency {} for scheduler '{}'",
            maxConcurrency,
            schedulerInstanceName);
  }

  @Override
  public void shutdown(boolean waitForJobsToComplete) {
    getLog().debug("Shutting down VirtualThreadPool...");
    shutdown.set(true);
    synchronized (lock) {
      lock.notifyAll();
    }

    if (waitForJobsToComplete) {
      boolean interrupted = false;
      try {
        synchronized (lock) {
          while (activeCount.get() > 0) {
            try {
              lock.wait(200);
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      getLog().debug("No executing jobs remaining, VirtualThreadPool stopped.");
    }
    getLog().debug("Shutdown of VirtualThreadPool complete.");
  }

  @Override
  public boolean runInThread(Runnable runnable) {
    if (runnable == null) {
      return false;
    }

    boolean acquired = false;
    synchronized (lock) {
      while (!shutdown.get()) {
        if (permits.tryAcquire()) {
          acquired = true;
          break;
        }
        try {
          lock.wait(500);
        } catch (InterruptedException ignore) {
        }
      }

      if (!acquired) {
        if (shutdown.get()) {
          // Match SimpleThreadPool: still run hand-off jobs during shutdown.
          startVirtualThread(runnable, false);
          return true;
        }
        return false;
      }
    }

    startVirtualThread(runnable, true);
    return true;
  }

  @Override
  public int blockForAvailableThreads() {
    synchronized (lock) {
      while (!shutdown.get() && permits.availablePermits() < 1) {
        try {
          lock.wait(500);
        } catch (InterruptedException ignore) {
        }
      }
      return permits.availablePermits();
    }
  }

  private void startVirtualThread(Runnable runnable, boolean releasePermit) {
    activeCount.incrementAndGet();
    String name = nextThreadName();
    Thread.ofVirtual()
        .name(name)
        .start(
            () -> {
              try {
                runnable.run();
              } finally {
                activeCount.decrementAndGet();
                if (releasePermit) {
                  permits.release();
                }
                synchronized (lock) {
                  lock.notifyAll();
                }
              }
            });
  }

  private String nextThreadName() {
    String prefix = threadNamePrefix;
    if (prefix == null) {
      prefix =
          (schedulerInstanceName == null ? "Quartz" : schedulerInstanceName) + "_VirtualWorker";
    }
    return prefix + "-" + threadNumber.incrementAndGet();
  }
}
