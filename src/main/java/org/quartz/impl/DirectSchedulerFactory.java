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

package org.quartz.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerPlugin;
import org.quartz.spi.ThreadExecutor;
import org.quartz.spi.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A singleton implementation of <code>{@link org.quartz.SchedulerFactory}</code>.
 *
 * <p>Here are some examples of using this class:
 *
 * <p>To create a scheduler that does not write anything to the database (is not persistent), you
 * can call <code>createVolatileScheduler</code>:
 *
 * <pre>
 *  DirectSchedulerFactory.getInstance().createVolatileScheduler(10); // 10 threads * // don't forget to start the scheduler: DirectSchedulerFactory.getInstance().getScheduler().start();
 * </pre>
 *
 * <p>Several create methods are provided for convenience. All create methods eventually end up
 * calling the create method with thread pool, job store, and optional plugins.
 *
 * <p>Here is an example of using this method:
 *
 * <pre>
 * SimpleThreadPool threadPool = new SimpleThreadPool(maxThreads, Thread.NORM_PRIORITY);
 * threadPool.initialize();
 * JobStore jobStore = new RAMJobStore();
 * DirectSchedulerFactory.getInstance().createScheduler("My Quartz Scheduler", "My Instance", threadPool, jobStore);
 * DirectSchedulerFactory.getInstance().getScheduler("My Quartz Scheduler").start();
 * </pre>
 *
 * <p>You can also use {@link org.quartz.impl.mongodb.MongoJobStore} instead of {@link RAMJobStore}:
 *
 * <pre>
 *  org.quartz.impl.mongodb.MongoJobStore mongoJobStore = new org.quartz.impl.mongodb.MongoJobStore();
 *  mongoJobStore.setMongoUri("mongodb://localhost:27017");
 *  mongoJobStore.setDbName("quartz");
 *  mongoJobStore.setInstanceId("My Instance");
 *  mongoJobStore.setClustered(true);
 * </pre>
 *
 * @author Mohammad Rezaei
 * @author James House
 * @see JobStore
 * @see ThreadPool
 */
public class DirectSchedulerFactory implements SchedulerFactory {

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constants.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */
  public static final String DEFAULT_INSTANCE_ID = "SIMPLE_NON_CLUSTERED";

  public static final String DEFAULT_SCHEDULER_NAME = "SimpleQuartzScheduler";

  private static final DefaultThreadExecutor DEFAULT_THREAD_EXECUTOR = new DefaultThreadExecutor();

  private static final int DEFAULT_BATCH_MAX_SIZE = 1;

  private static final long DEFAULT_BATCH_TIME_WINDOW = 0L;

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Data members.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  private boolean initialized = false;

  private static final DirectSchedulerFactory instance = new DirectSchedulerFactory();

  private final Logger log = LoggerFactory.getLogger(getClass());

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constructors.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  protected Logger getLog() {
    return log;
  }

  /** Constructor */
  protected DirectSchedulerFactory() {}

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Interface.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  public static DirectSchedulerFactory getInstance() {
    return instance;
  }

  /**
   * Creates an in memory job store (<code>{@link RAMJobStore}</code>) The thread priority is set to
   * Thread.NORM_PRIORITY
   *
   * @param maxThreads The number of threads in the thread pool
   * @throws SchedulerException if initialization failed.
   */
  public void createVolatileScheduler(int maxThreads) throws SchedulerException {
    SimpleThreadPool threadPool = new SimpleThreadPool(maxThreads, Thread.NORM_PRIORITY);
    JobStore jobStore = new RAMJobStore();
    this.createScheduler(threadPool, jobStore);
  }

  /**
   * Creates a scheduler using the specified thread pool and job store. This scheduler can be
   * retrieved via {@link DirectSchedulerFactory#getScheduler()}
   *
   * @param threadPool The thread pool for executing jobs
   * @param jobStore The type of job store
   * @throws SchedulerException if initialization failed
   */
  public void createScheduler(ThreadPool threadPool, JobStore jobStore) throws SchedulerException {
    createScheduler(DEFAULT_SCHEDULER_NAME, DEFAULT_INSTANCE_ID, threadPool, jobStore);
  }

  /**
   * Same as {@link DirectSchedulerFactory#createScheduler(ThreadPool threadPool, JobStore
   * jobStore)}, with the addition of specifying the scheduler name and instance ID. This scheduler
   * can only be retrieved via {@link DirectSchedulerFactory#getScheduler(String)}
   *
   * @param schedulerName The name for the scheduler.
   * @param schedulerInstanceId The instance ID for the scheduler.
   * @param threadPool The thread pool for executing jobs
   * @param jobStore The type of job store
   * @throws SchedulerException if initialization failed
   */
  public void createScheduler(
      String schedulerName, String schedulerInstanceId, ThreadPool threadPool, JobStore jobStore)
      throws SchedulerException {
    createScheduler(schedulerName, schedulerInstanceId, threadPool, jobStore, null, -1, -1);
  }

  /** Creates a scheduler using the specified thread pool, job store, and plugins. */
  public void createScheduler(
      String schedulerName,
      String schedulerInstanceId,
      ThreadPool threadPool,
      JobStore jobStore,
      Map<String, SchedulerPlugin> schedulerPluginMap,
      long idleWaitTime,
      long dbFailureRetryInterval)
      throws SchedulerException {
    createScheduler(
        schedulerName,
        schedulerInstanceId,
        threadPool,
        DEFAULT_THREAD_EXECUTOR,
        jobStore,
        schedulerPluginMap,
        idleWaitTime,
        dbFailureRetryInterval,
        DEFAULT_BATCH_MAX_SIZE,
        DEFAULT_BATCH_TIME_WINDOW,
        false);
  }

  /** Creates a scheduler using the specified thread pool, job store, and plugins. */
  public void createScheduler(
      String schedulerName,
      String schedulerInstanceId,
      ThreadPool threadPool,
      ThreadExecutor threadExecutor,
      JobStore jobStore,
      Map<String, SchedulerPlugin> schedulerPluginMap,
      long idleWaitTime,
      long dbFailureRetryInterval,
      int maxBatchSize,
      long batchTimeWindow,
      boolean makeSchedThreadDaemon)
      throws SchedulerException {

    JobRunShellFactory jrsf = new StdJobRunShellFactory();

    threadPool.setInstanceName(schedulerName);
    threadPool.initialize();

    QuartzSchedulerResources qrs = new QuartzSchedulerResources();

    qrs.setName(schedulerName);
    qrs.setInstanceId(schedulerInstanceId);
    qrs.setMakeSchedulerThreadDaemon(makeSchedThreadDaemon);
    SchedulerDetailsSetter.setDetails(threadPool, schedulerName, schedulerInstanceId);
    qrs.setJobRunShellFactory(jrsf);
    qrs.setThreadPool(threadPool);
    qrs.setThreadExecutor(threadExecutor);
    qrs.setJobStore(jobStore);
    qrs.setMaxBatchSize(maxBatchSize);
    qrs.setBatchTimeWindow(batchTimeWindow);

    if (schedulerPluginMap != null) {
      for (SchedulerPlugin schedulerPlugin : schedulerPluginMap.values()) {
        qrs.addSchedulerPlugin(schedulerPlugin);
      }
    }

    QuartzScheduler qs = new QuartzScheduler(qrs, idleWaitTime, dbFailureRetryInterval);

    ClassLoadHelper cch = new CascadingClassLoadHelper();
    cch.initialize();

    SchedulerDetailsSetter.setDetails(jobStore, schedulerName, schedulerInstanceId);

    jobStore.initialize(cch, qs.getSchedulerSignaler());

    Scheduler scheduler = new StdScheduler(qs);

    jrsf.initialize(scheduler);

    qs.initialize();

    if (schedulerPluginMap != null) {
      for (Entry<String, SchedulerPlugin> pluginEntry : schedulerPluginMap.entrySet()) {
        pluginEntry.getValue().initialize(pluginEntry.getKey(), scheduler, cch);
      }
    }

    getLog().info("Quartz scheduler '{}", scheduler.getSchedulerName());

    getLog().info("Quartz scheduler version: {}", qs.getVersion());

    SchedulerRepository schedRep = SchedulerRepository.getInstance();

    qs.addNoGCObject(schedRep);

    schedRep.bind(scheduler);

    initialized = true;
  }

  /**
   * Returns a handle to the Scheduler produced by this factory.
   *
   * <p>you must call createScheduler methods before calling getScheduler()
   */
  public Scheduler getScheduler() throws SchedulerException {
    if (!initialized) {
      throw new SchedulerException(
          "you must call createScheduler methods before calling getScheduler()");
    }

    return getScheduler(DEFAULT_SCHEDULER_NAME);
  }

  /** Returns a handle to the Scheduler with the given name, if it exists. */
  public Scheduler getScheduler(String schedName) throws SchedulerException {
    SchedulerRepository schedRep = SchedulerRepository.getInstance();

    return schedRep.lookup(schedName);
  }

  /** Returns a handle to all known Schedulers (made by any StdSchedulerFactory instance.). */
  public Collection<Scheduler> getAllSchedulers() throws SchedulerException {
    return SchedulerRepository.getInstance().lookupAll();
  }
}
