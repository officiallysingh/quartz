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

package org.quartz.core;

import java.util.ArrayList;
import java.util.List;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerPlugin;
import org.quartz.spi.ThreadExecutor;
import org.quartz.spi.ThreadPool;

/**
 * Contains all of the resources (<code>JobStore</code>,<code>ThreadPool</code>, etc.) necessary to
 * create a <code>{@link QuartzScheduler}</code> instance.
 *
 * @see QuartzScheduler
 * @author James House
 */
public class QuartzSchedulerResources {

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Data members.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  private String name;

  private String instanceId;

  private String threadName;

  private ThreadPool threadPool;

  private JobStore jobStore;

  private JobRunShellFactory jobRunShellFactory;

  private final List<SchedulerPlugin> schedulerPlugins = new ArrayList<>(10);

  private boolean makeSchedulerThreadDaemon = false;

  private boolean threadsInheritInitializersClassLoadContext = false;

  private ThreadExecutor threadExecutor;

  private long batchTimeWindow = 0;

  private int maxBatchSize = 1;

  private boolean interruptJobsOnShutdown = false;
  private boolean interruptJobsOnShutdownWithWait = false;

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constructors.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /** Create an instance with no properties initialized. */
  public QuartzSchedulerResources() {
    // do nothing...
  }

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Interface.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /** Get the name for the <code>{@link QuartzScheduler}</code>. */
  public String getName() {
    return name;
  }

  /**
   * Set the name for the <code>{@link QuartzScheduler}</code>.
   *
   * @exception IllegalArgumentException if name is null or empty.
   */
  public void setName(String name) {
    if (name == null || name.trim().isEmpty()) {
      throw new IllegalArgumentException("Scheduler name cannot be empty.");
    }

    this.name = name;

    if (threadName == null) {
      // thread name not already set, use default thread name
      setThreadName(name + "_QuartzSchedulerThread");
    }
  }

  /** Get the instance Id for the <code>{@link QuartzScheduler}</code>. */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * Set the name for the <code>{@link QuartzScheduler}</code>.
   *
   * @exception IllegalArgumentException if name is null or empty.
   */
  public void setInstanceId(String instanceId) {
    if (instanceId == null || instanceId.trim().isEmpty()) {
      throw new IllegalArgumentException("Scheduler instanceId cannot be empty.");
    }

    this.instanceId = instanceId;
  }

  public static String getUniqueIdentifier(String schedName, String schedInstId) {
    return schedName + "_$_" + schedInstId;
  }

  public String getUniqueIdentifier() {
    return getUniqueIdentifier(name, instanceId);
  }

  /** Get the name for the <code>{@link QuartzSchedulerThread}</code>. */
  public String getThreadName() {
    return threadName;
  }

  /**
   * Set the name for the <code>{@link QuartzSchedulerThread}</code>.
   *
   * @exception IllegalArgumentException if name is null or empty.
   */
  public void setThreadName(String threadName) {
    if (threadName == null || threadName.trim().isEmpty()) {
      throw new IllegalArgumentException("Scheduler thread name cannot be empty.");
    }

    this.threadName = threadName;
  }

  /**
   * Get the <code>{@link ThreadPool}</code> for the <code>{@link QuartzScheduler}</code> to use.
   */
  public ThreadPool getThreadPool() {
    return threadPool;
  }

  /**
   * Set the <code>{@link ThreadPool}</code> for the <code>{@link QuartzScheduler}</code> to use.
   *
   * @exception IllegalArgumentException if threadPool is null.
   */
  public void setThreadPool(ThreadPool threadPool) {
    if (threadPool == null) {
      throw new IllegalArgumentException("ThreadPool cannot be null.");
    }

    this.threadPool = threadPool;
  }

  /** Get the <code>{@link JobStore}</code> for the <code>{@link QuartzScheduler}</code> to use. */
  public JobStore getJobStore() {
    return jobStore;
  }

  /**
   * Set the <code>{@link JobStore}</code> for the <code>{@link QuartzScheduler}</code> to use.
   *
   * @exception IllegalArgumentException if jobStore is null.
   */
  public void setJobStore(JobStore jobStore) {
    if (jobStore == null) {
      throw new IllegalArgumentException("JobStore cannot be null.");
    }

    this.jobStore = jobStore;
  }

  /**
   * Get the <code>{@link JobRunShellFactory}</code> for the <code>{@link QuartzScheduler}</code> to
   * use.
   */
  public JobRunShellFactory getJobRunShellFactory() {
    return jobRunShellFactory;
  }

  /**
   * Set the <code>{@link JobRunShellFactory}</code> for the <code>{@link QuartzScheduler}</code> to
   * use.
   *
   * @exception IllegalArgumentException if jobRunShellFactory is null.
   */
  public void setJobRunShellFactory(JobRunShellFactory jobRunShellFactory) {
    if (jobRunShellFactory == null) {
      throw new IllegalArgumentException("JobRunShellFactory cannot be null.");
    }

    this.jobRunShellFactory = jobRunShellFactory;
  }

  /**
   * Add the given <code>{@link org.quartz.spi.SchedulerPlugin}</code> for the <code>
   * {@link QuartzScheduler}</code> to use. This method expects the plugin's "initialize" method to
   * be invoked externally (either before or after this method is called).
   */
  public void addSchedulerPlugin(SchedulerPlugin plugin) {
    schedulerPlugins.add(plugin);
  }

  /**
   * Get the <code>List</code> of all <code>{@link org.quartz.spi.SchedulerPlugin}</code>s for the
   * <code>{@link QuartzScheduler}</code> to use.
   */
  public List<SchedulerPlugin> getSchedulerPlugins() {
    return schedulerPlugins;
  }

  /**
   * Get whether to mark the Quartz scheduling thread as daemon.
   *
   * @see Thread#setDaemon(boolean)
   */
  public boolean getMakeSchedulerThreadDaemon() {
    return makeSchedulerThreadDaemon;
  }

  /**
   * Set whether to mark the Quartz scheduling thread as daemon.
   *
   * @see Thread#setDaemon(boolean)
   */
  public void setMakeSchedulerThreadDaemon(boolean makeSchedulerThreadDaemon) {
    this.makeSchedulerThreadDaemon = makeSchedulerThreadDaemon;
  }

  /**
   * Get whether to set the class load context of spawned threads to that of the initializing
   * thread.
   */
  public boolean isThreadsInheritInitializersClassLoadContext() {
    return threadsInheritInitializersClassLoadContext;
  }

  /**
   * Set whether to set the class load context of spawned threads to that of the initializing
   * thread.
   */
  public void setThreadsInheritInitializersClassLoadContext(
      boolean threadsInheritInitializersClassLoadContext) {
    this.threadsInheritInitializersClassLoadContext = threadsInheritInitializersClassLoadContext;
  }

  /** Get the ThreadExecutor which runs the QuartzSchedulerThread */
  public ThreadExecutor getThreadExecutor() {
    return threadExecutor;
  }

  /** Set the ThreadExecutor which runs the QuartzSchedulerThread */
  public void setThreadExecutor(ThreadExecutor threadExecutor) {
    this.threadExecutor = threadExecutor;
  }

  public long getBatchTimeWindow() {
    return batchTimeWindow;
  }

  public void setBatchTimeWindow(long batchTimeWindow) {
    this.batchTimeWindow = batchTimeWindow;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public void setMaxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  public boolean isInterruptJobsOnShutdown() {
    return interruptJobsOnShutdown;
  }

  public void setInterruptJobsOnShutdown(boolean interruptJobsOnShutdown) {
    this.interruptJobsOnShutdown = interruptJobsOnShutdown;
  }

  public boolean isInterruptJobsOnShutdownWithWait() {
    return interruptJobsOnShutdownWithWait;
  }

  public void setInterruptJobsOnShutdownWithWait(boolean interruptJobsOnShutdownWithWait) {
    this.interruptJobsOnShutdownWithWait = interruptJobsOnShutdownWithWait;
  }
}
