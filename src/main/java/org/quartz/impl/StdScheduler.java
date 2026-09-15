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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.ListenerManager;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.UnableToInterruptJobException;
import org.quartz.core.QuartzScheduler;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.JobFactory;

/**
 * An implementation of the <code>Scheduler</code> interface that directly proxies all method calls
 * to the equivalent call on a given <code>QuartzScheduler</code> instance.
 *
 * @see org.quartz.Scheduler
 * @see org.quartz.core.QuartzScheduler
 * @author James House
 */
public class StdScheduler implements Scheduler {

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Data members.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  private final QuartzScheduler sched;

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constructors.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /**
   * Construct a <code>StdScheduler</code> instance to proxy the given <code>QuartzScheduler</code>
   * instance, and with the given <code>SchedulingContext</code>.
   */
  public StdScheduler(QuartzScheduler sched) {
    this.sched = sched;
  }

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Interface.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /** Returns the name of the <code>Scheduler</code>. */
  public String getSchedulerName() {
    return sched.getSchedulerName();
  }

  /** Returns the instance Id of the <code>Scheduler</code>. */
  public String getSchedulerInstanceId() {
    return sched.getSchedulerInstanceId();
  }

  public SchedulerMetaData getMetaData() {
    return new SchedulerMetaData(
        getSchedulerName(),
        getSchedulerInstanceId(),
        getClass(),
        false,
        isStarted(),
        isInStandbyMode(),
        isShutdown(),
        sched.runningSince(),
        sched.numJobsExecuted(),
        sched.getJobStoreClass(),
        sched.supportsPersistence(),
        sched.isClustered(),
        sched.getThreadPoolClass(),
        sched.getThreadPoolSize(),
        sched.getVersion());
  }

  /** Returns the <code>SchedulerContext</code> of the <code>Scheduler</code>. */
  public SchedulerContext getContext() throws SchedulerException {
    return sched.getSchedulerContext();
  }

  ///////////////////////////////////////////////////////////////////////////
  ///
  /// Scheduler State Management Methods
  ///
  ///////////////////////////////////////////////////////////////////////////

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void start() throws SchedulerException {
    sched.start();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void startDelayed(int seconds) throws SchedulerException {
    sched.startDelayed(seconds);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void standby() {
    sched.standby();
  }

  /**
   * Whether the scheduler has been started.
   *
   * <p>Note: This only reflects whether <code>{@link #start()}</code> has ever been called on this
   * Scheduler, so it will return <code>true</code> even if the <code>Scheduler</code> is currently
   * in standby mode or has been since shutdown.
   *
   * @see #start()
   * @see #isShutdown()
   * @see #isInStandbyMode()
   */
  public boolean isStarted() {
    return (sched.runningSince() != null);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public boolean isInStandbyMode() {
    return sched.isInStandbyMode();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void shutdown() {
    sched.shutdown();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void shutdown(boolean waitForJobsToComplete) {
    sched.shutdown(waitForJobsToComplete);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public boolean isShutdown() {
    return sched.isShutdown();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public List<JobExecutionContext> getCurrentlyExecutingJobs() {
    return sched.getCurrentlyExecutingJobs();
  }

  ///////////////////////////////////////////////////////////////////////////
  ///
  /// Scheduling-related Methods
  ///
  ///////////////////////////////////////////////////////////////////////////

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void clear() throws SchedulerException {
    sched.clear();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public Instant scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
    return sched.scheduleJob(jobDetail, trigger);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public Instant scheduleJob(Trigger trigger) throws SchedulerException {
    return sched.scheduleJob(trigger);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void addJob(JobDetail jobDetail, boolean replace) throws SchedulerException {
    sched.addJob(jobDetail, replace);
  }

  public void addJob(
      JobDetail jobDetail, boolean replace, boolean storeNonDurableWhileAwaitingScheduling)
      throws SchedulerException {
    sched.addJob(jobDetail, replace, storeNonDurableWhileAwaitingScheduling);
  }

  public boolean deleteJobs(List<JobKey> jobKeys) throws SchedulerException {
    return sched.deleteJobs(jobKeys);
  }

  public void scheduleJobs(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
      throws SchedulerException {
    sched.scheduleJobs(triggersAndJobs, replace);
  }

  public void scheduleJob(
      JobDetail jobDetail, Set<? extends Trigger> triggersForJob, boolean replace)
      throws SchedulerException {
    sched.scheduleJob(jobDetail, triggersForJob, replace);
  }

  public boolean unscheduleJobs(List<TriggerKey> triggerKeys) throws SchedulerException {
    return sched.unscheduleJobs(triggerKeys);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public boolean deleteJob(JobKey jobKey) throws SchedulerException {
    return sched.deleteJob(jobKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public boolean unscheduleJob(TriggerKey triggerKey) throws SchedulerException {
    return sched.unscheduleJob(triggerKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public Instant rescheduleJob(TriggerKey triggerKey, Trigger newTrigger)
      throws SchedulerException {
    return sched.rescheduleJob(triggerKey, newTrigger);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void triggerJob(JobKey jobKey) throws SchedulerException {
    triggerJob(jobKey, null);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void triggerJob(JobKey jobKey, JobDataMap data) throws SchedulerException {
    sched.triggerJob(jobKey, data);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void pauseTrigger(TriggerKey triggerKey) throws SchedulerException {
    sched.pauseTrigger(triggerKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void pauseTriggers(GroupMatcher<TriggerKey> matcher) throws SchedulerException {
    sched.pauseTriggers(matcher);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void pauseJob(JobKey jobKey) throws SchedulerException {
    sched.pauseJob(jobKey);
  }

  /**
   * @see org.quartz.Scheduler#getPausedTriggerGroups()
   */
  public Set<String> getPausedTriggerGroups() throws SchedulerException {
    return sched.getPausedTriggerGroups();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void pauseJobs(GroupMatcher<JobKey> matcher) throws SchedulerException {
    sched.pauseJobs(matcher);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void resumeTrigger(TriggerKey triggerKey) throws SchedulerException {
    sched.resumeTrigger(triggerKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void resumeTriggers(GroupMatcher<TriggerKey> matcher) throws SchedulerException {
    sched.resumeTriggers(matcher);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void resumeJob(JobKey jobKey) throws SchedulerException {
    sched.resumeJob(jobKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void resumeJobs(GroupMatcher<JobKey> matcher) throws SchedulerException {
    sched.resumeJobs(matcher);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void pauseAll() throws SchedulerException {
    sched.pauseAll();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void resumeAll() throws SchedulerException {
    sched.resumeAll();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public List<String> getJobGroupNames() throws SchedulerException {
    return sched.getJobGroupNames();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public List<? extends Trigger> getTriggersOfJob(JobKey jobKey) throws SchedulerException {
    return sched.getTriggersOfJob(jobKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws SchedulerException {
    return sched.getJobKeys(matcher);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public List<String> getTriggerGroupNames() throws SchedulerException {
    return sched.getTriggerGroupNames();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
      throws SchedulerException {
    return sched.getTriggerKeys(matcher);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public JobDetail getJobDetail(JobKey jobKey) throws SchedulerException {
    return sched.getJobDetail(jobKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public List<JobDetail> getJobDetails(GroupMatcher<JobKey> matcher) throws SchedulerException {
    return sched.getJobDetails(matcher);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public Trigger getTrigger(TriggerKey triggerKey) throws SchedulerException {
    return sched.getTrigger(triggerKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public TriggerState getTriggerState(TriggerKey triggerKey) throws SchedulerException {
    return sched.getTriggerState(triggerKey);
  }

  /**
   * Reset the current state of the identified <code>{@link Trigger}</code> from {@link
   * TriggerState#ERROR} to {@link TriggerState#NORMAL} or {@link TriggerState#PAUSED} as
   * appropriate.
   *
   * <p>Only affects triggers that are in ERROR state - if identified trigger is not in that state
   * then the result is a no-op.
   *
   * <p>The result will be the trigger returning to the normal, waiting to be fired state, unless
   * the trigger's group has been paused, in which case it will go into the PAUSED state.
   *
   * @see Trigger.TriggerState
   */
  public void resetTriggerFromErrorState(TriggerKey triggerKey) throws SchedulerException {
    sched.resetTriggerFromErrorState(triggerKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public void addCalendar(
      String calName, Calendar calendar, boolean replace, boolean updateTriggers)
      throws SchedulerException {
    sched.addCalendar(calName, calendar, replace, updateTriggers);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public boolean deleteCalendar(String calName) throws SchedulerException {
    return sched.deleteCalendar(calName);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public Calendar getCalendar(String calName) throws SchedulerException {
    return sched.getCalendar(calName);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public List<String> getCalendarNames() throws SchedulerException {
    return sched.getCalendarNames();
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public boolean checkExists(JobKey jobKey) throws SchedulerException {
    return sched.checkExists(jobKey);
  }

  /** Calls the equivalent method on the 'proxied' <code>QuartzScheduler</code>. */
  public boolean checkExists(TriggerKey triggerKey) throws SchedulerException {
    return sched.checkExists(triggerKey);
  }

  ///////////////////////////////////////////////////////////////////////////
  ///
  /// Other Methods
  ///
  ///////////////////////////////////////////////////////////////////////////

  /**
   * @see org.quartz.Scheduler#setJobFactory(org.quartz.spi.JobFactory)
   */
  public void setJobFactory(JobFactory factory) throws SchedulerException {
    sched.setJobFactory(factory);
  }

  /**
   * @see org.quartz.Scheduler#getListenerManager()
   */
  public ListenerManager getListenerManager() throws SchedulerException {
    return sched.getListenerManager();
  }

  public boolean interrupt(JobKey jobKey) throws UnableToInterruptJobException {
    return sched.interrupt(jobKey);
  }

  public boolean interrupt(String fireInstanceId) throws UnableToInterruptJobException {
    return sched.interrupt(fireInstanceId);
  }
}
