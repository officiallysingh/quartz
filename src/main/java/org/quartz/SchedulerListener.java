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

package org.quartz;

/**
 * The interface to be implemented by classes that want to be informed of major <code>
 * {@link Scheduler}</code> events.
 *
 * @see Scheduler
 * @see JobListener
 * @see TriggerListener
 * @author James House
 */
public interface SchedulerListener {

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Interface.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link org.quartz.JobDetail}</code>
   * is scheduled.
   */
  void jobScheduled(Trigger trigger);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link org.quartz.JobDetail}</code>
   * is unscheduled.
   *
   * @see SchedulerListener#schedulingDataCleared()
   */
  void jobUnscheduled(TriggerKey triggerKey);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link Trigger}</code> has reached
   * the condition in which it will never fire again.
   */
  void triggerFinalized(Trigger trigger);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link Trigger}</code> has been
   * paused.
   */
  void triggerPaused(TriggerKey triggerKey);

  /**
   * Called by the <code>{@link Scheduler}</code> when a group of <code>{@link Trigger}s</code> has
   * been paused.
   *
   * <p>If all groups were paused then triggerGroup will be null
   *
   * @param triggerGroup the paused group, or null if all were paused
   */
  void triggersPaused(String triggerGroup);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link Trigger}</code> has been
   * un-paused.
   */
  void triggerResumed(TriggerKey triggerKey);

  /**
   * Called by the <code>{@link Scheduler}</code> when a group of <code>{@link Trigger}s</code> has
   * been un-paused.
   */
  void triggersResumed(String triggerGroup);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link org.quartz.JobDetail}</code>
   * has been added.
   */
  void jobAdded(JobDetail jobDetail);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link org.quartz.JobDetail}</code>
   * has been deleted.
   */
  void jobDeleted(JobKey jobKey);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link org.quartz.JobDetail}</code>
   * has been paused.
   */
  void jobPaused(JobKey jobKey);

  /**
   * Called by the <code>{@link Scheduler}</code> when a group of <code>
   * {@link org.quartz.JobDetail}s</code> has been paused.
   *
   * @param jobGroup the paused group, or null if all were paused
   */
  void jobsPaused(String jobGroup);

  /**
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link org.quartz.JobDetail}</code>
   * has been un-paused.
   */
  void jobResumed(JobKey jobKey);

  /**
   * Called by the <code>{@link Scheduler}</code> when a group of <code>
   * {@link org.quartz.JobDetail}s</code> has been un-paused.
   */
  void jobsResumed(String jobGroup);

  /**
   * Called by the <code>{@link Scheduler}</code> when a serious error has occurred within the
   * scheduler - such as repeated failures in the <code>JobStore</code>, or the inability to
   * instantiate a <code>Job</code> instance when its <code>Trigger</code> has fired.
   *
   * <p>The <code>getErrorCode()</code> method of the given SchedulerException can be used to
   * determine more specific information about the type of error that was encountered.
   */
  void schedulerError(String msg, SchedulerException cause);

  /**
   * Called by the <code>{@link Scheduler}</code> to inform the listener that it has move to standby
   * mode.
   */
  void schedulerInStandbyMode();

  /** Called by the <code>{@link Scheduler}</code> to inform the listener that it has started. */
  void schedulerStarted();

  /** Called by the <code>{@link Scheduler}</code> to inform the listener that it is starting. */
  void schedulerStarting();

  /** Called by the <code>{@link Scheduler}</code> to inform the listener that it has shutdown. */
  void schedulerShutdown();

  /**
   * Called by the <code>{@link Scheduler}</code> to inform the listener that it has begun the
   * shutdown sequence.
   */
  void schedulerShuttingdown();

  /**
   * Called by the <code>{@link Scheduler}</code> to inform the listener that all jobs, triggers and
   * calendars were deleted.
   */
  void schedulingDataCleared();
}
