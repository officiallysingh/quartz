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

package org.quartz.impl.triggers;

import java.time.Duration;
import java.time.Instant;
import java.time.Year;
import java.time.ZoneId;
import org.quartz.Calendar;
import org.quartz.CronTrigger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.spi.OperableTrigger;

/**
 * A concrete <code>{@link Trigger}</code> that is used to fire a <code>{@link org.quartz.JobDetail}
 * </code> at a given moment in time, and optionally repeated at a specified interval.
 *
 * @see Trigger
 * @see CronTrigger
 * @see TriggerUtils
 * @author James House
 * @author contributions by Lieven Govaerts of Ebitec Nv, Belgium.
 */
public class SimpleTriggerImpl extends AbstractTrigger<SimpleTrigger>
    implements SimpleTrigger, CoreTrigger {

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constants.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /**
   * Required for serialization support. Introduced in Quartz 1.6.1 to maintain compatibility after
   * the introduction of hasAdditionalProperties method.
   *
   * @see java.io.Serializable
   */
  private static final long serialVersionUID = -3735980074222850397L;

  private static final int YEAR_TO_GIVEUP_SCHEDULING_AT = Year.now().getValue() + 100;

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Data members.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  private Instant startTime = null;

  private Instant endTime = null;

  private Instant nextFireTime = null;

  private Instant previousFireTime = null;

  private int repeatCount = 0;

  private Duration repeatInterval = Duration.ZERO;

  private int timesTriggered = 0;

  private final boolean complete = false;

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constructors.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /**
   * Create a <code>SimpleTrigger</code> with no settings. Prefer {@link org.quartz.TriggerBuilder}.
   */
  public SimpleTriggerImpl() {
    super();
  }

  /** Create a <code>SimpleTrigger</code> that will occur at the given time, and not repeat. */
  public SimpleTriggerImpl(String name, String group, Instant startTime) {
    this(name, group, startTime, null, 0, Duration.ZERO);
  }

  /**
   * Create a <code>SimpleTrigger</code> that will occur at the given time, and repeat at the given
   * interval the given number of times, or until the given end time.
   */
  public SimpleTriggerImpl(
      String name,
      String group,
      Instant startTime,
      Instant endTime,
      int repeatCount,
      Duration repeatInterval) {
    super(name, group);
    setStartTime(startTime);
    setEndTime(endTime);
    setRepeatCount(repeatCount);
    setRepeatInterval(repeatInterval);
  }

  /**
   * Create a <code>SimpleTrigger</code> that will occur at the given time, fire the identified
   * <code>Job</code> and repeat at the given interval the given number of times, or until the given
   * end time.
   */
  public SimpleTriggerImpl(
      String name,
      String group,
      String jobName,
      String jobGroup,
      Instant startTime,
      Instant endTime,
      int repeatCount,
      Duration repeatInterval) {
    super(name, group, jobName, jobGroup);
    setStartTime(startTime);
    setEndTime(endTime);
    setRepeatCount(repeatCount);
    setRepeatInterval(repeatInterval);
  }

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Interface.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /** Get the time at which the <code>SimpleTrigger</code> should occur. */
  @Override
  public Instant getStartTime() {
    return startTime;
  }

  /**
   * Set the time at which the <code>SimpleTrigger</code> should occur.
   *
   * @exception IllegalArgumentException if startTime is <code>null</code>.
   */
  @Override
  public void setStartTime(Instant startTime) {
    if (startTime == null) {
      throw new IllegalArgumentException("Start time cannot be null");
    }
    if (endTime != null && endTime.isBefore(startTime)) {
      throw new IllegalArgumentException("End time cannot be before start time");
    }
    this.startTime = startTime;
  }

  /**
   * Get the time at which the <code>SimpleTrigger</code> should quit repeating - even if
   * repeatCount isn't yet satisfied.
   *
   * @see #getFinalFireTime()
   */
  @Override
  public Instant getEndTime() {
    return endTime;
  }

  /**
   * Set the time at which the <code>SimpleTrigger</code> should quit repeating (and be
   * automatically deleted).
   *
   * @exception IllegalArgumentException if endTime is before start time.
   */
  @Override
  public void setEndTime(Instant endTime) {
    if (startTime != null && endTime != null && startTime.isAfter(endTime)) {
      throw new IllegalArgumentException("End time cannot be before start time");
    }
    this.endTime = endTime;
  }

  /* (non-Javadoc)
   * @see org.quartz.SimpleTriggerI#getRepeatCount()
   */
  public int getRepeatCount() {
    return repeatCount;
  }

  /**
   * Set the number of time the <code>SimpleTrigger</code> should repeat, after which it will be
   * automatically deleted.
   *
   * @see #REPEAT_INDEFINITELY
   * @exception IllegalArgumentException if repeatCount is &lt; 0
   */
  public void setRepeatCount(int repeatCount) {
    if (repeatCount < 0 && repeatCount != REPEAT_INDEFINITELY) {
      throw new IllegalArgumentException(
          "Repeat count must be >= 0, use the " + "constant REPEAT_INDEFINITELY for infinite.");
    }

    this.repeatCount = repeatCount;
  }

  /* (non-Javadoc)
   * @see org.quartz.SimpleTriggerI#getRepeatInterval()
   */
  public Duration getRepeatInterval() {
    return repeatInterval;
  }

  /**
   * Set the time interval at which the <code>SimpleTrigger</code> should repeat.
   *
   * @exception IllegalArgumentException if repeatInterval is <code>null</code> or negative
   */
  public void setRepeatInterval(Duration repeatInterval) {
    if (repeatInterval == null || repeatInterval.isNegative()) {
      throw new IllegalArgumentException("Repeat interval must be >= 0");
    }

    this.repeatInterval = repeatInterval;
  }

  /** Get the number of times the <code>SimpleTrigger</code> has already fired. */
  public int getTimesTriggered() {
    return timesTriggered;
  }

  /** Set the number of times the <code>SimpleTrigger</code> has already fired. */
  public void setTimesTriggered(int timesTriggered) {
    this.timesTriggered = timesTriggered;
  }

  @Override
  protected boolean validateMisfireInstruction(int misfireInstruction) {
    if (misfireInstruction < MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
      return false;
    }

    return misfireInstruction <= MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_EXISTING_COUNT;
  }

  /**
   * Updates the <code>SimpleTrigger</code>'s state based on the MISFIRE_INSTRUCTION_XXX that was
   * selected when the <code>SimpleTrigger</code> was created.
   *
   * <p>If the misfire instruction is set to MISFIRE_INSTRUCTION_SMART_POLICY, then the following
   * scheme will be used:
   *
   * <ul>
   *   <li>If the Repeat Count is <code>0</code>, then the instruction will be interpreted as <code>
   *       MISFIRE_INSTRUCTION_FIRE_NOW</code>.
   *   <li>If the Repeat Count is <code>REPEAT_INDEFINITELY</code>, then the instruction will be
   *       interpreted as <code>MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT</code>.
   *       <b>WARNING:</b> using MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT with a
   *       trigger that has a non-null end-time may cause the trigger to never fire again if the
   *       end-time arrived during the misfire time span.
   *   <li>If the Repeat Count is <code>&gt; 0</code>, then the instruction will be interpreted as
   *       <code>MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT</code>.
   * </ul>
   */
  @Override
  public void updateAfterMisfire(Calendar cal) {
    int instr = getMisfireInstruction();

    if (instr == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) return;

    if (instr == Trigger.MISFIRE_INSTRUCTION_SMART_POLICY) {
      if (getRepeatCount() == 0) {
        instr = MISFIRE_INSTRUCTION_FIRE_NOW;
      } else if (getRepeatCount() == REPEAT_INDEFINITELY) {
        instr = MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT;
      } else {
        // if (getRepeatCount() > 0)
        instr = MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT;
      }
    } else if (instr == MISFIRE_INSTRUCTION_FIRE_NOW && getRepeatCount() != 0) {
      instr = MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT;
    }

    if (instr == MISFIRE_INSTRUCTION_FIRE_NOW) {
      setNextFireTime(Instant.now());
    } else if (instr == MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_EXISTING_COUNT) {
      Instant newFireTime = skipExcluded(fireTimeAfter(Instant.now()), cal);
      setNextFireTime(newFireTime);
    } else if (instr == MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT) {
      Instant newFireTime = skipExcluded(fireTimeAfter(Instant.now()), cal);
      if (newFireTime != null) {
        setTimesTriggered(
            getTimesTriggered() + computeNumTimesFiredBetween(nextFireTime, newFireTime));
      }
      setNextFireTime(newFireTime);
    } else if (instr == MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT) {
      Instant newFireTime = Instant.now();
      if (repeatCount != 0 && repeatCount != REPEAT_INDEFINITELY) {
        setRepeatCount(getRepeatCount() - getTimesTriggered());
        setTimesTriggered(0);
      }
      if (endTime != null && endTime.isBefore(newFireTime)) {
        setNextFireTime(null);
      } else {
        setStartTime(newFireTime);
        setNextFireTime(newFireTime);
      }
    } else if (instr == MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT) {
      Instant newFireTime = Instant.now();
      int timesMissed = computeNumTimesFiredBetween(nextFireTime, newFireTime);
      if (repeatCount != 0 && repeatCount != REPEAT_INDEFINITELY) {
        int remainingCount = getRepeatCount() - (getTimesTriggered() + timesMissed);
        setRepeatCount(Math.max(remainingCount, 0));
        setTimesTriggered(0);
      }
      if (endTime != null && endTime.isBefore(newFireTime)) {
        setNextFireTime(null);
      } else {
        setStartTime(newFireTime);
        setNextFireTime(newFireTime);
      }
    }
  }

  /**
   * Called when the <code>{@link Scheduler}</code> has decided to 'fire' the trigger (execute the
   * associated <code>Job</code>), in order to give the <code>Trigger</code> a chance to update
   * itself for its next triggering (if any).
   *
   * @see #executionComplete(JobExecutionContext, JobExecutionException)
   */
  @Override
  public void triggered(Calendar calendar) {
    timesTriggered++;
    previousFireTime = nextFireTime;
    nextFireTime = skipExcluded(fireTimeAfter(nextFireTime), calendar);
  }

  /**
   * @see org.quartz.impl.triggers.AbstractTrigger#updateWithNewCalendar(org.quartz.Calendar, long)
   */
  @Override
  public void updateWithNewCalendar(Calendar calendar, Duration misfireThreshold) {
    nextFireTime = fireTimeAfter(previousFireTime);
    if (nextFireTime == null || calendar == null) {
      return;
    }
    Instant now = Instant.now();
    Duration threshold = misfireThreshold == null ? Duration.ZERO : misfireThreshold;
    while (nextFireTime != null && !calendar.isTimeIncluded(nextFireTime.toEpochMilli())) {
      nextFireTime = fireTimeAfter(nextFireTime);
      if (pastGiveUpYear(nextFireTime)) {
        nextFireTime = null;
        break;
      }
      if (nextFireTime != null && nextFireTime.isBefore(now)) {
        if (Duration.between(nextFireTime, now).compareTo(threshold) >= 0) {
          nextFireTime = fireTimeAfter(nextFireTime);
        }
      }
    }
  }

  /**
   * Called by the scheduler at the time a <code>Trigger</code> is first added to the scheduler, in
   * order to have the <code>Trigger</code> compute its first fire time, based on any associated
   * calendar.
   *
   * <p>After this method has been called, <code>getNextFireTime()</code> should return a valid
   * answer.
   *
   * @return the first time at which the <code>Trigger</code> will be fired by the scheduler, which
   *     is also the same value <code>getNextFireTime()</code> will return (until after the first
   *     firing of the <code>Trigger</code>).
   */
  @Override
  public Instant computeFirstFireTime(Calendar calendar) {
    nextFireTime = skipExcluded(startTime, calendar);
    return nextFireTime;
  }

  /**
   * Returns the next time at which the <code>Trigger</code> is scheduled to fire. If the trigger
   * will not fire again, <code>null</code> will be returned. Note that the time returned can
   * possibly be in the past, if the time that was computed for the trigger to next fire has already
   * arrived, but the scheduler has not yet been able to fire the trigger (which would likely be due
   * to lack of resources e.g. threads).
   *
   * <p>The value returned is not guaranteed to be valid until after the <code>Trigger</code> has
   * been added to the scheduler.
   *
   * @see TriggerUtils#computeFireTimesBetween(OperableTrigger, Calendar, Instant, Instant)
   */
  @Override
  public Instant getNextFireTime() {
    return nextFireTime;
  }

  /**
   * Returns the previous time at which the <code>SimpleTrigger</code> fired. If the trigger has not
   * yet fired, <code>null</code> will be returned.
   */
  @Override
  public Instant getPreviousFireTime() {
    return previousFireTime;
  }

  /**
   * Set the next time at which the <code>SimpleTrigger</code> should fire.
   *
   * <p><b>This method should not be invoked by client code.</b>
   */
  public void setNextFireTime(Instant nextFireTime) {
    this.nextFireTime = nextFireTime;
  }

  /**
   * Set the previous time at which the <code>SimpleTrigger</code> fired.
   *
   * <p><b>This method should not be invoked by client code.</b>
   */
  public void setPreviousFireTime(Instant previousFireTime) {
    this.previousFireTime = previousFireTime;
  }

  /**
   * Returns the next time at which the <code>SimpleTrigger</code> will fire, after the given time.
   * If the trigger will not fire after the given time, <code>null</code> will be returned.
   */
  @Override
  public Instant getFireTimeAfter(Instant afterTime) {
    return fireTimeAfter(afterTime);
  }

  public Instant fireTimeAfter(Instant afterTime) {
    if (complete) {
      return null;
    }
    if ((timesTriggered > repeatCount) && (repeatCount != REPEAT_INDEFINITELY)) {
      return null;
    }
    if (afterTime == null) {
      afterTime = Instant.now();
    }
    if (repeatCount == 0 && !afterTime.isBefore(startTime)) {
      return null;
    }
    long startMillis = startTime.toEpochMilli();
    long afterMillis = afterTime.toEpochMilli();
    long endMillis = (endTime == null) ? Long.MAX_VALUE : endTime.toEpochMilli();
    long intervalMillis = repeatInterval.toMillis();
    if (endMillis <= afterMillis) {
      return null;
    }
    if (afterMillis < startMillis) {
      return startTime;
    }
    long numberOfTimesExecuted = ((afterMillis - startMillis) / intervalMillis) + 1;
    if ((numberOfTimesExecuted > repeatCount) && (repeatCount != REPEAT_INDEFINITELY)) {
      return null;
    }
    Instant time = Instant.ofEpochMilli(startMillis + (numberOfTimesExecuted * intervalMillis));
    if (endMillis <= time.toEpochMilli()) {
      return null;
    }
    return time;
  }

  /**
   * Returns the last time at which the <code>SimpleTrigger</code> will fire, before the given time.
   * If the trigger will not fire before the given time, <code>null</code> will be returned.
   */
  public Instant getFireTimeBefore(Instant end) {
    if (end.toEpochMilli() < startTime.toEpochMilli()) {
      return null;
    }
    int numFires = computeNumTimesFiredBetween(startTime, end);
    return Instant.ofEpochMilli(startTime.toEpochMilli() + (numFires * repeatInterval.toMillis()));
  }

  public int computeNumTimesFiredBetween(Instant start, Instant end) {
    long intervalMillis = repeatInterval.toMillis();
    if (intervalMillis < 1 || start == null || end == null) {
      return 0;
    }
    return (int) ((end.toEpochMilli() - start.toEpochMilli()) / intervalMillis);
  }

  /**
   * Returns the final time at which the <code>SimpleTrigger</code> will fire, if repeatCount is
   * REPEAT_INDEFINITELY, null will be returned.
   *
   * <p>Note that the return time may be in the past.
   */
  @Override
  public Instant getFinalFireTime() {
    if (repeatCount == 0) {
      return startTime;
    }
    if (repeatCount == REPEAT_INDEFINITELY) {
      return endTime == null ? null : getFireTimeBefore(endTime);
    }
    Instant lastTrigger =
        Instant.ofEpochMilli(startTime.toEpochMilli() + (repeatCount * repeatInterval.toMillis()));
    if (endTime == null || lastTrigger.toEpochMilli() < endTime.toEpochMilli()) {
      return lastTrigger;
    }
    return getFireTimeBefore(endTime);
  }

  private Instant skipExcluded(Instant candidate, Calendar calendar) {
    Instant time = candidate;
    while (time != null && calendar != null && !calendar.isTimeIncluded(time.toEpochMilli())) {
      time = fireTimeAfter(time);
      if (pastGiveUpYear(time)) {
        return null;
      }
    }
    return pastGiveUpYear(time) ? null : time;
  }

  private static boolean pastGiveUpYear(Instant time) {
    return time != null
        && time.atZone(ZoneId.systemDefault()).getYear() > YEAR_TO_GIVEUP_SCHEDULING_AT;
  }

  /** Determines whether or not the <code>SimpleTrigger</code> will occur again. */
  @Override
  public boolean mayFireAgain() {
    return (getNextFireTime() != null);
  }

  /**
   * Validates whether the properties of the <code>JobDetail</code> are valid for submission into a
   * <code>Scheduler</code>.
   *
   * @throws IllegalStateException if a required property (such as Name, Group, Class) is not set.
   */
  @Override
  public void validate() throws SchedulerException {
    super.validate();

    if (repeatCount != 0 && repeatInterval.toMillis() < 1) {
      throw new SchedulerException("Repeat Interval cannot be zero.");
    }
  }

  /**
   * Used by extensions of SimpleTrigger to imply that there are additional properties, specifically
   * so that extensions can choose whether to be stored as a serialized blob, or as a flattened
   * SimpleTrigger table.
   */
  public boolean hasAdditionalProperties() {
    return false;
  }

  /**
   * Get a {@link ScheduleBuilder} that is configured to produce a schedule identical to this
   * trigger's schedule.
   *
   * @see #getTriggerBuilder()
   */
  @Override
  public ScheduleBuilder<SimpleTrigger> getScheduleBuilder() {

    SimpleScheduleBuilder sb =
        SimpleScheduleBuilder.simpleSchedule()
            .withInterval(getRepeatInterval())
            .withRepeatCount(getRepeatCount());

    switch (getMisfireInstruction()) {
      case MISFIRE_INSTRUCTION_FIRE_NOW:
        sb.withMisfireHandlingInstructionFireNow();
        break;
      case MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_EXISTING_COUNT:
        sb.withMisfireHandlingInstructionNextWithExistingCount();
        break;
      case MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT:
        sb.withMisfireHandlingInstructionNextWithRemainingCount();
        break;
      case MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT:
        sb.withMisfireHandlingInstructionNowWithExistingCount();
        break;
      case MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT:
        sb.withMisfireHandlingInstructionNowWithRemainingCount();
        break;
    }

    return sb;
  }
}
