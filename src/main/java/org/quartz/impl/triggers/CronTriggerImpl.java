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

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.spi.OperableTrigger;

/**
 * A concrete <code>{@link Trigger}</code> that is used to fire a <code>{@link org.quartz.JobDetail}
 * </code> at given moments in time, defined with Unix 'cron-like' definitions.
 *
 * @author Sharada Jambula, James House
 * @author Contributions from Mads Henderson
 */
@Slf4j
public class CronTriggerImpl extends AbstractTrigger<CronTrigger>
    implements CronTrigger, CoreTrigger {

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
  private static final long serialVersionUID = -8644953146451592766L;

  protected static final int YEAR_TO_GIVEUP_SCHEDULING_AT = CronExpression.MAX_YEAR;

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Data members.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  private CronExpression cronEx = null;
  private Instant startTime = null;
  private Instant endTime = null;
  private Instant nextFireTime = null;
  private Instant previousFireTime = null;
  private transient TimeZone timeZone = null;

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constructors.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /**
   * Create a <code>CronTrigger</code> with no settings.
   *
   * <p>The start-time will also be set to the current time, and the time zone will be set the
   * system's default time zone.
   */
  public CronTriggerImpl() {
    super();
    setStartTime(Instant.now());
    setTimeZone(TimeZone.getDefault());
  }

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Interface.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  @Override
  public Object clone() {
    CronTriggerImpl copy = (CronTriggerImpl) super.clone();
    if (cronEx != null) {
      copy.setCronExpression(new CronExpression(cronEx));
    }
    return copy;
  }

  public void setCronExpression(String cronExpression) throws ParseException {
    TimeZone origTz = getTimeZone();
    this.cronEx = new CronExpression(cronExpression);
    this.cronEx.setTimeZone(origTz);
  }

  /* (non-Javadoc)
   * @see org.quartz.CronTriggerI#getCronExpression()
   */
  public String getCronExpression() {
    return cronEx == null ? null : cronEx.getCronExpression();
  }

  /**
   * Set the CronExpression to the given one. The TimeZone on the passed-in CronExpression
   * over-rides any that was already set on the Trigger.
   */
  public void setCronExpression(CronExpression cronExpression) {
    this.cronEx = cronExpression;
    this.timeZone = cronExpression.getTimeZone();
  }

  /** Get the time at which the <code>CronTrigger</code> should occur. */
  @Override
  public Instant getStartTime() {
    return this.startTime;
  }

  @Override
  public void setStartTime(Instant startTime) {
    if (startTime == null) {
      throw new IllegalArgumentException("Start time cannot be null");
    }
    if (endTime != null && endTime.isBefore(startTime)) {
      throw new IllegalArgumentException("End time cannot be before start time");
    }
    this.startTime = startTime.truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
  }

  /**
   * Get the time at which the <code>CronTrigger</code> should quit repeating - even if repeatCount
   * isn't yet satisfied.
   *
   * @see #getFinalFireTime()
   */
  @Override
  public Instant getEndTime() {
    return this.endTime;
  }

  @Override
  public void setEndTime(Instant endTime) {
    if (startTime != null && endTime != null && startTime.isAfter(endTime)) {
      throw new IllegalArgumentException("End time cannot be before start time");
    }
    this.endTime = endTime;
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
   * @see TriggerUtils#computeFireTimesBetween(OperableTrigger, org.quartz.Calendar, Instant,
   *     Instant)
   */
  @Override
  public Instant getNextFireTime() {
    return this.nextFireTime;
  }

  /**
   * Returns the previous time at which the <code>CronTrigger</code> fired. If the trigger has not
   * yet fired, <code>null</code> will be returned.
   */
  @Override
  public Instant getPreviousFireTime() {
    return this.previousFireTime;
  }

  /**
   * Sets the next time at which the <code>CronTrigger</code> will fire. <b>This method should not
   * be invoked by client code.</b>
   */
  public void setNextFireTime(Instant nextFireTime) {
    this.nextFireTime = nextFireTime;
  }

  /**
   * Set the previous time at which the <code>CronTrigger</code> fired.
   *
   * <p><b>This method should not be invoked by client code.</b>
   */
  public void setPreviousFireTime(Instant previousFireTime) {
    this.previousFireTime = previousFireTime;
  }

  /* (non-Javadoc)
   * @see org.quartz.CronTriggerI#getTimeZone()
   */
  public TimeZone getTimeZone() {

    if (cronEx != null) {
      return cronEx.getTimeZone();
    }

    if (timeZone == null) {
      timeZone = TimeZone.getDefault();
    }
    return timeZone;
  }

  /**
   * Sets the time zone for which the <code>cronExpression</code> of this <code>CronTrigger</code>
   * will be resolved.
   *
   * <p>If {@link #setCronExpression(CronExpression)} is called after this method, the TimeZon
   * setting on the CronExpression will "win". However if {@link #setCronExpression(String)} is
   * called after this method, the time zone applied by this method will remain in effect, since the
   * String cron expression does not carry a time zone!
   */
  public void setTimeZone(TimeZone timeZone) {
    if (cronEx != null) {
      cronEx.setTimeZone(timeZone);
    }
    this.timeZone = timeZone;
  }

  /**
   * Returns the next time at which the <code>CronTrigger</code> will fire, after the given time. If
   * the trigger will not fire after the given time, <code>null</code> will be returned.
   *
   * <p>Note that the date returned is NOT validated against the related org.quartz.Calendar (if
   * any)
   */
  @Override
  public Instant getFireTimeAfter(Instant afterTime) {
    return fireTimeAfter(afterTime);
  }

  public Instant fireTimeAfter(Instant afterTime) {
    if (afterTime == null) {
      afterTime = Instant.now();
    }
    if (startTime.isAfter(afterTime)) {
      afterTime = startTime.minusMillis(1000L);
    }
    if (endTime != null && !afterTime.isBefore(endTime)) {
      return null;
    }
    Instant pot = getTimeAfter(afterTime);
    if (endTime != null && pot != null && pot.isAfter(endTime)) {
      return null;
    }
    return pot;
  }

  /**
   * NOT YET IMPLEMENTED: Returns the final time at which the <code>CronTrigger</code> will fire.
   *
   * <p>Note that the return time *may* be in the past. and the date returned is not validated
   * against org.quartz.calendar
   */
  @Override
  public Instant getFinalFireTime() {
    Instant resultTime;
    if (endTime != null) {
      resultTime = getTimeBefore(endTime.plusMillis(1000L));
    } else {
      resultTime = (cronEx == null) ? null : cronEx.getFinalFireTime();
    }
    if (resultTime != null && startTime != null && resultTime.isBefore(startTime)) {
      return null;
    }
    return resultTime;
  }

  /** Determines whether or not the <code>CronTrigger</code> will occur again. */
  @Override
  public boolean mayFireAgain() {
    return (getNextFireTime() != null);
  }

  @Override
  protected boolean validateMisfireInstruction(int misfireInstruction) {
    return misfireInstruction >= MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY
        && misfireInstruction <= MISFIRE_INSTRUCTION_DO_NOTHING;
  }

  /**
   * Updates the <code>CronTrigger</code>'s state based on the MISFIRE_INSTRUCTION_XXX that was
   * selected when the <code>CronTrigger</code> was created.
   *
   * <p>If the misfire instruction is set to MISFIRE_INSTRUCTION_SMART_POLICY, then the following
   * scheme will be used:
   *
   * <ul>
   *   <li>The instruction will be interpreted as <code>MISFIRE_INSTRUCTION_FIRE_ONCE_NOW</code>
   * </ul>
   */
  @Override
  public void updateAfterMisfire(org.quartz.Calendar cal) {
    int instr = getMisfireInstruction();

    if (instr == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) return;

    if (instr == MISFIRE_INSTRUCTION_SMART_POLICY) {
      instr = MISFIRE_INSTRUCTION_FIRE_ONCE_NOW;
    }

    if (instr == MISFIRE_INSTRUCTION_DO_NOTHING) {
      Instant newFireTime = fireTimeAfter(Instant.now());
      while (newFireTime != null
          && cal != null
          && !cal.isTimeIncluded(newFireTime.toEpochMilli())) {
        newFireTime = fireTimeAfter(newFireTime);
      }
      setNextFireTime(newFireTime);
    } else if (instr == MISFIRE_INSTRUCTION_FIRE_ONCE_NOW) {
      setNextFireTime(Instant.now());
    }
  }

  /**
   * Determines whether the date and (optionally) time of the given Calendar instance falls on a
   * scheduled fire-time of this trigger.
   *
   * <p>Equivalent to calling <code>willFireOn(cal, false)</code>.
   *
   * @param test the date to compare
   * @see #willFireOn(Calendar, boolean)
   */
  public boolean willFireOn(Calendar test) {
    return willFireOn(test, false);
  }

  /**
   * Determines whether the date and (optionally) time of the given Calendar instance falls on a
   * scheduled fire-time of this trigger.
   *
   * <p>Note that the value returned is NOT validated against the related org.quartz.Calendar (if
   * any)
   *
   * @param test the date to compare
   * @param dayOnly if set to true, the method will only determine if the trigger will fire during
   *     the day represented by the given Calendar (hours, minutes and seconds will be ignored).
   * @see #willFireOn(Calendar)
   */
  public boolean willFireOn(Calendar test, boolean dayOnly) {

    test = (Calendar) test.clone();

    test.set(Calendar.MILLISECOND, 0); // don't compare millis.

    if (dayOnly) {
      test.set(Calendar.HOUR_OF_DAY, 0);
      test.set(Calendar.MINUTE, 0);
      test.set(Calendar.SECOND, 0);
    }

    Instant testTime = test.toInstant();

    Instant fta = fireTimeAfter(testTime.minusMillis(1000));

    if (fta == null) return false;

    Calendar p = Calendar.getInstance(test.getTimeZone());
    p.setTimeInMillis(fta.toEpochMilli());

    int year = p.get(Calendar.YEAR);
    int month = p.get(Calendar.MONTH);
    int day = p.get(Calendar.DATE);

    if (dayOnly) {
      return (year == test.get(Calendar.YEAR)
          && month == test.get(Calendar.MONTH)
          && day == test.get(Calendar.DATE));
    }

    while (fta.isBefore(testTime)) {
      fta = fireTimeAfter(fta);
    }

    return fta.equals(testTime);
  }

  /**
   * Called when the <code>{@link Scheduler}</code> has decided to 'fire' the trigger (execute the
   * associated <code>Job</code>), in order to give the <code>Trigger</code> a chance to update
   * itself for its next triggering (if any).
   *
   * @see #executionComplete(JobExecutionContext, JobExecutionException)
   */
  @Override
  public void triggered(org.quartz.Calendar calendar) {
    previousFireTime = nextFireTime;
    nextFireTime = fireTimeAfter(nextFireTime);

    while (nextFireTime != null
        && calendar != null
        && !calendar.isTimeIncluded(nextFireTime.toEpochMilli())) {
      nextFireTime = fireTimeAfter(nextFireTime);
    }
  }

  /**
   * @see AbstractTrigger#updateWithNewCalendar(org.quartz.Calendar, long)
   */
  @Override
  public void updateWithNewCalendar(org.quartz.Calendar calendar, Duration misfireThreshold) {
    nextFireTime = fireTimeAfter(previousFireTime);

    if (nextFireTime == null || calendar == null) {
      return;
    }

    Instant now = Instant.now();
    while (nextFireTime != null && !calendar.isTimeIncluded(nextFireTime.toEpochMilli())) {

      nextFireTime = fireTimeAfter(nextFireTime);

      if (nextFireTime == null) break;

      if (nextFireTime.atZone(java.time.ZoneId.systemDefault()).getYear()
          > YEAR_TO_GIVEUP_SCHEDULING_AT) {
        nextFireTime = null;
      }

      if (nextFireTime != null && nextFireTime.isBefore(now)) {
        Duration threshold = misfireThreshold == null ? Duration.ZERO : misfireThreshold;
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
  public Instant computeFirstFireTime(org.quartz.Calendar calendar) {
    nextFireTime = fireTimeAfter(startTime.minusMillis(1000L));

    while (nextFireTime != null
        && calendar != null
        && !calendar.isTimeIncluded(nextFireTime.toEpochMilli())) {
      nextFireTime = fireTimeAfter(nextFireTime);
    }

    return nextFireTime;
  }

  /* (non-Javadoc)
   * @see org.quartz.CronTriggerI#getExpressionSummary()
   */
  public String getExpressionSummary() {
    return cronEx == null ? null : cronEx.getExpressionSummary();
  }

  /**
   * Used by extensions of CronTrigger to imply that there are additional properties, specifically
   * so that extensions can choose whether to be stored as a serialized blob, or as a flattened
   * CronTrigger table.
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
  public ScheduleBuilder<CronTrigger> getScheduleBuilder() {

    CronScheduleBuilder cb =
        CronScheduleBuilder.cronSchedule(getCronExpression()).inTimeZone(getTimeZone());

    int misfireInstruction = getMisfireInstruction();
    switch (misfireInstruction) {
      case MISFIRE_INSTRUCTION_SMART_POLICY:
        break;
      case MISFIRE_INSTRUCTION_DO_NOTHING:
        cb.withMisfireHandlingInstructionDoNothing();
        break;
      case MISFIRE_INSTRUCTION_FIRE_ONCE_NOW:
        cb.withMisfireHandlingInstructionFireAndProceed();
        break;
      case MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY:
        cb.withMisfireHandlingInstructionIgnoreMisfires();
        break;
      default:
        log.warn(
            "Unrecognized misfire policy {}. Derived builder will use the default cron trigger behavior (MISFIRE_INSTRUCTION_FIRE_ONCE_NOW)",
            misfireInstruction);
    }

    return cb;
  }

  ////////////////////////////////////////////////////////////////////////////
  //
  // Computation Functions
  //
  ////////////////////////////////////////////////////////////////////////////

  protected Instant getTimeAfter(Instant afterTime) {
    return (cronEx == null) ? null : cronEx.getTimeAfter(afterTime);
  }

  /**
   * NOT YET IMPLEMENTED: Returns the time before the given time that this <code>CronTrigger</code>
   * will fire.
   */
  protected Instant getTimeBefore(Instant eTime) {
    return (cronEx == null) ? null : cronEx.getTimeBefore(eTime);
  }
}
