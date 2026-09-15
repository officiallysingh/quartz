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
import java.util.Calendar;
import java.time.Instant;
import java.util.Date;

import org.quartz.Instants;
import java.util.TimeZone;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * A concrete <code>{@link Trigger}</code> that is used to fire a <code>{@link org.quartz.JobDetail}</code>
 * at given moments in time, defined with Unix 'cron-like' definitions.
 * </p>
 * 
 * 
 * @author Sharada Jambula, James House
 * @author Contributions from Mads Henderson
 */
public class CronTriggerImpl extends AbstractTrigger<CronTrigger> implements CronTrigger, CoreTrigger {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Constants.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * Required for serialization support. Introduced in Quartz 1.6.1 to 
     * maintain compatibility after the introduction of hasAdditionalProperties
     * method. 
     * 
     * @see java.io.Serializable
     */
    private static final long serialVersionUID = -8644953146451592766L;

    private static final Logger LOGGER = LoggerFactory.getLogger(CronTriggerImpl.class);

    protected static final int YEAR_TO_GIVEUP_SCHEDULING_AT = CronExpression.MAX_YEAR;
    
    
    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Data members.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    private CronExpression cronEx = null;
    private Date startTime = null;
    private Date endTime = null;
    private Date nextFireTime = null;
    private Date previousFireTime = null;
    private transient TimeZone timeZone = null;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Constructors.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * <p>
     * Create a <code>CronTrigger</code> with no settings.
     * </p>
     * 
     * <p>
     * The start-time will also be set to the current time, and the time zone
     * will be set the system's default time zone.
     * </p>
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
     * Set the CronExpression to the given one.  The TimeZone on the passed-in
     * CronExpression over-rides any that was already set on the Trigger.
     */
    public void setCronExpression(CronExpression cronExpression) {
        this.cronEx = cronExpression;
        this.timeZone = cronExpression.getTimeZone();
    }
    
    /**
     * <p>
     * Get the time at which the <code>CronTrigger</code> should occur.
     * </p>
     */
    @Override
    public Instant getStartTime() {
        return Instants.fromDate(this.startTime);
    }

    @Override
    public void setStartTime(Instant startTime) {
        if (startTime == null) {
            throw new IllegalArgumentException("Start time cannot be null");
        }

        Date start = Instants.toDate(startTime);
        Date eTime = endTime;
        if (eTime != null && eTime.before(start)) {
            throw new IllegalArgumentException(
                "End time cannot be before start time");
        }
        
        Calendar cl = Calendar.getInstance();
        cl.setTime(start);
        cl.set(Calendar.MILLISECOND, 0);

        this.startTime = cl.getTime();
    }

    /**
     * <p>
     * Get the time at which the <code>CronTrigger</code> should quit
     * repeating - even if repeatCount isn't yet satisfied.
     * </p>
     * 
     * @see #getFinalFireTime()
     */
    @Override
    public Instant getEndTime() {
        return Instants.fromDate(this.endTime);
    }

    @Override
    public void setEndTime(Instant endTime) {
        Date end = Instants.toDate(endTime);
        Date sTime = startTime;
        if (sTime != null && end != null && sTime.after(end)) {
            throw new IllegalArgumentException(
                    "End time cannot be before start time");
        }

        this.endTime = end;
    }

    /**
     * <p>
     * Returns the next time at which the <code>Trigger</code> is scheduled to fire. If
     * the trigger will not fire again, <code>null</code> will be returned.  Note that
     * the time returned can possibly be in the past, if the time that was computed
     * for the trigger to next fire has already arrived, but the scheduler has not yet
     * been able to fire the trigger (which would likely be due to lack of resources
     * e.g. threads).
     * </p>
     *
     * <p>The value returned is not guaranteed to be valid until after the <code>Trigger</code>
     * has been added to the scheduler.
     * </p>
     *
     * @see TriggerUtils#computeFireTimesBetween(OperableTrigger, org.quartz.Calendar, Instant, Instant)
     */
    @Override
    public Instant getNextFireTime() {
        return Instants.fromDate(this.nextFireTime);
    }

    /**
     * <p>
     * Returns the previous time at which the <code>CronTrigger</code> 
     * fired. If the trigger has not yet fired, <code>null</code> will be
     * returned.
     */
    @Override
    public Instant getPreviousFireTime() {
        return Instants.fromDate(this.previousFireTime);
    }

    /**
     * <p>
     * Sets the next time at which the <code>CronTrigger</code> will fire.
     * <b>This method should not be invoked by client code.</b>
     * </p>
     */
    public void setNextFireTime(Instant nextFireTime) {
        this.nextFireTime = Instants.toDate(nextFireTime);
    }

    /**
     * <p>
     * Set the previous time at which the <code>CronTrigger</code> fired.
     * </p>
     * 
     * <p>
     * <b>This method should not be invoked by client code.</b>
     * </p>
     */
    public void setPreviousFireTime(Instant previousFireTime) {
        this.previousFireTime = Instants.toDate(previousFireTime);
    }

    /* (non-Javadoc)
     * @see org.quartz.CronTriggerI#getTimeZone()
     */
    public TimeZone getTimeZone() {
        
        if(cronEx != null) {
            return cronEx.getTimeZone();
        }
        
        if (timeZone == null) {
            timeZone = TimeZone.getDefault();
        }
        return timeZone;
    }

    /**
     * <p>
     * Sets the time zone for which the <code>cronExpression</code> of this
     * <code>CronTrigger</code> will be resolved.
     * </p>
     * 
     * <p>If {@link #setCronExpression(CronExpression)} is called after this
     * method, the TimeZon setting on the CronExpression will "win".  However
     * if {@link #setCronExpression(String)} is called after this method, the
     * time zone applied by this method will remain in effect, since the 
     * String cron expression does not carry a time zone!
     */
    public void setTimeZone(TimeZone timeZone) {
        if(cronEx != null) {
            cronEx.setTimeZone(timeZone);
        }
        this.timeZone = timeZone;
    }

    /**
     * <p>
     * Returns the next time at which the <code>CronTrigger</code> will fire,
     * after the given time. If the trigger will not fire after the given time,
     * <code>null</code> will be returned.
     * </p>
     * 
     * <p>
     * Note that the date returned is NOT validated against the related
     * org.quartz.Calendar (if any)
     * </p>
     */
    @Override
    public Instant getFireTimeAfter(Instant afterTime) {
        return Instants.fromDate(fireTimeAfter(Instants.toDate(afterTime)));
    }

    public Date fireTimeAfter(Date afterTime) {
        if (afterTime == null) {
            afterTime = new Date();
        }

        if (startTime.after(afterTime)) {
            afterTime = new Date(startTime.getTime() - 1000L);
        }

        if (endTime != null && (afterTime.compareTo(endTime) >= 0)) {
            return null;
        }
        
        Date pot = getTimeAfter(afterTime);
        if (endTime != null && pot != null && pot.after(endTime)) {
            return null;
        }

        return pot;
    }

    /**
     * <p>
     * NOT YET IMPLEMENTED: Returns the final time at which the 
     * <code>CronTrigger</code> will fire.
     * </p>
     * 
     * <p>
     * Note that the return time *may* be in the past. and the date returned is
     * not validated against org.quartz.calendar
     * </p>
     */
    @Override
    public Instant getFinalFireTime() {
        Date resultTime;
        if (endTime != null) {
            resultTime = getTimeBefore(new Date(endTime.getTime() + 1000L));
        } else {
            resultTime = (cronEx == null) ? null : cronEx.getFinalFireTime();
        }
        
        if ((resultTime != null) && (startTime != null) && (resultTime.before(startTime))) {
            return null;
        } 
        
        return Instants.fromDate(resultTime);
    }

    /**
     * <p>
     * Determines whether or not the <code>CronTrigger</code> will occur
     * again.
     * </p>
     */
    @Override
    public boolean mayFireAgain() {
        return (getNextFireTime() != null);
    }

    @Override
    protected boolean validateMisfireInstruction(int misfireInstruction) {
        return misfireInstruction >= MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY && misfireInstruction <= MISFIRE_INSTRUCTION_DO_NOTHING;
    }

    /**
     * <p>
     * Updates the <code>CronTrigger</code>'s state based on the
     * MISFIRE_INSTRUCTION_XXX that was selected when the <code>CronTrigger</code>
     * was created.
     * </p>
     * 
     * <p>
     * If the misfire instruction is set to MISFIRE_INSTRUCTION_SMART_POLICY,
     * then the following scheme will be used: </p>
     * <ul>
     * <li>The instruction will be interpreted as <code>MISFIRE_INSTRUCTION_FIRE_ONCE_NOW</code>
     * </ul>
     */
    @Override
    public void updateAfterMisfire(org.quartz.Calendar cal) {
        int instr = getMisfireInstruction();

        if(instr == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY)
            return;

        if (instr == MISFIRE_INSTRUCTION_SMART_POLICY) {
            instr = MISFIRE_INSTRUCTION_FIRE_ONCE_NOW;
        }

        if (instr == MISFIRE_INSTRUCTION_DO_NOTHING) {
            Date newFireTime = fireTimeAfter(new Date());
            while (newFireTime != null && cal != null
                    && !cal.isTimeIncluded(newFireTime.getTime())) {
                newFireTime = fireTimeAfter(newFireTime);
            }
            setNextFireTime(Instants.fromDate(newFireTime));
        } else if (instr == MISFIRE_INSTRUCTION_FIRE_ONCE_NOW) {
            setNextFireTime(Instant.now());
        }
    }

    /**
     * <p>
     * Determines whether the date and (optionally) time of the given Calendar 
     * instance falls on a scheduled fire-time of this trigger.
     * </p>
     * 
     * <p>
     * Equivalent to calling <code>willFireOn(cal, false)</code>.
     * </p>
     * 
     * @param test the date to compare
     * 
     * @see #willFireOn(Calendar, boolean)
     */
    public boolean willFireOn(Calendar test) {
        return willFireOn(test, false);
    }
    
    /**
     * <p>
     * Determines whether the date and (optionally) time of the given Calendar 
     * instance falls on a scheduled fire-time of this trigger.
     * </p>
     * 
     * <p>
     * Note that the value returned is NOT validated against the related
     * org.quartz.Calendar (if any)
     * </p>
     * 
     * @param test the date to compare
     * @param dayOnly if set to true, the method will only determine if the
     * trigger will fire during the day represented by the given Calendar
     * (hours, minutes and seconds will be ignored).
     * @see #willFireOn(Calendar)
     */
    public boolean willFireOn(Calendar test, boolean dayOnly) {

        test = (Calendar) test.clone();
        
        test.set(Calendar.MILLISECOND, 0); // don't compare millis.
        
        if(dayOnly) {
            test.set(Calendar.HOUR_OF_DAY, 0); 
            test.set(Calendar.MINUTE, 0); 
            test.set(Calendar.SECOND, 0); 
        }
        
        Date testTime = test.getTime();
        
        Date fta = fireTimeAfter(new Date(test.getTime().getTime() - 1000));
        
        if(fta == null)
            return false;

        Calendar p = Calendar.getInstance(test.getTimeZone());
        p.setTime(fta);
        
        int year = p.get(Calendar.YEAR);
        int month = p.get(Calendar.MONTH);
        int day = p.get(Calendar.DATE);
        
        if(dayOnly) {
            return (year == test.get(Calendar.YEAR) 
                    && month == test.get(Calendar.MONTH) 
                    && day == test.get(Calendar.DATE));
        }
        
        while(fta.before(testTime)) {
            fta = fireTimeAfter(fta);
        }

        return fta.equals(testTime);
    }

    /**
     * <p>
     * Called when the <code>{@link Scheduler}</code> has decided to 'fire'
     * the trigger (execute the associated <code>Job</code>), in order to
     * give the <code>Trigger</code> a chance to update itself for its next
     * triggering (if any).
     * </p>
     * 
     * @see #executionComplete(JobExecutionContext, JobExecutionException)
     */
    @Override
    public void triggered(org.quartz.Calendar calendar) {
        previousFireTime = nextFireTime;
        nextFireTime = fireTimeAfter(nextFireTime);

        while (nextFireTime != null && calendar != null
                && !calendar.isTimeIncluded(nextFireTime.getTime())) {
            nextFireTime = fireTimeAfter(nextFireTime);
        }
    }

    /**
     *  
     * @see AbstractTrigger#updateWithNewCalendar(org.quartz.Calendar, long)
     */
    @Override
    public void updateWithNewCalendar(org.quartz.Calendar calendar, long misfireThreshold)
    {
        nextFireTime = fireTimeAfter(previousFireTime);
        
        if (nextFireTime == null || calendar == null) {
            return;
        }
        
        Date now = new Date();
        while (nextFireTime != null && !calendar.isTimeIncluded(nextFireTime.getTime())) {

            nextFireTime = fireTimeAfter(nextFireTime);

            if(nextFireTime == null)
                break;
            
            //avoid infinite loop
            // Use gregorian only because the constant is based on Gregorian
            java.util.Calendar c = new java.util.GregorianCalendar(); 
            c.setTime(nextFireTime);
            if (c.get(java.util.Calendar.YEAR) > YEAR_TO_GIVEUP_SCHEDULING_AT) {
                nextFireTime = null;
            }
            
            if(nextFireTime != null && nextFireTime.before(now)) {
                long diff = now.getTime() - nextFireTime.getTime();
                if(diff >= misfireThreshold) {
                    nextFireTime = fireTimeAfter(nextFireTime);
                }
            }
        }
    }

    /**
     * <p>
     * Called by the scheduler at the time a <code>Trigger</code> is first
     * added to the scheduler, in order to have the <code>Trigger</code>
     * compute its first fire time, based on any associated calendar.
     * </p>
     * 
     * <p>
     * After this method has been called, <code>getNextFireTime()</code>
     * should return a valid answer.
     * </p>
     * 
     * @return the first time at which the <code>Trigger</code> will be fired
     *         by the scheduler, which is also the same value <code>getNextFireTime()</code>
     *         will return (until after the first firing of the <code>Trigger</code>).
     */
    @Override
    public Instant computeFirstFireTime(org.quartz.Calendar calendar) {
        nextFireTime = fireTimeAfter(new Date(startTime.getTime() - 1000L));

        while (nextFireTime != null && calendar != null
                && !calendar.isTimeIncluded(nextFireTime.getTime())) {
            nextFireTime = fireTimeAfter(nextFireTime);
        }

        return Instants.fromDate(nextFireTime);
    }

    /* (non-Javadoc)
     * @see org.quartz.CronTriggerI#getExpressionSummary()
     */
    public String getExpressionSummary() {
        return cronEx == null ? null : cronEx.getExpressionSummary();
    }

    /**
     * Used by extensions of CronTrigger to imply that there are additional 
     * properties, specifically so that extensions can choose whether to be 
     * stored as a serialized blob, or as a flattened CronTrigger table. 
     */
    public boolean hasAdditionalProperties() { 
        return false;
    }
    /**
     * Get a {@link ScheduleBuilder} that is configured to produce a 
     * schedule identical to this trigger's schedule.
     * 
     * @see #getTriggerBuilder()
     */
    @Override
    public ScheduleBuilder<CronTrigger> getScheduleBuilder() {
        
        CronScheduleBuilder cb = CronScheduleBuilder.cronSchedule(getCronExpression())
                .inTimeZone(getTimeZone());

        int misfireInstruction = getMisfireInstruction();
        switch(misfireInstruction) {
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
                LOGGER.warn("Unrecognized misfire policy {}. Derived builder will use the default cron trigger behavior (MISFIRE_INSTRUCTION_FIRE_ONCE_NOW)", misfireInstruction);
        }
        
        return cb;
    }
    
    ////////////////////////////////////////////////////////////////////////////
    //
    // Computation Functions
    //
    ////////////////////////////////////////////////////////////////////////////

    protected Date getTimeAfter(Date afterTime) {
        return (cronEx == null) ? null : cronEx.getTimeAfter(afterTime);
    }

    /**
     * NOT YET IMPLEMENTED: Returns the time before the given time
     * that this <code>CronTrigger</code> will fire.
     */ 
    protected Date getTimeBefore(Date eTime) {
        return (cronEx == null) ? null : cronEx.getTimeBefore(eTime);
    }

    
}

