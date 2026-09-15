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

package org.quartz.spi;

import java.time.Instant;

import org.quartz.Calendar;
import org.quartz.JobDetail;

/**
 * <p>
 * A simple class (structure) used for returning execution-time data from the
 * JobStore to the <code>QuartzSchedulerThread</code>.
 * </p>
 * 
 * @see org.quartz.core.QuartzSchedulerThread
 * 
 * @author James House
 */
public class TriggerFiredBundle implements java.io.Serializable {
  
    private static final long serialVersionUID = -6414106108306999265L;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Data members.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    private final JobDetail job;

    private final OperableTrigger trigger;

    private final Calendar cal;

    private final boolean jobIsRecovering;

    private final Instant fireTime;

    private final Instant scheduledFireTime;

    private final Instant prevFireTime;

    private final Instant nextFireTime;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Constructors.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    public TriggerFiredBundle(JobDetail job, OperableTrigger trigger, Calendar cal,
            boolean jobIsRecovering, Instant fireTime, Instant scheduledFireTime,
            Instant prevFireTime, Instant nextFireTime) {
        this.job = job;
        this.trigger = trigger;
        this.cal = cal;
        this.jobIsRecovering = jobIsRecovering;
        this.fireTime = fireTime;
        this.scheduledFireTime = scheduledFireTime;
        this.prevFireTime = prevFireTime;
        this.nextFireTime = nextFireTime;
    }

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Interface.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    public JobDetail getJobDetail() {
        return job;
    }

    public OperableTrigger getTrigger() {
        return trigger;
    }

    public Calendar getCalendar() {
        return cal;
    }

    public boolean isRecovering() {
        return jobIsRecovering;
    }

    /**
     * @return Returns the fireTime.
     */
    public Instant getFireTime() {
        return fireTime;
    }

    /**
     * @return Returns the nextFireTime.
     */
    public Instant getNextFireTime() {
        return nextFireTime;
    }

    /**
     * @return Returns the prevFireTime.
     */
    public Instant getPrevFireTime() {
        return prevFireTime;
    }

    /**
     * @return Returns the scheduledFireTime.
     */
    public Instant getScheduledFireTime() {
        return scheduledFireTime;
    }

}