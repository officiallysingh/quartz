package org.quartz.plugins.history;

import java.text.MessageFormat;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.matchers.EverythingMatcher;
import org.quartz.spi.SchedulerPlugin;

/**
 * Logs job executions and vetoes via SLF4J. Message templates use {@link MessageFormat}; time
 * placeholders are ISO-8601 {@link Instant} strings, not {@code java.util.Date}.
 *
 * <p>Slots: {0} job name, {1} job group, {2} now, {3} trigger name, {4} trigger group, {5} previous
 * fire, {6} next fire, {7} refire count. Success and failed also use {8} (result or exception
 * message).
 */
@Slf4j
public class LoggingJobHistoryPlugin implements SchedulerPlugin, JobListener {

  private String name;
  private String jobToBeFiredMessage = "Job {1}.{0} fired (by trigger {4}.{3}) at: {2}";
  private String jobSuccessMessage = "Job {1}.{0} execution complete at {2} and reports: {8}";
  private String jobFailedMessage = "Job {1}.{0} execution failed at {2} and reports: {8}";
  private String jobWasVetoedMessage =
      "Job {1}.{0} was vetoed. It was to be fired (by trigger {4}.{3}) at: {2}";

  public String getJobSuccessMessage() {
    return jobSuccessMessage;
  }

  public void setJobSuccessMessage(String jobSuccessMessage) {
    this.jobSuccessMessage = jobSuccessMessage;
  }

  public String getJobFailedMessage() {
    return jobFailedMessage;
  }

  public void setJobFailedMessage(String jobFailedMessage) {
    this.jobFailedMessage = jobFailedMessage;
  }

  public String getJobToBeFiredMessage() {
    return jobToBeFiredMessage;
  }

  public void setJobToBeFiredMessage(String jobToBeFiredMessage) {
    this.jobToBeFiredMessage = jobToBeFiredMessage;
  }

  public String getJobWasVetoedMessage() {
    return jobWasVetoedMessage;
  }

  public void setJobWasVetoedMessage(String jobWasVetoedMessage) {
    this.jobWasVetoedMessage = jobWasVetoedMessage;
  }

  @Override
  public void initialize(String pluginName, Scheduler scheduler) throws SchedulerException {
    this.name = pluginName;
    scheduler.getListenerManager().addJobListener(this, EverythingMatcher.allJobs());
  }

  @Override
  public void start() {}

  @Override
  public void shutdown() {}

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void jobToBeExecuted(JobExecutionContext context) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(MessageFormat.format(getJobToBeFiredMessage(), jobArgs(context, null)));
  }

  @Override
  public void jobExecutionVetoed(JobExecutionContext context) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(MessageFormat.format(getJobWasVetoedMessage(), jobArgs(context, null)));
  }

  @Override
  public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
    if (jobException != null) {
      if (!log.isWarnEnabled()) {
        return;
      }
      log.warn(
          MessageFormat.format(getJobFailedMessage(), jobArgs(context, jobException.getMessage())),
          jobException);
      return;
    }
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(
        MessageFormat.format(
            getJobSuccessMessage(), jobArgs(context, String.valueOf(context.getResult()))));
  }

  private static Object[] jobArgs(JobExecutionContext context, Object extra) {
    Trigger trigger = context.getTrigger();
    return new Object[] {
      context.getJobDetail().getKey().getName(),
      context.getJobDetail().getKey().getGroup(),
      Instant.now().toString(),
      trigger.getKey().getName(),
      trigger.getKey().getGroup(),
      formatInstant(trigger.getPreviousFireTime()),
      formatInstant(trigger.getNextFireTime()),
      context.getRefireCount(),
      extra
    };
  }

  private static String formatInstant(Instant instant) {
    return instant == null ? null : instant.toString();
  }
}
