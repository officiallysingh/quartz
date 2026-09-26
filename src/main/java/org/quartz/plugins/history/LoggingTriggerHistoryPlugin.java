package org.quartz.plugins.history;

import java.text.MessageFormat;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerListener;
import org.quartz.impl.matchers.EverythingMatcher;
import org.quartz.spi.SchedulerPlugin;

/**
 * Logs trigger firings, misfires, and completions via SLF4J. Message templates use {@link
 * MessageFormat}; time placeholders are ISO-8601 {@link Instant} strings, not {@code
 * java.util.Date}.
 *
 * <p>Slots: {0} trigger name, {1} trigger group, {2} previous fire, {3} next fire, {4} now, {5} job
 * name, {6} job group, {7} refire count. Complete also uses {8} instruction enum and {9} a
 * human-readable instruction.
 */
@Slf4j
public class LoggingTriggerHistoryPlugin implements SchedulerPlugin, TriggerListener {

  private String name;
  private String triggerFiredMessage = "Trigger {1}.{0} fired job {6}.{5} at: {4}";
  private String triggerMisfiredMessage =
      "Trigger {1}.{0} misfired job {6}.{5} at: {4}. Should have fired at: {3}";
  private String triggerCompleteMessage =
      "Trigger {1}.{0} completed firing job {6}.{5} at {4} with resulting trigger instruction code: {9}";

  public String getTriggerCompleteMessage() {
    return triggerCompleteMessage;
  }

  public void setTriggerCompleteMessage(String triggerCompleteMessage) {
    this.triggerCompleteMessage = triggerCompleteMessage;
  }

  public String getTriggerFiredMessage() {
    return triggerFiredMessage;
  }

  public void setTriggerFiredMessage(String triggerFiredMessage) {
    this.triggerFiredMessage = triggerFiredMessage;
  }

  public String getTriggerMisfiredMessage() {
    return triggerMisfiredMessage;
  }

  public void setTriggerMisfiredMessage(String triggerMisfiredMessage) {
    this.triggerMisfiredMessage = triggerMisfiredMessage;
  }

  @Override
  public void initialize(String pluginName, Scheduler scheduler) throws SchedulerException {
    this.name = pluginName;
    scheduler.getListenerManager().addTriggerListener(this, EverythingMatcher.allTriggers());
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
  public void triggerFired(Trigger trigger, JobExecutionContext context) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(
        MessageFormat.format(
            getTriggerFiredMessage(),
            triggerArgs(
                trigger,
                context.getJobDetail().getKey().getName(),
                context.getJobDetail().getKey().getGroup(),
                context.getRefireCount(),
                null,
                null)));
  }

  @Override
  public void triggerMisfired(Trigger trigger) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(
        MessageFormat.format(
            getTriggerMisfiredMessage(),
            triggerArgs(
                trigger,
                trigger.getJobKey().getName(),
                trigger.getJobKey().getGroup(),
                null,
                null,
                null)));
  }

  @Override
  public void triggerComplete(
      Trigger trigger,
      JobExecutionContext context,
      CompletedExecutionInstruction triggerInstructionCode) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(
        MessageFormat.format(
            getTriggerCompleteMessage(),
            triggerArgs(
                trigger,
                context.getJobDetail().getKey().getName(),
                context.getJobDetail().getKey().getGroup(),
                context.getRefireCount(),
                triggerInstructionCode.toString(),
                instructionLabel(triggerInstructionCode))));
  }

  @Override
  public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
    return false;
  }

  private static String instructionLabel(CompletedExecutionInstruction code) {
    return switch (code) {
      case DELETE_TRIGGER -> "DELETE TRIGGER";
      case NOOP -> "DO NOTHING";
      case RE_EXECUTE_JOB -> "RE-EXECUTE JOB";
      case SET_ALL_JOB_TRIGGERS_COMPLETE -> "SET ALL OF JOB'S TRIGGERS COMPLETE";
      case SET_TRIGGER_COMPLETE -> "SET THIS TRIGGER COMPLETE";
      case SET_TRIGGER_ERROR -> "SET THIS TRIGGER ERROR";
      case SET_ALL_JOB_TRIGGERS_ERROR -> "SET ALL OF JOB'S TRIGGERS ERROR";
    };
  }

  private static Object[] triggerArgs(
      Trigger trigger,
      String jobName,
      String jobGroup,
      Object refireCount,
      Object instruction,
      Object instructionLabel) {
    return new Object[] {
      trigger.getKey().getName(),
      trigger.getKey().getGroup(),
      formatInstant(trigger.getPreviousFireTime()),
      formatInstant(trigger.getNextFireTime()),
      Instant.now().toString(),
      jobName,
      jobGroup,
      refireCount,
      instruction,
      instructionLabel
    };
  }

  private static String formatInstant(Instant instant) {
    return instant == null ? null : instant.toString();
  }
}
