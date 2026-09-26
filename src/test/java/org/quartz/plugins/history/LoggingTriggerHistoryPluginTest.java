package org.quartz.plugins.history;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerKey;
import org.quartz.core.ListenerManagerImpl;
import org.slf4j.LoggerFactory;

class LoggingTriggerHistoryPluginTest {

  private static final Instant PREVIOUS = Instant.parse("2026-09-25T18:30:00Z");
  private static final Instant NEXT = Instant.parse("2026-09-26T18:30:00Z");

  private final LoggingTriggerHistoryPlugin plugin = new LoggingTriggerHistoryPlugin();
  private ListAppender<ILoggingEvent> appender;
  private Logger logger;

  @BeforeEach
  void attachAppender() {
    logger = (Logger) LoggerFactory.getLogger(LoggingTriggerHistoryPlugin.class);
    appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
  }

  @AfterEach
  void detachAppender() {
    logger.detachAppender(appender);
  }

  @Test
  void initializeRegistersTriggerListenerUnderPluginName() throws Exception {
    Scheduler scheduler = mock(Scheduler.class);
    ListenerManagerImpl listeners = new ListenerManagerImpl();
    when(scheduler.getListenerManager()).thenReturn(listeners);

    plugin.initialize("triggerHistory", scheduler);

    assertEquals("triggerHistory", plugin.getName());
    assertEquals(plugin, listeners.getTriggerListener("triggerHistory"));
  }

  @Test
  void triggerFiredLogsIso8601Instants() {
    plugin.setTriggerFiredMessage("Trigger {1}.{0} job={6}.{5} prev={2} next={3} at={4}");

    plugin.triggerFired(trigger(), context());

    String message = lastMessage();
    assertTrue(
        message.contains(
            "Trigger maintenance.cleanup-midnight job=maintenance.cleanup prev="
                + PREVIOUS
                + " next="
                + NEXT));
    assertTrue(message.matches(".*at=\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*"));
    assertFalse(message.contains("java.util.Date"));
  }

  @Test
  void triggerMisfiredLogsIso8601NextFire() {
    plugin.triggerMisfired(trigger());

    String message = lastMessage();
    assertTrue(message.contains("Trigger maintenance.cleanup-midnight misfired job"));
    assertTrue(message.contains("Should have fired at: " + NEXT));
  }

  @Test
  void triggerCompleteLogsHumanReadableInstruction() {
    plugin.triggerComplete(trigger(), context(), CompletedExecutionInstruction.NOOP);

    String message = lastMessage();
    assertTrue(message.contains("completed firing job maintenance.cleanup"));
    assertTrue(message.contains("DO NOTHING"));
    assertTrue(message.matches(".*\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*"));
  }

  @Test
  void triggerCompleteIncludesErrorInstruction() {
    plugin.triggerComplete(trigger(), context(), CompletedExecutionInstruction.SET_TRIGGER_ERROR);

    assertTrue(lastMessage().contains("SET THIS TRIGGER ERROR"));
  }

  private String lastMessage() {
    assertFalse(appender.list.isEmpty());
    return appender.list.getLast().getFormattedMessage();
  }

  private static Trigger trigger() {
    Trigger trigger = mock(Trigger.class);
    when(trigger.getKey()).thenReturn(TriggerKey.triggerKey("cleanup-midnight", "maintenance"));
    when(trigger.getJobKey()).thenReturn(JobKey.jobKey("cleanup", "maintenance"));
    when(trigger.getPreviousFireTime()).thenReturn(PREVIOUS);
    when(trigger.getNextFireTime()).thenReturn(NEXT);
    return trigger;
  }

  private static JobExecutionContext context() {
    JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("cleanup", "maintenance").build();
    JobExecutionContext context = mock(JobExecutionContext.class);
    when(context.getJobDetail()).thenReturn(job);
    when(context.getRefireCount()).thenReturn(0);
    return context;
  }

  public static class NoOpJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {}
  }
}
