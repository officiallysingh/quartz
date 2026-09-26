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
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.core.ListenerManagerImpl;
import org.slf4j.LoggerFactory;

class LoggingJobHistoryPluginTest {

  private static final Instant PREVIOUS = Instant.parse("2026-09-25T18:30:00Z");
  private static final Instant NEXT = Instant.parse("2026-09-26T18:30:00Z");

  private final LoggingJobHistoryPlugin plugin = new LoggingJobHistoryPlugin();
  private ListAppender<ILoggingEvent> appender;
  private Logger logger;

  @BeforeEach
  void attachAppender() {
    logger = (Logger) LoggerFactory.getLogger(LoggingJobHistoryPlugin.class);
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
  void initializeRegistersJobListenerUnderPluginName() throws Exception {
    Scheduler scheduler = mock(Scheduler.class);
    ListenerManagerImpl listeners = new ListenerManagerImpl();
    when(scheduler.getListenerManager()).thenReturn(listeners);

    plugin.initialize("jobHistory", scheduler);

    assertEquals("jobHistory", plugin.getName());
    assertEquals(plugin, listeners.getJobListener("jobHistory"));
  }

  @Test
  void jobToBeExecutedLogsIso8601Instants() {
    plugin.setJobToBeFiredMessage("Job {1}.{0} prev={5} next={6} at={2}");

    plugin.jobToBeExecuted(context());

    String message = lastMessage();
    assertTrue(message.contains("Job maintenance.cleanup prev=" + PREVIOUS + " next=" + NEXT));
    assertTrue(message.matches(".*at=\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*"));
    assertFalse(message.contains("java.util.Date"));
  }

  @Test
  void jobWasExecutedLogsSuccessWithIso8601Now() {
    JobExecutionContext context = context();
    when(context.getResult()).thenReturn("purged");

    plugin.jobWasExecuted(context, null);

    String message = lastMessage();
    assertTrue(message.contains("Job maintenance.cleanup execution complete at "));
    assertTrue(message.contains(" and reports: purged"));
    assertTrue(message.matches(".*\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*"));
  }

  @Test
  void jobWasExecutedLogsFailureAtWarn() {
    plugin.jobWasExecuted(context(), new JobExecutionException("disk full"));

    ILoggingEvent event = appender.list.getLast();
    assertEquals(Level.WARN, event.getLevel());
    assertTrue(event.getFormattedMessage().contains("execution failed"));
    assertTrue(event.getFormattedMessage().contains("disk full"));
  }

  @Test
  void jobExecutionVetoedLogsIso8601Now() {
    plugin.jobExecutionVetoed(context());

    String message = lastMessage();
    assertTrue(message.contains("Job maintenance.cleanup was vetoed"));
    assertTrue(message.matches(".*\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*"));
  }

  private String lastMessage() {
    assertFalse(appender.list.isEmpty());
    return appender.list.getLast().getFormattedMessage();
  }

  private static JobExecutionContext context() {
    JobDetail job = JobBuilder.newJob(NoOpJob.class).withIdentity("cleanup", "maintenance").build();
    Trigger trigger = mock(Trigger.class);
    when(trigger.getKey()).thenReturn(TriggerKey.triggerKey("cleanup-midnight", "maintenance"));
    when(trigger.getPreviousFireTime()).thenReturn(PREVIOUS);
    when(trigger.getNextFireTime()).thenReturn(NEXT);

    JobExecutionContext context = mock(JobExecutionContext.class);
    when(context.getJobDetail()).thenReturn(job);
    when(context.getTrigger()).thenReturn(trigger);
    when(context.getRefireCount()).thenReturn(0);
    return context;
  }

  public static class NoOpJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {}
  }
}
