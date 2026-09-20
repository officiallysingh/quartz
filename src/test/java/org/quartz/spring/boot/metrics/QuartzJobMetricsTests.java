package org.quartz.spring.boot.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.TriggerListener;
import org.quartz.core.ListenerManagerImpl;

class QuartzJobMetricsTests {

  private final MeterRegistry registry = new SimpleMeterRegistry();

  private final ListenerManagerImpl listenerManager = new ListenerManagerImpl();

  private Scheduler scheduler;

  @BeforeEach
  void bindMetrics() throws Exception {
    this.scheduler = mock(Scheduler.class);
    when(this.scheduler.getListenerManager()).thenReturn(this.listenerManager);
    new QuartzJobMetrics(this.scheduler).bindTo(this.registry);
  }

  @Test
  void bindingRegistersJobAndTriggerListeners() {
    assertEquals(1, this.listenerManager.getJobListeners().size());
    assertEquals(1, this.listenerManager.getTriggerListeners().size());
  }

  @Test
  void bindingAgainReplacesListenersInsteadOfDoubleCounting() {
    new QuartzJobMetrics(this.scheduler).bindTo(this.registry);

    assertEquals(1, this.listenerManager.getJobListeners().size());
    assertEquals(1, this.listenerManager.getTriggerListeners().size());
  }

  @Test
  void runningJobIsActiveUntilItCompletes() {
    JobExecutionContext context = context(250);

    jobListener().jobToBeExecuted(context);

    assertEquals(1, this.registry.get(QuartzJobMetrics.ACTIVE).longTaskTimer().activeTasks());

    jobListener().jobWasExecuted(context, null);

    assertEquals(0, this.registry.get(QuartzJobMetrics.ACTIVE).longTaskTimer().activeTasks());
  }

  @Test
  void successfulExecutionIsTimed() {
    JobExecutionContext context = context(250);

    jobListener().jobToBeExecuted(context);
    jobListener().jobWasExecuted(context, null);

    Timer timer =
        this.registry
            .get(QuartzJobMetrics.EXECUTION)
            .tags("group", "reports", "job", "nightly", "outcome", "SUCCESS", "exception", "none")
            .timer();
    assertEquals(1, timer.count());
    assertEquals(250, timer.totalTime(MILLISECONDS));
  }

  @Test
  void failedExecutionIsTaggedWithTheMostSpecificCause() {
    JobExecutionContext context = context(10);
    JobExecutionException jobException =
        new JobExecutionException(new IllegalStateException("boom"));

    jobListener().jobToBeExecuted(context);
    jobListener().jobWasExecuted(context, jobException);

    assertEquals(
        1,
        this.registry
            .get(QuartzJobMetrics.EXECUTION)
            .tags("outcome", "ERROR", "exception", "IllegalStateException")
            .timer()
            .count());
  }

  @Test
  void vetoedExecutionIsCounted() {
    jobListener().jobExecutionVetoed(context(0));

    assertEquals(
        1,
        this.registry
            .get(QuartzJobMetrics.VETOED)
            .tags("group", "reports", "job", "nightly")
            .counter()
            .count());
  }

  @Test
  void misfiredTriggerIsCounted() {
    Trigger trigger = mock(Trigger.class);
    when(trigger.getKey()).thenReturn(TriggerKey.triggerKey("nightly-trigger", "reports"));

    triggerListener().triggerMisfired(trigger);

    assertEquals(
        1,
        this.registry
            .get(QuartzJobMetrics.MISFIRES)
            .tags("group", "reports", "trigger", "nightly-trigger")
            .counter()
            .count());
  }

  private JobListener jobListener() {
    return this.listenerManager.getJobListener(QuartzJobMetrics.JOB_LISTENER_NAME);
  }

  private TriggerListener triggerListener() {
    return this.listenerManager.getTriggerListener(QuartzJobMetrics.TRIGGER_LISTENER_NAME);
  }

  private static JobExecutionContext context(long jobRunTime) {
    JobDetail jobDetail = mock(JobDetail.class);
    when(jobDetail.getKey()).thenReturn(JobKey.jobKey("nightly", "reports"));
    JobExecutionContext context = mock(JobExecutionContext.class);
    when(context.getJobDetail()).thenReturn(jobDetail);
    when(context.getFireInstanceId()).thenReturn("instance-1-1700000000000");
    when(context.getJobRunTime()).thenReturn(jobRunTime);
    return context;
  }
}
