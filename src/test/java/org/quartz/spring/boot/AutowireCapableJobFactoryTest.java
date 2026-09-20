package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

class AutowireCapableJobFactoryTest {

  @Test
  void nullJobClassIsSchedulerException() throws Exception {
    JobDetailImpl detail = new JobDetailImpl();
    detail.setName("orphan");
    detail.setGroup("g");
    AutowireCapableJobFactory factory = new AutowireCapableJobFactory();
    SchedulerException thrown =
        assertThrows(SchedulerException.class, () -> factory.newJob(bundle(detail), scheduler()));
    assertTrue(thrown.getMessage().contains("has no job class"));
  }

  @Test
  void springCreatedJobIsDestroyedAfterClose() throws Exception {
    JobDetail jobDetail = JobBuilder.newJob(TrackingJob.class).withIdentity("j").build();
    TrackingJob instance = new TrackingJob();
    AutowireCapableBeanFactory beans = mock(AutowireCapableBeanFactory.class);
    when(beans.createBean(TrackingJob.class)).thenReturn(instance);

    AutowireCapableJobFactory factory = new AutowireCapableJobFactory();
    factory.setBeanFactory(beans);
    Job created = factory.newJob(bundle(jobDetail), scheduler());

    assertInstanceOf(AutoCloseable.class, created);
    assertFalse(created instanceof InterruptableJob);
    created.execute(null);
    ((AutoCloseable) created).close();
    verify(beans).destroyBean(instance);
  }

  @Test
  void interruptableJobsStayInterruptable() throws Exception {
    JobDetail jobDetail =
        JobBuilder.newJob(TrackingInterruptableJob.class).withIdentity("j").build();
    TrackingInterruptableJob instance = new TrackingInterruptableJob();
    AutowireCapableBeanFactory beans = mock(AutowireCapableBeanFactory.class);
    when(beans.createBean(TrackingInterruptableJob.class)).thenReturn(instance);

    AutowireCapableJobFactory factory = new AutowireCapableJobFactory();
    factory.setBeanFactory(beans);
    Job created = factory.newJob(bundle(jobDetail), scheduler());

    InterruptableJob interruptable = assertInstanceOf(InterruptableJob.class, created);
    interruptable.interrupt();
    assertTrue(instance.interrupted);
    ((AutoCloseable) created).close();
    verify(beans).destroyBean(instance);
  }

  @Test
  void simpleJobFactoryRejectsNullJobClass() {
    JobDetailImpl detail = new JobDetailImpl();
    detail.setName("orphan");
    org.quartz.simpl.SimpleJobFactory factory = new org.quartz.simpl.SimpleJobFactory();
    SchedulerException thrown =
        assertThrows(SchedulerException.class, () -> factory.newJob(bundle(detail), scheduler()));
    assertEquals("Job 'DEFAULT.orphan' has no job class", thrown.getMessage());
  }

  private static TriggerFiredBundle bundle(JobDetail job) {
    return new TriggerFiredBundle(
        job,
        new SimpleTriggerImpl("t", "g", Instant.now()),
        null,
        false,
        Instant.now(),
        Instant.now(),
        null,
        null);
  }

  private static Scheduler scheduler() throws SchedulerException {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.getContext()).thenReturn(new SchedulerContext());
    return scheduler;
  }

  public static class TrackingJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {}
  }

  public static class TrackingInterruptableJob implements InterruptableJob {
    boolean interrupted;

    @Override
    public void execute(JobExecutionContext context) {}

    @Override
    public void interrupt() throws UnableToInterruptJobException {
      interrupted = true;
    }
  }
}
