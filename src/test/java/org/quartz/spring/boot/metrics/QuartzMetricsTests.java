package org.quartz.spring.boot.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.quartz.impl.StdScheduler;
import org.quartz.impl.mongodb.MongoJobStore;
import org.quartz.simpl.SimpleThreadPool;

class QuartzMetricsTests {

  private final MeterRegistry registry = new SimpleMeterRegistry();

  @Test
  void schedulerMetersAreTaggedAndPopulated() throws Exception {
    Scheduler scheduler = scheduler(metaData(true, false), 3);

    new QuartzMetrics(scheduler).bindTo(this.registry);

    assertEquals(42, this.registry.get("quartz.scheduler.jobs.executed").functionCounter().count());
    assertEquals(3, this.registry.get("quartz.scheduler.jobs.executing").gauge().value());
    assertEquals(10, this.registry.get("quartz.scheduler.threads").gauge().value());
    assertEquals(1, this.registry.get("quartz.scheduler.running").gauge().value());
    assertEquals(
        "quartzScheduler",
        this.registry.get("quartz.scheduler.threads").gauge().getId().getTag("scheduler"));
  }

  @Test
  void standbySchedulerIsNotRunning() throws Exception {
    Scheduler scheduler = scheduler(metaData(true, true), 0);

    new QuartzMetrics(scheduler).bindTo(this.registry);

    assertEquals(0, this.registry.get("quartz.scheduler.running").gauge().value());
  }

  @Test
  void unreadableSchedulerDoesNotBreakMeters() throws Exception {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.getSchedulerName()).thenReturn("quartzScheduler");
    when(scheduler.getMetaData())
        .thenReturn(metaData(true, false))
        .thenThrow(new SchedulerException("shut down"));
    when(scheduler.getCurrentlyExecutingJobs()).thenThrow(new SchedulerException("shut down"));

    new QuartzMetrics(scheduler).bindTo(this.registry);

    assertEquals(42, this.registry.get("quartz.scheduler.jobs.executed").functionCounter().count());
    assertEquals(42, this.registry.get("quartz.scheduler.jobs.executed").functionCounter().count());
    assertTrue(Double.isNaN(this.registry.get("quartz.scheduler.threads").gauge().value()));
    assertTrue(Double.isNaN(this.registry.get("quartz.scheduler.jobs.executing").gauge().value()));
  }

  private static Scheduler scheduler(SchedulerMetaData metaData, int executingJobs)
      throws SchedulerException {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.getSchedulerName()).thenReturn(metaData.getSchedulerName());
    when(scheduler.getMetaData()).thenReturn(metaData);
    when(scheduler.getCurrentlyExecutingJobs())
        .thenReturn(Collections.nCopies(executingJobs, mock(JobExecutionContext.class)));
    return scheduler;
  }

  private static SchedulerMetaData metaData(boolean started, boolean standby) {
    return new SchedulerMetaData(
        "quartzScheduler",
        "instance-1",
        StdScheduler.class,
        false,
        started,
        standby,
        false,
        Instant.parse("2026-01-01T10:15:30Z"),
        42,
        MongoJobStore.class,
        true,
        true,
        SimpleThreadPool.class,
        10);
  }
}
