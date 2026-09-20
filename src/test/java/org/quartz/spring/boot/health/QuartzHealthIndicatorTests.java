package org.quartz.spring.boot.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

class QuartzHealthIndicatorTests {

  private static final Instant RUNNING_SINCE = Instant.parse("2026-01-01T10:15:30Z");

  @Test
  void startedSchedulerIsUp() throws Exception {
    Health health = health(metaData(true, false, false), 2);

    assertEquals(Status.UP, health.getStatus());
    assertEquals("quartzScheduler", health.getDetails().get("name"));
    assertEquals("instance-1", health.getDetails().get("instanceId"));
    assertEquals(MongoJobStore.class.getName(), health.getDetails().get("jobStore"));
    assertEquals(true, health.getDetails().get("clustered"));
    assertEquals(false, health.getDetails().get("standby"));
    assertEquals(10, health.getDetails().get("threadPoolSize"));
    assertEquals(42, health.getDetails().get("jobsExecuted"));
    assertEquals(2, health.getDetails().get("executingJobs"));
    assertEquals(RUNNING_SINCE, health.getDetails().get("runningSince"));
  }

  @Test
  void standbySchedulerIsOutOfService() throws Exception {
    Health health = health(metaData(true, true, false), 0);

    assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
    assertEquals(true, health.getDetails().get("standby"));
  }

  @Test
  void notYetStartedSchedulerIsOutOfService() throws Exception {
    Health health = health(metaData(false, false, false), 0);

    assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
    assertNull(health.getDetails().get("runningSince"));
  }

  @Test
  void shutDownSchedulerIsDown() throws Exception {
    Health health = health(metaData(false, false, true), 0);

    assertEquals(Status.DOWN, health.getStatus());
  }

  @Test
  void unreadableSchedulerIsDown() throws Exception {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.getMetaData()).thenThrow(new SchedulerException("no scheduler"));

    Health health = new QuartzHealthIndicator(scheduler).health();

    assertEquals(Status.DOWN, health.getStatus());
    assertTrue(
        health.getDetails().get("error").toString().startsWith(SchedulerException.class.getName()));
  }

  private static Health health(SchedulerMetaData metaData, int executingJobs) throws Exception {
    Scheduler scheduler = mock(Scheduler.class);
    when(scheduler.getMetaData()).thenReturn(metaData);
    when(scheduler.getCurrentlyExecutingJobs())
        .thenReturn(Collections.nCopies(executingJobs, mock(JobExecutionContext.class)));
    return new QuartzHealthIndicator(scheduler).health();
  }

  private static SchedulerMetaData metaData(boolean started, boolean standby, boolean shutdown) {
    return new SchedulerMetaData(
        "quartzScheduler",
        "instance-1",
        StdScheduler.class,
        false,
        started,
        standby,
        shutdown,
        started ? RUNNING_SINCE : null,
        42,
        MongoJobStore.class,
        true,
        true,
        SimpleThreadPool.class,
        10);
  }
}
