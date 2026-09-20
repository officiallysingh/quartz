package org.quartz.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.MongoSchedulerSupport;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.listeners.JobListenerSupport;

class JobRunShellFinalizeTest {

  @DisallowConcurrentExecution
  public static class ExclusiveJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {}
  }

  @Test
  void listenerFailureStillClearsBlockedAndAcquired() throws Exception {
    Properties props = MongoSchedulerSupport.schedulerProperties("jobRunShellFinalize", 2);
    props.setProperty("org.quartz.jobStore.isClustered", "false");
    props.setProperty("org.quartz.scheduler.idleWaitTime", "1000");
    Scheduler scheduler = new StdSchedulerFactory(props).getScheduler();
    String dbName = props.getProperty("org.quartz.jobStore.dbName");
    CountDownLatch attempted = new CountDownLatch(1);
    try {
      scheduler
          .getListenerManager()
          .addJobListener(
              new JobListenerSupport() {
                @Override
                public String getName() {
                  return "boom";
                }

                @Override
                public void jobToBeExecuted(JobExecutionContext context) {
                  attempted.countDown();
                  throw new RuntimeException("listener begin failed");
                }
              });
      scheduler.scheduleJob(
          newJob(ExclusiveJob.class).withIdentity("shell-job", "g").build(),
          newTrigger()
              .withIdentity("shell-trig", "g")
              .startAt(Instant.now().minusSeconds(1))
              .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0))
              .build());
      scheduler.start();
      assertTrue(attempted.await(10, TimeUnit.SECONDS));
      Document trigger = awaitUnblockedTrigger(dbName, Duration.ofSeconds(10));
      assertEquals("WAITING", trigger.getString("state"));
      try (MongoClient client =
          MongoClients.create(MongoSchedulerSupport.mongo().getConnectionString())) {
        Document job =
            client
                .getDatabase(dbName)
                .getCollection("qrtz_jobs")
                .find(Filters.eq("name", "shell-job"))
                .first();
        assertFalse(Boolean.TRUE.equals(job.getBoolean("blocked")));
      }
    } finally {
      scheduler.shutdown(true);
    }
  }

  private static Document awaitUnblockedTrigger(String dbName, Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    Document trigger = null;
    while (System.currentTimeMillis() < deadline) {
      try (MongoClient client =
          MongoClients.create(MongoSchedulerSupport.mongo().getConnectionString())) {
        trigger =
            client
                .getDatabase(dbName)
                .getCollection("qrtz_triggers")
                .find(Filters.eq("name", "shell-trig"))
                .first();
      }
      if (trigger != null && "WAITING".equals(trigger.getString("state"))) {
        return trigger;
      }
      Thread.sleep(100L);
    }
    return trigger;
  }
}
