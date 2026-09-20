package org.quartz.spring.boot.health;

import java.time.Instant;
import org.quartz.Scheduler;
import org.quartz.SchedulerMetaData;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.util.Assert;

/**
 * {@link HealthIndicator} reporting the state of a Quartz {@link Scheduler}.
 *
 * <p>The check reads scheduler state only and does not query the job store, so Mongo availability
 * is reported by the Mongo health indicator instead.
 *
 * <ul>
 *   <li>{@link Status#UP} when the scheduler is started and processing triggers.
 *   <li>{@link Status#OUT_OF_SERVICE} when it has not been started yet or is in standby mode, that
 *       is, it is alive but deliberately not firing triggers.
 *   <li>{@link Status#DOWN} when it has been shut down or its state cannot be read.
 * </ul>
 */
public class QuartzHealthIndicator extends AbstractHealthIndicator {

  private final Scheduler scheduler;

  public QuartzHealthIndicator(Scheduler scheduler) {
    super("Quartz health check failed");
    Assert.notNull(scheduler, "'scheduler' must not be null");
    this.scheduler = scheduler;
  }

  @Override
  protected void doHealthCheck(Health.Builder builder) throws Exception {
    SchedulerMetaData metaData = this.scheduler.getMetaData();
    builder
        .status(status(metaData))
        .withDetail("name", metaData.getSchedulerName())
        .withDetail("instanceId", metaData.getSchedulerInstanceId())
        .withDetail("jobStore", metaData.getJobStoreClass().getName())
        .withDetail("clustered", metaData.isJobStoreClustered())
        .withDetail("standby", metaData.isInStandbyMode())
        .withDetail("threadPoolSize", metaData.getThreadPoolSize())
        .withDetail("jobsExecuted", metaData.getNumberOfJobsExecuted())
        .withDetail("executingJobs", this.scheduler.getCurrentlyExecutingJobs().size());
    Instant runningSince = metaData.getRunningSince();
    if (runningSince != null) {
      builder.withDetail("runningSince", runningSince);
    }
  }

  private static Status status(SchedulerMetaData metaData) {
    if (metaData.isShutdown()) {
      return Status.DOWN;
    }
    if (!metaData.isStarted() || metaData.isInStandbyMode()) {
      return Status.OUT_OF_SERVICE;
    }
    return Status.UP;
  }
}
