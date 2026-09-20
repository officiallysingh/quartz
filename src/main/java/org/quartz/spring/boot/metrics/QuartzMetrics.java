package org.quartz.spring.boot.metrics;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.concurrent.atomic.AtomicLong;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.springframework.util.Assert;

/**
 * {@link MeterBinder} for scheduler-wide Quartz metrics. All meters are tagged with the scheduler
 * name and read in-memory scheduler state only, so scraping them does not query the job store.
 *
 * @see QuartzJobMetrics for per-job execution metrics
 */
public class QuartzMetrics implements MeterBinder {

  private final Scheduler scheduler;

  private final Iterable<Tag> tags;

  /** Last successfully read execution count, so the counter never moves backwards. */
  private final AtomicLong jobsExecuted = new AtomicLong();

  public QuartzMetrics(Scheduler scheduler) {
    this(scheduler, Tags.empty());
  }

  public QuartzMetrics(Scheduler scheduler, Iterable<Tag> tags) {
    Assert.notNull(scheduler, "'scheduler' must not be null");
    Assert.notNull(tags, "'tags' must not be null");
    this.scheduler = scheduler;
    this.tags = tags;
  }

  @Override
  public void bindTo(MeterRegistry registry) {
    Tags meterTags = Tags.concat(this.tags, "scheduler", schedulerName());
    FunctionCounter.builder("quartz.scheduler.jobs.executed", this, QuartzMetrics::jobsExecuted)
        .tags(meterTags)
        .baseUnit("jobs")
        .description("Jobs executed by the scheduler since it was started")
        .register(registry);
    Gauge.builder("quartz.scheduler.jobs.executing", this.scheduler, QuartzMetrics::executingJobs)
        .tags(meterTags)
        .baseUnit("jobs")
        .description("Jobs currently being executed by the scheduler")
        .register(registry);
    Gauge.builder("quartz.scheduler.threads", this.scheduler, QuartzMetrics::threadPoolSize)
        .tags(meterTags)
        .baseUnit("threads")
        .description("Size of the scheduler's thread pool")
        .register(registry);
    Gauge.builder("quartz.scheduler.running", this.scheduler, QuartzMetrics::running)
        .tags(meterTags)
        .description("1 when the scheduler is started and not in standby mode, 0 otherwise")
        .register(registry);
  }

  private String schedulerName() {
    try {
      return this.scheduler.getSchedulerName();
    } catch (SchedulerException ex) {
      throw new IllegalStateException("Could not read the Quartz scheduler name", ex);
    }
  }

  private double jobsExecuted() {
    SchedulerMetaData metaData = metaData(this.scheduler);
    if (metaData != null) {
      this.jobsExecuted.set(metaData.getNumberOfJobsExecuted());
    }
    return this.jobsExecuted.get();
  }

  private static double executingJobs(Scheduler scheduler) {
    try {
      return scheduler.getCurrentlyExecutingJobs().size();
    } catch (SchedulerException ex) {
      return Double.NaN;
    }
  }

  private static double threadPoolSize(Scheduler scheduler) {
    SchedulerMetaData metaData = metaData(scheduler);
    return metaData != null ? metaData.getThreadPoolSize() : Double.NaN;
  }

  private static double running(Scheduler scheduler) {
    SchedulerMetaData metaData = metaData(scheduler);
    if (metaData == null) {
      return Double.NaN;
    }
    boolean running = metaData.isStarted() && !metaData.isInStandbyMode() && !metaData.isShutdown();
    return running ? 1 : 0;
  }

  private static SchedulerMetaData metaData(Scheduler scheduler) {
    try {
      return scheduler.getMetaData();
    } catch (SchedulerException ex) {
      return null;
    }
  }
}
