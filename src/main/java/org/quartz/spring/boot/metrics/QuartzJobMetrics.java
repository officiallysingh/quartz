package org.quartz.spring.boot.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.ListenerManager;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.listeners.JobListenerSupport;
import org.quartz.listeners.TriggerListenerSupport;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.util.Assert;

/**
 * {@link MeterBinder} for per-job Quartz metrics, collected through scheduler listeners:
 *
 * <ul>
 *   <li>{@code quartz.job.execution} - timer of completed executions, tagged with {@code outcome}
 *       and {@code exception}.
 *   <li>{@code quartz.job.active} - long task timer of in-flight executions, which reports how long
 *       the jobs running right now have been running.
 *   <li>{@code quartz.job.vetoed} - executions vetoed by a trigger listener.
 *   <li>{@code quartz.trigger.misfires} - misfires detected by the scheduler.
 * </ul>
 *
 * <p>Every meter is tagged with the job (or trigger) name and group. Schedulers with many uniquely
 * named jobs can drop those tags with a {@code MeterFilter}, or disable these meters altogether
 * with {@code management.metrics.enable.quartz.job=false}.
 */
public class QuartzJobMetrics implements MeterBinder {

  static final String EXECUTION = "quartz.job.execution";

  static final String ACTIVE = "quartz.job.active";

  static final String VETOED = "quartz.job.vetoed";

  static final String MISFIRES = "quartz.trigger.misfires";

  static final String JOB_LISTENER_NAME = "quartzJobMetrics";

  static final String TRIGGER_LISTENER_NAME = "quartzTriggerMetrics";

  private final Scheduler scheduler;

  private final Iterable<Tag> tags;

  /** In-flight executions, keyed by fire instance id. */
  private final Map<String, LongTaskTimer.Sample> activeExecutions = new ConcurrentHashMap<>();

  public QuartzJobMetrics(Scheduler scheduler) {
    this(scheduler, Tags.empty());
  }

  public QuartzJobMetrics(Scheduler scheduler, Iterable<Tag> tags) {
    Assert.notNull(scheduler, "'scheduler' must not be null");
    Assert.notNull(tags, "'tags' must not be null");
    this.scheduler = scheduler;
    this.tags = tags;
  }

  /**
   * Registers the listeners that record the metrics. Listeners are named, so binding the same
   * instance again replaces them rather than double counting.
   */
  @Override
  public void bindTo(MeterRegistry registry) {
    try {
      ListenerManager listenerManager = this.scheduler.getListenerManager();
      listenerManager.addJobListener(new JobExecutionMetrics(registry));
      listenerManager.addTriggerListener(new MisfireMetrics(registry));
    } catch (SchedulerException ex) {
      throw new IllegalStateException("Could not register the Quartz metrics listeners", ex);
    }
  }

  private Tags jobTags(JobKey jobKey) {
    return Tags.concat(this.tags, "group", jobKey.getGroup(), "job", jobKey.getName());
  }

  private static Duration executionTime(JobExecutionContext context, LongTaskTimer.Sample sample) {
    long jobRunTime = context.getJobRunTime();
    if (jobRunTime >= 0) {
      return Duration.ofMillis(jobRunTime);
    }
    return sample != null
        ? Duration.ofNanos((long) sample.duration(TimeUnit.NANOSECONDS))
        : Duration.ZERO;
  }

  private static String exceptionTag(JobExecutionException jobException) {
    if (jobException == null) {
      return "none";
    }
    return NestedExceptionUtils.getMostSpecificCause(jobException).getClass().getSimpleName();
  }

  private final class JobExecutionMetrics extends JobListenerSupport {

    private final MeterRegistry registry;

    private JobExecutionMetrics(MeterRegistry registry) {
      this.registry = registry;
    }

    @Override
    public String getName() {
      return JOB_LISTENER_NAME;
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
      LongTaskTimer.Sample sample =
          LongTaskTimer.builder(ACTIVE)
              .tags(jobTags(context.getJobDetail().getKey()))
              .description("Jobs currently being executed")
              .register(this.registry)
              .start();
      activeExecutions.put(context.getFireInstanceId(), sample);
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
      LongTaskTimer.Sample sample = activeExecutions.remove(context.getFireInstanceId());
      Duration executionTime = executionTime(context, sample);
      if (sample != null) {
        sample.stop();
      }
      Timer.builder(EXECUTION)
          .tags(jobTags(context.getJobDetail().getKey()))
          .tag("outcome", jobException != null ? "ERROR" : "SUCCESS")
          .tag("exception", exceptionTag(jobException))
          .description("Job executions completed by the scheduler")
          .register(this.registry)
          .record(executionTime);
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
      Counter.builder(VETOED)
          .tags(jobTags(context.getJobDetail().getKey()))
          .baseUnit("executions")
          .description("Job executions vetoed by a trigger listener")
          .register(this.registry)
          .increment();
    }
  }

  private final class MisfireMetrics extends TriggerListenerSupport {

    private final MeterRegistry registry;

    private MisfireMetrics(MeterRegistry registry) {
      this.registry = registry;
    }

    @Override
    public String getName() {
      return TRIGGER_LISTENER_NAME;
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
      Counter.builder(MISFIRES)
          .tags(
              Tags.concat(
                  tags,
                  "group",
                  trigger.getKey().getGroup(),
                  "trigger",
                  trigger.getKey().getName()))
          .baseUnit("misfires")
          .description("Trigger misfires detected by the scheduler")
          .register(this.registry)
          .increment();
    }
  }
}
