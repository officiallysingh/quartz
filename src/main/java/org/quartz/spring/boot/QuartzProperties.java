package org.quartz.spring.boot;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.boot.convert.DurationUnit;

/** {@code quartz.scheduler.*} settings for this fork's Spring Boot auto-configuration. */
@Getter
@Setter
@ConfigurationProperties(prefix = "quartz.scheduler")
public class QuartzProperties {

  /** Whether Quartz auto-configuration is enabled. */
  private boolean enabled = true;

  /** Scheduler instance name ({@code org.quartz.scheduler.instanceName}). */
  private String name = "quartzScheduler";

  /** Scheduler instance id. Empty means a fresh lease UUID ({@code AUTO}) on each process start. */
  private String instanceId;

  /** Whether to start the scheduler after the context is ready. */
  private boolean autoStartup = true;

  /** Delay before {@code scheduler.start()} so the rest of the app can finish booting. */
  private Duration startupDelay = Duration.ZERO;

  /** Wait for executing jobs when the context closes. */
  private boolean waitForJobsToCompleteOnShutdown = true;

  /** Replace existing job definitions when registering Spring {@code JobDetail} beans. */
  private boolean overwriteExistingJobs = false;

  /**
   * How late a trigger may fire before it is considered a misfire ({@code
   * org.quartz.jobStore.misfireThreshold}). Unitless numbers are milliseconds.
   */
  @DurationUnit(ChronoUnit.MILLIS)
  private Duration misfireThreshold = Duration.ofSeconds(60);

  /**
   * How long the scheduler thread sleeps when no triggers are ready ({@code
   * org.quartz.scheduler.idleWaitTime}). Must be at least 1s if set. Unitless numbers are
   * milliseconds.
   */
  @DurationUnit(ChronoUnit.MILLIS)
  private Duration idleWaitTime = Duration.ofSeconds(30);

  /**
   * Fire-ahead window when acquiring a batch of triggers ({@code
   * org.quartz.scheduler.batchTriggerAcquisitionFireAheadTimeWindow}). Unitless numbers are
   * milliseconds.
   */
  @DurationUnit(ChronoUnit.MILLIS)
  private Duration batchTimeWindow = Duration.ZERO;

  /**
   * Multi-JVM clustering ({@code org.quartz.jobStore.isClustered}). Shared Mongo is the default;
   * recover by expired lease, not recycled instance names.
   */
  private boolean clustered = true;

  /**
   * Cluster check-in interval ({@code org.quartz.jobStore.clusterCheckinInterval}). Unitless
   * numbers are milliseconds.
   */
  @DurationUnit(ChronoUnit.MILLIS)
  private Duration clusterCheckinInterval = Duration.ofSeconds(15);

  /** Prefix for Quartz collections ({@code qrtz_jobs}, {@code qrtz_triggers}, …). */
  @Setter(AccessLevel.NONE)
  private String collectionPrefix = "qrtz_";

  public void setCollectionPrefix(String collectionPrefix) {
    this.collectionPrefix =
        (collectionPrefix == null || collectionPrefix.isBlank()) ? "qrtz_" : collectionPrefix;
  }

  /** Extra Quartz keys ({@code org.quartz.*}). Applied first; typed Duration fields win. */
  @Setter(AccessLevel.NONE)
  private final Map<String, String> properties = new LinkedHashMap<>();

  @Setter(AccessLevel.NONE)
  @NestedConfigurationProperty
  private final ThreadPool threadPool = new ThreadPool();

  @Getter
  @Setter
  public static class ThreadPool {

    /** Worker threads / max concurrent virtual jobs. Quartz default is 10. */
    private int threadCount = 10;

    private int threadPriority = Thread.NORM_PRIORITY;

    /**
     * When true, use {@link org.quartz.simpl.VirtualThreadPool} instead of {@link
     * org.quartz.simpl.SimpleThreadPool}. {@code threadCount} still caps concurrency.
     */
    private boolean virtual = false;
  }
}
