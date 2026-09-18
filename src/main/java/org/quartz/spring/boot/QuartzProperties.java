package org.quartz.spring.boot;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/** {@code quartz.scheduler.*} settings for this fork's Spring Boot auto-configuration. */
@Getter
@Setter
@ConfigurationProperties(prefix = "quartz.scheduler")
public class QuartzProperties {

  /** Whether Quartz auto-configuration is enabled. */
  private boolean enabled = true;

  /**
   * Job store. {@link JobStoreType#AUTO} picks MongoDB when a MongoClient bean is present,
   * otherwise RAM.
   */
  private JobStoreType jobStoreType = JobStoreType.AUTO;

  /** Scheduler instance name ({@code org.quartz.scheduler.instanceName}). */
  private String name = "quartzScheduler";

  /** Scheduler instance id. Empty means AUTO when clustered, NON_CLUSTERED otherwise. */
  private String instanceId;

  /** Whether to start the scheduler after the context is ready. */
  private boolean autoStartup = true;

  /** Delay before {@code scheduler.start()} so the rest of the app can finish booting. */
  private Duration startupDelay = Duration.ZERO;

  /** Wait for executing jobs when the context closes. */
  private boolean waitForJobsToCompleteOnShutdown = true;

  /** Replace existing job definitions when registering Spring {@code JobDetail} beans. */
  private boolean overwriteExistingJobs = false;

  /** Extra Quartz keys ({@code org.quartz.*}). Overlayed last. */
  @Setter(AccessLevel.NONE)
  private final Map<String, String> properties = new LinkedHashMap<>();

  @Setter(AccessLevel.NONE)
  @NestedConfigurationProperty
  private final ThreadPool threadPool = new ThreadPool();

  @Setter(AccessLevel.NONE)
  @NestedConfigurationProperty
  private final Mongodb mongodb = new Mongodb();

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

  @Getter
  @Setter
  public static class Mongodb {

    /** Used only when no {@code MongoClient} bean is injected. */
    private String uri;

    /**
     * Mongo database for Quartz collections. When unset, uses the application's {@code
     * spring.mongodb.database} / {@code spring.data.mongodb.database} (or the database in the Mongo
     * URI).
     */
    private String database;

    /** Prefix for Quartz collections ({@code qrtz_jobs}, {@code qrtz_triggers}, …). */
    @Setter(AccessLevel.NONE)
    private String collectionPrefix = "qrtz_";

    /** Multi-JVM clustering (needs a replica set and NTP). */
    private boolean clustered = false;

    private Duration clusterCheckinInterval = Duration.ofSeconds(15);

    public void setCollectionPrefix(String collectionPrefix) {
      this.collectionPrefix =
          (collectionPrefix == null || collectionPrefix.isBlank()) ? "qrtz_" : collectionPrefix;
    }
  }
}
