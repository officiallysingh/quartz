package org.quartz.spring.boot;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * {@code spring.quartz.*} settings, aligned with Spring Boot's Quartz property names where they
 * still apply to this fork.
 *
 * @see <a
 *     href="https://github.com/spring-projects/spring-boot/tree/main/module/spring-boot-quartz">spring-boot-quartz</a>
 */
@ConfigurationProperties(prefix = "spring.quartz")
public class QuartzProperties {

  /** Whether Quartz auto-configuration is enabled. */
  private boolean enabled = true;

  /**
   * Job store. {@link JobStoreType#AUTO} picks MongoDB when a MongoClient bean is present,
   * otherwise RAM.
   */
  private JobStoreType jobStoreType = JobStoreType.AUTO;

  /** Scheduler instance name ({@code org.quartz.scheduler.instanceName}). */
  private String schedulerName = "quartzScheduler";

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
  private final Map<String, String> properties = new LinkedHashMap<>();

  @NestedConfigurationProperty private final ThreadPool threadPool = new ThreadPool();

  @NestedConfigurationProperty private final Mongodb mongodb = new Mongodb();

  @NestedConfigurationProperty private final Xml xml = new Xml();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public JobStoreType getJobStoreType() {
    return jobStoreType;
  }

  public void setJobStoreType(JobStoreType jobStoreType) {
    this.jobStoreType = jobStoreType;
  }

  public String getSchedulerName() {
    return schedulerName;
  }

  public void setSchedulerName(String schedulerName) {
    this.schedulerName = schedulerName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public boolean isAutoStartup() {
    return autoStartup;
  }

  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  public Duration getStartupDelay() {
    return startupDelay;
  }

  public void setStartupDelay(Duration startupDelay) {
    this.startupDelay = startupDelay;
  }

  public boolean isWaitForJobsToCompleteOnShutdown() {
    return waitForJobsToCompleteOnShutdown;
  }

  public void setWaitForJobsToCompleteOnShutdown(boolean waitForJobsToCompleteOnShutdown) {
    this.waitForJobsToCompleteOnShutdown = waitForJobsToCompleteOnShutdown;
  }

  public boolean isOverwriteExistingJobs() {
    return overwriteExistingJobs;
  }

  public void setOverwriteExistingJobs(boolean overwriteExistingJobs) {
    this.overwriteExistingJobs = overwriteExistingJobs;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public ThreadPool getThreadPool() {
    return threadPool;
  }

  public Mongodb getMongodb() {
    return mongodb;
  }

  public Xml getXml() {
    return xml;
  }

  public static class ThreadPool {

    /** Worker threads. Quartz default is 10. */
    private int threadCount = 10;

    private int threadPriority = Thread.NORM_PRIORITY;

    public int getThreadCount() {
      return threadCount;
    }

    public void setThreadCount(int threadCount) {
      this.threadCount = threadCount;
    }

    public int getThreadPriority() {
      return threadPriority;
    }

    public void setThreadPriority(int threadPriority) {
      this.threadPriority = threadPriority;
    }
  }

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
    private String collectionPrefix = "qrtz_";

    /** Multi-JVM clustering (needs a replica set and NTP). */
    private boolean clustered = false;

    private Duration clusterCheckinInterval = Duration.ofSeconds(15);

    public String getUri() {
      return uri;
    }

    public void setUri(String uri) {
      this.uri = uri;
    }

    public String getDatabase() {
      return database;
    }

    public void setDatabase(String database) {
      this.database = database;
    }

    public String getCollectionPrefix() {
      return collectionPrefix;
    }

    public void setCollectionPrefix(String collectionPrefix) {
      this.collectionPrefix =
          (collectionPrefix == null || collectionPrefix.isBlank()) ? "qrtz_" : collectionPrefix;
    }

    public boolean isClustered() {
      return clustered;
    }

    public void setClustered(boolean clustered) {
      this.clustered = clustered;
    }

    public Duration getClusterCheckinInterval() {
      return clusterCheckinInterval;
    }

    public void setClusterCheckinInterval(Duration clusterCheckinInterval) {
      this.clusterCheckinInterval = clusterCheckinInterval;
    }
  }

  public static class Xml {

    /** Load jobs from XML via {@code XMLSchedulingDataProcessorPlugin}. */
    private boolean enabled = false;

    /** Comma-separated classpath or file paths. */
    private String fileNames = "quartz_data.xml";

    private boolean failOnFileNotFound = true;

    /** Rescan interval; 0 disables scanning. */
    private Duration scanInterval = Duration.ZERO;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public String getFileNames() {
      return fileNames;
    }

    public void setFileNames(String fileNames) {
      this.fileNames = fileNames;
    }

    public boolean isFailOnFileNotFound() {
      return failOnFileNotFound;
    }

    public void setFailOnFileNotFound(boolean failOnFileNotFound) {
      this.failOnFileNotFound = failOnFileNotFound;
    }

    public Duration getScanInterval() {
      return scanInterval;
    }

    public void setScanInterval(Duration scanInterval) {
      this.scanInterval = scanInterval;
    }
  }
}
