package org.quartz;

import java.util.Properties;
import java.util.UUID;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Shared MongoDB for tests that start a real {@link org.quartz.Scheduler}. */
public final class MongoSchedulerSupport {

  private static final MongoDBContainer MONGO =
      new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

  static {
    MONGO.start();
  }

  private MongoSchedulerSupport() {}

  public static MongoDBContainer mongo() {
    return MONGO;
  }

  public static Properties schedulerProperties(String instanceName, int threadCount) {
    Properties config = new Properties();
    config.setProperty("org.quartz.scheduler.instanceName", instanceName);
    config.setProperty("org.quartz.scheduler.instanceId", "AUTO");
    config.setProperty("org.quartz.threadPool.threadCount", Integer.toString(threadCount));
    config.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    applyJobStore(config);
    return config;
  }

  public static void applyJobStore(Properties config) {
    config.setProperty("org.quartz.jobStore.mongoUri", MONGO.getConnectionString());
    config.setProperty(
        "org.quartz.jobStore.dbName", "qtz_" + UUID.randomUUID().toString().replace("-", ""));
    config.setProperty("org.quartz.jobStore.collectionPrefix", "qrtz_");
    config.setProperty("org.quartz.jobStore.misfireThreshold", "10000");
  }
}
