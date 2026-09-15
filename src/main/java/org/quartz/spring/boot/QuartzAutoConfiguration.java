package org.quartz.spring.boot;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import java.util.Map;
import java.util.Properties;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

/**
 * Auto-configuration for this Quartz fork. JDBC / DataSource wiring from <a
 * href="https://github.com/spring-projects/spring-boot/tree/main/module/spring-boot-quartz">spring-boot-quartz</a>
 * is omitted; MongoDB and RAM stores are configured instead.
 */
@AutoConfiguration(
    afterName = {
      "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration",
      "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration"
    })
@ConditionalOnClass(Scheduler.class)
@ConditionalOnProperty(prefix = "spring.quartz", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(QuartzProperties.class)
public class QuartzAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean({Scheduler.class, QuartzSchedulerFactoryBean.class})
  QuartzSchedulerFactoryBean quartzScheduler(
      QuartzProperties properties,
      ObjectProvider<MongoClient> mongoClients,
      ObjectProvider<QuartzSchedulerCustomizer> customizers,
      ObjectProvider<JobDetail> jobDetails,
      Map<String, Calendar> calendars,
      ObjectProvider<Trigger> triggers,
      ApplicationContext applicationContext,
      Environment environment) {

    MongoClient mongoClient = mongoClients.getIfAvailable();
    JobStoreType storeType = resolveStoreType(properties, mongoClient);

    QuartzSchedulerFactoryBean factoryBean = new QuartzSchedulerFactoryBean();
    factoryBean.setApplicationContext(applicationContext);
    factoryBean.setAutoStartup(properties.isAutoStartup());
    factoryBean.setStartupDelay((int) properties.getStartupDelay().toSeconds());
    factoryBean.setWaitForJobsToCompleteOnShutdown(properties.isWaitForJobsToCompleteOnShutdown());
    factoryBean.setOverwriteExistingJobs(properties.isOverwriteExistingJobs());
    factoryBean.setMongoClient(mongoClient);
    factoryBean.setQuartzProperties(
        buildQuartzProperties(properties, storeType, mongoClient, environment));
    factoryBean.setJobDetails(jobDetails.orderedStream().toArray(JobDetail[]::new));
    factoryBean.setCalendars(calendars);
    factoryBean.setTriggers(triggers.orderedStream().toArray(Trigger[]::new));
    customizers.orderedStream().forEach(customizer -> customizer.customize(factoryBean));
    return factoryBean;
  }

  static JobStoreType resolveStoreType(QuartzProperties properties, MongoClient mongoClient) {
    JobStoreType configured = properties.getJobStoreType();
    if (configured == null || configured == JobStoreType.AUTO) {
      return mongoClient != null ? JobStoreType.MONGODB : JobStoreType.MEMORY;
    }
    return configured;
  }

  static Properties buildQuartzProperties(
      QuartzProperties properties,
      JobStoreType storeType,
      MongoClient mongoClient,
      Environment environment) {
    Properties quartz = new Properties();
    quartz.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, properties.getSchedulerName());
    quartz.setProperty(
        StdSchedulerFactory.PROP_THREAD_POOL_CLASS, SimpleThreadPool.class.getName());
    quartz.setProperty(
        "org.quartz.threadPool.threadCount",
        Integer.toString(properties.getThreadPool().getThreadCount()));
    quartz.setProperty(
        "org.quartz.threadPool.threadPriority",
        Integer.toString(properties.getThreadPool().getThreadPriority()));
    quartz.setProperty("org.quartz.jobStore.misfireThreshold", "60000");

    boolean clustered = storeType == JobStoreType.MONGODB && properties.getMongodb().isClustered();
    String instanceId = properties.getInstanceId();
    if (!StringUtils.hasText(instanceId)) {
      instanceId =
          clustered
              ? StdSchedulerFactory.AUTO_GENERATE_INSTANCE_ID
              : StdSchedulerFactory.DEFAULT_INSTANCE_ID;
    }
    quartz.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID, instanceId);

    if (storeType == JobStoreType.MONGODB) {
      quartz.setProperty(
          StdSchedulerFactory.PROP_JOB_STORE_CLASS, SpringMongoJobStore.class.getName());
      QuartzProperties.Mongodb mongo = properties.getMongodb();
      if (StringUtils.hasText(mongo.getUri()) && mongoClient == null) {
        quartz.setProperty("org.quartz.jobStore.mongoUri", mongo.getUri());
      }
      quartz.setProperty("org.quartz.jobStore.dbName", resolveDatabase(properties, environment));
      quartz.setProperty("org.quartz.jobStore.collectionPrefix", resolveCollectionPrefix(mongo));
      quartz.setProperty("org.quartz.jobStore.isClustered", Boolean.toString(mongo.isClustered()));
      quartz.setProperty(
          "org.quartz.jobStore.clusterCheckinInterval",
          Long.toString(mongo.getClusterCheckinInterval().toMillis()));
    } else {
      quartz.setProperty(StdSchedulerFactory.PROP_JOB_STORE_CLASS, RAMJobStore.class.getName());
    }

    if (properties.getXml().isEnabled()) {
      quartz.setProperty(
          "org.quartz.plugin.jobInitializer.class",
          "org.quartz.plugins.xml.XMLSchedulingDataProcessorPlugin");
      quartz.setProperty(
          "org.quartz.plugin.jobInitializer.fileNames", properties.getXml().getFileNames());
      quartz.setProperty(
          "org.quartz.plugin.jobInitializer.failOnFileNotFound",
          Boolean.toString(properties.getXml().isFailOnFileNotFound()));
      quartz.setProperty(
          "org.quartz.plugin.jobInitializer.scanInterval",
          Long.toString(properties.getXml().getScanInterval().toSeconds()));
    }

    properties.getProperties().forEach(quartz::setProperty);
    return quartz;
  }

  static String resolveCollectionPrefix(QuartzProperties.Mongodb mongo) {
    String prefix = mongo.getCollectionPrefix();
    return StringUtils.hasText(prefix) ? prefix : "qrtz_";
  }

  /**
   * Quartz database if set, otherwise the application's Mongo database ({@code
   * spring.mongodb.database} / {@code spring.data.mongodb.database} or the database in the
   * connection URI).
   */
  static String resolveDatabase(QuartzProperties properties, Environment environment) {
    if (StringUtils.hasText(properties.getMongodb().getDatabase())) {
      return properties.getMongodb().getDatabase();
    }
    if (environment != null) {
      String database =
          firstProperty(environment, "spring.mongodb.database", "spring.data.mongodb.database");
      if (StringUtils.hasText(database)) {
        return database;
      }
      String uri = firstProperty(environment, "spring.mongodb.uri", "spring.data.mongodb.uri");
      if (!StringUtils.hasText(uri) && StringUtils.hasText(properties.getMongodb().getUri())) {
        uri = properties.getMongodb().getUri();
      }
      String fromUri = databaseFromMongoUri(uri);
      if (StringUtils.hasText(fromUri)) {
        return fromUri;
      }
    }
    return "test";
  }

  private static String firstProperty(Environment environment, String... keys) {
    for (String key : keys) {
      String value = environment.getProperty(key);
      if (StringUtils.hasText(value)) {
        return value;
      }
    }
    return null;
  }

  private static String databaseFromMongoUri(String uri) {
    if (!StringUtils.hasText(uri)) {
      return null;
    }
    try {
      return new ConnectionString(uri).getDatabase();
    } catch (RuntimeException ex) {
      return null;
    }
  }
}
