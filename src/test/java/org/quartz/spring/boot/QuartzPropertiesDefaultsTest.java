package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.mock.env.MockEnvironment;

class QuartzPropertiesDefaultsTest {

  @Test
  void mongoStoreIsAlwaysConfigured() {
    QuartzProperties properties = new QuartzProperties();
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("qrtz_", quartz.getProperty("org.quartz.jobStore.collectionPrefix"));
    assertEquals("10", quartz.getProperty("org.quartz.threadPool.threadCount"));
    assertEquals(
        StdSchedulerFactory.AUTO_GENERATE_INSTANCE_ID,
        quartz.getProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID));
    assertEquals("true", quartz.getProperty("org.quartz.jobStore.isClustered"));
    assertEquals("test", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void collectionPrefixDefaultsToQrtz() {
    QuartzProperties properties = new QuartzProperties();
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("qrtz_", quartz.getProperty("org.quartz.jobStore.collectionPrefix"));
  }

  @Test
  void collectionPrefixIsConfigurable() {
    QuartzProperties properties = new QuartzProperties();
    properties.setCollectionPrefix("oxneer_");
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("oxneer_", quartz.getProperty("org.quartz.jobStore.collectionPrefix"));
  }

  @Test
  void databaseUsesApplicationMongo() {
    QuartzProperties properties = new QuartzProperties();
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("spring.mongodb.database", "oxneer");
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, environment);
    assertEquals("oxneer", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void databaseUsesLegacySpringDataProperty() {
    QuartzProperties properties = new QuartzProperties();
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("spring.data.mongodb.database", "legacy");
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, environment);
    assertEquals("legacy", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void databaseUsesMongoUriPath() {
    QuartzProperties properties = new QuartzProperties();
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("spring.mongodb.uri", "mongodb://localhost:27017/fromuri");
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, environment);
    assertEquals("fromuri", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void usesSimpleThreadPoolByDefault() {
    QuartzProperties properties = new QuartzProperties();
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals(
        org.quartz.simpl.SimpleThreadPool.class.getName(),
        quartz.getProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS));
  }

  @Test
  void virtualThreadPoolPropertySwitchesClass() {
    QuartzProperties properties = new QuartzProperties();
    properties.getThreadPool().setVirtual(true);
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals(
        org.quartz.simpl.VirtualThreadPool.class.getName(),
        quartz.getProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS));
  }

  @Test
  void misfireThresholdDefaultsToSixtySeconds() {
    QuartzProperties properties = new QuartzProperties();
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("60000", quartz.getProperty("org.quartz.jobStore.misfireThreshold"));
  }

  @Test
  void misfireThresholdDurationIsWrittenAsMillis() {
    QuartzProperties properties = new QuartzProperties();
    properties.setMisfireThreshold(java.time.Duration.ofSeconds(120));
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("120000", quartz.getProperty("org.quartz.jobStore.misfireThreshold"));
  }

  @Test
  void idleWaitTimeDefaultsToThirtySeconds() {
    QuartzProperties properties = new QuartzProperties();
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("30000", quartz.getProperty(StdSchedulerFactory.PROP_SCHED_IDLE_WAIT_TIME));
  }

  @Test
  void idleWaitTimeDurationIsWrittenAsMillis() {
    QuartzProperties properties = new QuartzProperties();
    properties.setIdleWaitTime(java.time.Duration.ofSeconds(45));
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("45000", quartz.getProperty(StdSchedulerFactory.PROP_SCHED_IDLE_WAIT_TIME));
  }

  @Test
  void batchTimeWindowDefaultsToZero() {
    QuartzProperties properties = new QuartzProperties();
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("0", quartz.getProperty(StdSchedulerFactory.PROP_SCHED_BATCH_TIME_WINDOW));
  }

  @Test
  void typedDurationWinsOverPropertiesOverlay() {
    QuartzProperties properties = new QuartzProperties();
    properties.setMisfireThreshold(java.time.Duration.ofSeconds(120));
    properties.getProperties().put("org.quartz.jobStore.misfireThreshold", "1");
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("120000", quartz.getProperty("org.quartz.jobStore.misfireThreshold"));
  }

  @Test
  void batchTimeWindowDurationIsWrittenAsMillis() {
    QuartzProperties properties = new QuartzProperties();
    properties.setBatchTimeWindow(java.time.Duration.ofMillis(500));
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("500", quartz.getProperty(StdSchedulerFactory.PROP_SCHED_BATCH_TIME_WINDOW));
  }

  @Test
  void clusteredAndCheckinIntervalAreSchedulerLevel() {
    QuartzProperties properties = new QuartzProperties();
    properties.setClustered(true);
    properties.setClusterCheckinInterval(java.time.Duration.ofSeconds(20));
    var quartz = QuartzAutoConfiguration.buildQuartzProperties(properties, null);
    assertEquals("true", quartz.getProperty("org.quartz.jobStore.isClustered"));
    assertEquals("20000", quartz.getProperty("org.quartz.jobStore.clusterCheckinInterval"));
    assertEquals(
        StdSchedulerFactory.AUTO_GENERATE_INSTANCE_ID,
        quartz.getProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID));
  }
}
