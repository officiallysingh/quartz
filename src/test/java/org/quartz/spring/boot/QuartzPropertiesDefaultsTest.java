package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.springframework.mock.env.MockEnvironment;

class QuartzPropertiesDefaultsTest {

  @Test
  void autoUsesRamWhenNoMongoClient() {
    QuartzProperties properties = new QuartzProperties();
    assertEquals(JobStoreType.MEMORY, QuartzAutoConfiguration.resolveStoreType(properties, null));
  }

  @Test
  void memoryStoreProperties() {
    QuartzProperties properties = new QuartzProperties();
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(properties, JobStoreType.MEMORY, null, null);
    assertEquals(
        RAMJobStore.class.getName(), quartz.getProperty(StdSchedulerFactory.PROP_JOB_STORE_CLASS));
    assertEquals("10", quartz.getProperty("org.quartz.threadPool.threadCount"));
    assertTrue(
        quartz
            .getProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID)
            .equals(StdSchedulerFactory.DEFAULT_INSTANCE_ID));
  }

  @Test
  void collectionPrefixDefaultsToQrtz() {
    QuartzProperties properties = new QuartzProperties();
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(properties, JobStoreType.MONGODB, null, null);
    assertEquals("qrtz_", quartz.getProperty("org.quartz.jobStore.collectionPrefix"));
  }

  @Test
  void collectionPrefixIsConfigurable() {
    QuartzProperties properties = new QuartzProperties();
    properties.getMongodb().setCollectionPrefix("oxneer_");
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(properties, JobStoreType.MONGODB, null, null);
    assertEquals("oxneer_", quartz.getProperty("org.quartz.jobStore.collectionPrefix"));
  }

  @Test
  void quartzDatabaseOverridesApplicationMongo() {
    QuartzProperties properties = new QuartzProperties();
    properties.getMongodb().setDatabase("sched");
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("spring.mongodb.database", "oxneer");
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(
            properties, JobStoreType.MONGODB, null, environment);
    assertEquals("sched", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void databaseFallsBackToApplicationMongo() {
    QuartzProperties properties = new QuartzProperties();
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("spring.mongodb.database", "oxneer");
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(
            properties, JobStoreType.MONGODB, null, environment);
    assertEquals("oxneer", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void databaseFallsBackToLegacySpringDataProperty() {
    QuartzProperties properties = new QuartzProperties();
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("spring.data.mongodb.database", "legacy");
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(
            properties, JobStoreType.MONGODB, null, environment);
    assertEquals("legacy", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void databaseFallsBackToMongoUriPath() {
    QuartzProperties properties = new QuartzProperties();
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("spring.mongodb.uri", "mongodb://localhost:27017/fromuri");
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(
            properties, JobStoreType.MONGODB, null, environment);
    assertEquals("fromuri", quartz.getProperty("org.quartz.jobStore.dbName"));
  }

  @Test
  void memoryStoreUsesSimpleThreadPoolByDefault() {
    QuartzProperties properties = new QuartzProperties();
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(properties, JobStoreType.MEMORY, null, null);
    assertEquals(
        org.quartz.simpl.SimpleThreadPool.class.getName(),
        quartz.getProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS));
  }

  @Test
  void virtualThreadPoolPropertySwitchesClass() {
    QuartzProperties properties = new QuartzProperties();
    properties.getThreadPool().setVirtual(true);
    var quartz =
        QuartzAutoConfiguration.buildQuartzProperties(properties, JobStoreType.MEMORY, null, null);
    assertEquals(
        org.quartz.simpl.VirtualThreadPool.class.getName(),
        quartz.getProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS));
  }
}
