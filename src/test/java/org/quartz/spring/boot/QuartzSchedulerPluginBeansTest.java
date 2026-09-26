package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.Test;
import org.quartz.MongoSchedulerSupport;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.plugins.history.LoggingJobHistoryPlugin;
import org.quartz.plugins.history.LoggingTriggerHistoryPlugin;
import org.quartz.spi.SchedulerPlugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(classes = QuartzSchedulerPluginBeansTest.App.class)
class QuartzSchedulerPluginBeansTest {

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class App {
    @Bean
    MongoClient mongoClient() {
      return MongoClients.create(MongoSchedulerSupport.mongo().getConnectionString());
    }

    @Bean
    LoggingJobHistoryPlugin jobHistory() {
      return new LoggingJobHistoryPlugin();
    }

    @Bean
    LoggingTriggerHistoryPlugin triggerHistory() {
      return new LoggingTriggerHistoryPlugin();
    }

    @Bean
    TrackingPlugin trackingPlugin() {
      return new TrackingPlugin();
    }
  }

  @DynamicPropertySource
  static void mongo(DynamicPropertyRegistry registry) {
    registry.add("spring.mongodb.database", () -> "quartz_plugin_beans_test");
  }

  @Autowired Scheduler scheduler;
  @Autowired LoggingJobHistoryPlugin jobHistory;
  @Autowired LoggingTriggerHistoryPlugin triggerHistory;
  @Autowired TrackingPlugin trackingPlugin;

  @Test
  void pluginBeansAreInitializedByBeanNameAndStarted() throws Exception {
    assertEquals("jobHistory", jobHistory.getName());
    assertEquals("triggerHistory", triggerHistory.getName());
    assertSame(jobHistory, scheduler.getListenerManager().getJobListener("jobHistory"));
    assertSame(triggerHistory, scheduler.getListenerManager().getTriggerListener("triggerHistory"));
    assertEquals("trackingPlugin", trackingPlugin.initializedName);
    assertTrue(trackingPlugin.started);
    assertTrue(scheduler.isStarted());
  }

  static final class TrackingPlugin implements SchedulerPlugin {
    volatile String initializedName;
    volatile boolean started;

    @Override
    public void initialize(String name, Scheduler scheduler) throws SchedulerException {
      initializedName = name;
    }

    @Override
    public void start() {
      started = true;
    }

    @Override
    public void shutdown() {}
  }
}
