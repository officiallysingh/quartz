package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.Test;
import org.quartz.MongoSchedulerSupport;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(classes = QuartzAutoConfigurationTests.App.class)
class QuartzAutoConfigurationTests {

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class App {
    @Bean
    MongoClient mongoClient() {
      return MongoClients.create(MongoSchedulerSupport.mongo().getConnectionString());
    }
  }

  @DynamicPropertySource
  static void mongo(DynamicPropertyRegistry registry) {
    registry.add("spring.mongodb.database", () -> "quartz_boot_test");
  }

  @Autowired Scheduler scheduler;

  @Test
  void mongoSchedulerStarts() throws Exception {
    assertEquals("quartzScheduler", scheduler.getSchedulerName());
    assertTrue(scheduler.isStarted());
    assertFalse(scheduler.isShutdown());
  }
}
