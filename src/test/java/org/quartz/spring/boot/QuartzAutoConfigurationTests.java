package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = QuartzAutoConfigurationTests.App.class)
class QuartzAutoConfigurationTests {

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class App {}

  @Autowired Scheduler scheduler;

  @Test
  void memorySchedulerStarts() throws Exception {
    assertEquals("quartzScheduler", scheduler.getSchedulerName());
    assertTrue(scheduler.isStarted());
    assertFalse(scheduler.isShutdown());
  }
}
