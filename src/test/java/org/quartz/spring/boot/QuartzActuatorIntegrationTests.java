package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.MongoSchedulerSupport;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.integrations.tests.HelloJob;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint.QuartzJobDetailsDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.health.actuate.endpoint.HealthDescriptor;
import org.springframework.boot.health.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Health, metrics and the {@code quartz} endpoint against a real Mongo-backed scheduler. */
@SpringBootTest(
    classes = QuartzActuatorIntegrationTests.App.class,
    properties = {
      "quartz.scheduler.name=actuatorScheduler",
      "quartz.scheduler.clustered=false",
      "management.endpoints.web.exposure.include=health,quartz"
    })
class QuartzActuatorIntegrationTests {

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class App {

    @Bean
    MongoClient mongoClient() {
      return MongoClients.create(MongoSchedulerSupport.mongo().getConnectionString());
    }

    @Bean
    JobDetail helloJob() {
      return JobBuilder.newJob(HelloJob.class)
          .withIdentity("hello", "reports")
          .storeDurably()
          .build();
    }

    @Bean
    Trigger helloTrigger() {
      return TriggerBuilder.newTrigger()
          .withIdentity("hello-trigger", "reports")
          .forJob(helloJob())
          .startAt(Instant.now().plus(Duration.ofHours(1)))
          .withSchedule(SimpleScheduleBuilder.repeatHourlyForever())
          .build();
    }
  }

  @DynamicPropertySource
  static void mongo(DynamicPropertyRegistry registry) {
    registry.add("spring.mongodb.database", () -> "quartz_actuator_test");
  }

  @Autowired Scheduler scheduler;

  @Autowired HealthEndpoint healthEndpoint;

  @Autowired QuartzEndpoint quartzEndpoint;

  @Autowired MeterRegistry meterRegistry;

  @Test
  void healthEndpointReportsTheRunningScheduler() {
    HealthDescriptor health = this.healthEndpoint.healthForPath("quartz");

    assertEquals(Status.UP.getCode(), health.getStatus().getCode());
  }

  @Test
  void quartzEndpointReportsTheRegisteredJobAndTrigger() throws Exception {
    assertTrue(this.quartzEndpoint.quartzReport().getJobs().getGroups().contains("reports"));

    QuartzJobDetailsDescriptor job = this.quartzEndpoint.quartzJob("reports", "hello", false);

    assertEquals(HelloJob.class.getName(), job.getClassName());
    assertEquals("hello-trigger", job.getTriggers().stream().findFirst().orElseThrow().get("name"));
  }

  @Test
  void schedulerMetersAreBoundBySpringBoot() {
    assertEquals(
        10,
        this.meterRegistry
            .get("quartz.scheduler.threads")
            .tag("scheduler", "actuatorScheduler")
            .gauge()
            .value());
    assertEquals(1, this.meterRegistry.get("quartz.scheduler.running").gauge().value());
  }

  @Test
  void executingAJobRecordsJobMeters() throws Exception {
    this.quartzEndpoint.triggerQuartzJob("reports", "hello");

    long executions = awaitJobExecutions();

    assertEquals(1, executions);
  }

  private long awaitJobExecutions() throws InterruptedException {
    Instant deadline = Instant.now().plusSeconds(10);
    while (Instant.now().isBefore(deadline)) {
      var timer =
          this.meterRegistry
              .find("quartz.job.execution")
              .tags("group", "reports", "job", "hello")
              .timer();
      if (timer != null && timer.count() > 0) {
        return timer.count();
      }
      Thread.sleep(100);
    }
    return 0;
  }
}
