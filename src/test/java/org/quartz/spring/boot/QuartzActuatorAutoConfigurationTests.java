package org.quartz.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.quartz.core.ListenerManagerImpl;
import org.quartz.impl.StdScheduler;
import org.quartz.impl.mongodb.MongoJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint;
import org.quartz.spring.boot.health.QuartzHealthIndicator;
import org.quartz.spring.boot.metrics.QuartzJobMetrics;
import org.quartz.spring.boot.metrics.QuartzMetrics;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Slice tests for the health, metrics and endpoint auto-configurations. */
class QuartzActuatorAutoConfigurationTests {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  QuartzHealthContributorAutoConfiguration.class,
                  QuartzMetricsAutoConfiguration.class,
                  QuartzEndpointAutoConfiguration.class));

  @Test
  void backsOffWithoutAScheduler() {
    this.contextRunner
        .withPropertyValues("management.endpoints.web.exposure.include=quartz")
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .run(
            context -> {
              assertFalse(context.containsBean("quartzHealthContributor"));
              assertEquals(0, context.getBeanNamesForType(QuartzMetrics.class).length);
              assertEquals(0, context.getBeanNamesForType(QuartzEndpoint.class).length);
            });
  }

  @Test
  void healthContributorIsAutoConfigured() {
    schedulerRunner()
        .run(
            context -> {
              HealthContributor contributor = context.getBean(HealthContributor.class);
              assertTrue(contributor instanceof QuartzHealthIndicator);
            });
  }

  @Test
  void healthContributorCanBeDisabled() {
    schedulerRunner()
        .withPropertyValues("management.health.quartz.enabled=false")
        .run(
            context ->
                assertEquals(0, context.getBeanNamesForType(HealthContributor.class).length));
  }

  @Test
  void healthContributorBacksOffOnUserBean() {
    QuartzHealthIndicator userIndicator = mock(QuartzHealthIndicator.class);
    schedulerRunner()
        .withBean("quartzHealthIndicator", QuartzHealthIndicator.class, () -> userIndicator)
        .run(context -> assertSame(userIndicator, context.getBean(HealthContributor.class)));
  }

  @Test
  void meterBindersAreAutoConfigured() {
    schedulerRunner()
        .run(
            context -> {
              assertEquals(1, context.getBeanNamesForType(QuartzMetrics.class).length);
              assertEquals(1, context.getBeanNamesForType(QuartzJobMetrics.class).length);
            });
  }

  @Test
  void meterBindersBackOffWithoutAMeterRegistry() {
    this.contextRunner
        .withBean(Scheduler.class, QuartzActuatorAutoConfigurationTests::scheduler)
        .run(
            context -> {
              assertEquals(0, context.getBeanNamesForType(QuartzMetrics.class).length);
              assertEquals(0, context.getBeanNamesForType(QuartzJobMetrics.class).length);
            });
  }

  @Test
  void endpointIsAutoConfiguredWhenExposed() {
    schedulerRunner()
        .withPropertyValues("management.endpoints.web.exposure.include=quartz")
        .run(context -> assertEquals(1, context.getBeanNamesForType(QuartzEndpoint.class).length));
  }

  @Test
  void endpointBacksOffWhenAccessIsNone() {
    schedulerRunner()
        .withPropertyValues(
            "management.endpoints.web.exposure.include=quartz",
            "management.endpoint.quartz.access=NONE")
        .run(context -> assertEquals(0, context.getBeanNamesForType(QuartzEndpoint.class).length));
  }

  private ApplicationContextRunner schedulerRunner() {
    return this.contextRunner.withUserConfiguration(SchedulerConfiguration.class);
  }

  @Configuration(proxyBeanMethods = false)
  static class SchedulerConfiguration {

    @Bean
    Scheduler scheduler() {
      return QuartzActuatorAutoConfigurationTests.scheduler();
    }

    @Bean
    MeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
    }
  }

  private static Scheduler scheduler() {
    try {
      Scheduler scheduler = mock(Scheduler.class);
      when(scheduler.getSchedulerName()).thenReturn("quartzScheduler");
      when(scheduler.getListenerManager()).thenReturn(new ListenerManagerImpl());
      when(scheduler.getMetaData())
          .thenReturn(
              new SchedulerMetaData(
                  "quartzScheduler",
                  "instance-1",
                  StdScheduler.class,
                  false,
                  true,
                  false,
                  false,
                  Instant.parse("2026-01-01T10:15:30Z"),
                  0,
                  MongoJobStore.class,
                  true,
                  true,
                  SimpleThreadPool.class,
                  10));
      return scheduler;
    } catch (SchedulerException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
