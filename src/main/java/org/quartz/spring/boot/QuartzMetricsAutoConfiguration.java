package org.quartz.spring.boot;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.quartz.Scheduler;
import org.quartz.spring.boot.metrics.QuartzJobMetrics;
import org.quartz.spring.boot.metrics.QuartzMetrics;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for Quartz Micrometer metrics. The {@link MeterBinder} beans are bound to
 * every {@link MeterRegistry} by Spring Boot. Individual meters can be switched off with {@code
 * management.metrics.enable.quartz*}.
 */
@AutoConfiguration(
    after = QuartzAutoConfiguration.class,
    afterName = {
      "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
      "org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration"
    })
@ConditionalOnClass({Scheduler.class, MeterBinder.class})
@ConditionalOnBean({Scheduler.class, MeterRegistry.class})
public class QuartzMetricsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  QuartzMetrics quartzMetrics(Scheduler scheduler) {
    return new QuartzMetrics(scheduler);
  }

  @Bean
  @ConditionalOnMissingBean
  QuartzJobMetrics quartzJobMetrics(Scheduler scheduler) {
    return new QuartzJobMetrics(scheduler);
  }
}
