package org.quartz.spring.boot;

import org.quartz.Scheduler;
import org.quartz.spring.boot.health.QuartzHealthIndicator;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.autoconfigure.contributor.CompositeHealthContributorConfiguration;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for {@link QuartzHealthIndicator}, contributed to the {@code health} endpoint
 * as {@code quartz}. Disable with {@code management.health.quartz.enabled=false}.
 */
@AutoConfiguration(after = QuartzAutoConfiguration.class)
@ConditionalOnClass({
  Scheduler.class,
  QuartzHealthIndicator.class,
  ConditionalOnEnabledHealthIndicator.class
})
@ConditionalOnBean(Scheduler.class)
@ConditionalOnEnabledHealthIndicator("quartz")
public class QuartzHealthContributorAutoConfiguration
    extends CompositeHealthContributorConfiguration<QuartzHealthIndicator, Scheduler> {

  QuartzHealthContributorAutoConfiguration() {
    super(QuartzHealthIndicator::new);
  }

  @Bean
  @ConditionalOnMissingBean(name = {"quartzHealthIndicator", "quartzHealthContributor"})
  HealthContributor quartzHealthContributor(ConfigurableListableBeanFactory beanFactory) {
    return createContributor(beanFactory, Scheduler.class);
  }
}
