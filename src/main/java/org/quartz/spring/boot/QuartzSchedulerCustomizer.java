package org.quartz.spring.boot;

/**
 * Callback to fine-tune the auto-configured {@link QuartzSchedulerFactoryBean}, same role as Spring
 * Boot's {@code SchedulerFactoryBeanCustomizer}.
 */
@FunctionalInterface
public interface QuartzSchedulerCustomizer {

  void customize(QuartzSchedulerFactoryBean factoryBean);
}
