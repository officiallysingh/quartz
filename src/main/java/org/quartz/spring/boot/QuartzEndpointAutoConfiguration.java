package org.quartz.spring.boot;

import org.quartz.Scheduler;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpointWebExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.autoconfigure.endpoint.expose.EndpointExposure;
import org.springframework.boot.actuate.endpoint.SanitizingFunction;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the {@link QuartzEndpoint}. Expose it with {@code
 * management.endpoints.web.exposure.include=quartz}.
 */
@AutoConfiguration(after = QuartzAutoConfiguration.class)
@ConditionalOnClass({Scheduler.class, QuartzEndpoint.class, ConditionalOnAvailableEndpoint.class})
@ConditionalOnAvailableEndpoint(QuartzEndpoint.class)
@EnableConfigurationProperties(QuartzEndpointProperties.class)
public class QuartzEndpointAutoConfiguration {

  @Bean
  @ConditionalOnBean(Scheduler.class)
  @ConditionalOnMissingBean
  QuartzEndpoint quartzEndpoint(
      Scheduler scheduler, ObjectProvider<SanitizingFunction> sanitizingFunctions) {
    return new QuartzEndpoint(scheduler, sanitizingFunctions.orderedStream().toList());
  }

  @Bean
  @ConditionalOnBean(QuartzEndpoint.class)
  @ConditionalOnMissingBean
  @ConditionalOnAvailableEndpoint(exposure = EndpointExposure.WEB)
  QuartzEndpointWebExtension quartzEndpointWebExtension(
      QuartzEndpoint endpoint, QuartzEndpointProperties properties) {
    return new QuartzEndpointWebExtension(
        endpoint, properties.getShowValues(), properties.getRoles());
  }
}
