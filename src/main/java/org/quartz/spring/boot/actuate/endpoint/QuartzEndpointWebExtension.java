package org.quartz.spring.boot.actuate.endpoint;

import java.util.Set;
import org.quartz.SchedulerException;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint.QuartzGroupsDescriptor;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint.QuartzJobDetailsDescriptor;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint.QuartzJobGroupSummaryDescriptor;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint.QuartzTriggerGroupSummaryDescriptor;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpointWebExtension.QuartzEndpointWebExtensionRuntimeHints;
import org.springframework.aot.hint.BindingReflectionHintsRegistrar;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.Show;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.EndpointWebExtension;
import org.springframework.context.annotation.ImportRuntimeHints;

/**
 * {@link EndpointWebExtension @EndpointWebExtension} for the {@link QuartzEndpoint}, exposing
 * {@code /actuator/quartz/jobs}, {@code /actuator/quartz/triggers} and their group and item paths.
 */
@EndpointWebExtension(endpoint = QuartzEndpoint.class)
@ImportRuntimeHints(QuartzEndpointWebExtensionRuntimeHints.class)
public class QuartzEndpointWebExtension {

  private final QuartzEndpoint delegate;

  private final Show showValues;

  private final Set<String> roles;

  public QuartzEndpointWebExtension(QuartzEndpoint delegate, Show showValues, Set<String> roles) {
    this.delegate = delegate;
    this.showValues = showValues;
    this.roles = roles;
  }

  @ReadOperation
  public WebEndpointResponse<QuartzGroupsDescriptor> quartzJobOrTriggerGroups(
      @Selector String jobsOrTriggers) throws SchedulerException {
    return handle(
        jobsOrTriggers, this.delegate::quartzJobGroups, this.delegate::quartzTriggerGroups);
  }

  @ReadOperation
  public WebEndpointResponse<Object> quartzJobOrTriggerGroup(
      @Selector String jobsOrTriggers, @Selector String group) throws SchedulerException {
    return handle(
        jobsOrTriggers,
        () -> this.delegate.quartzJobGroupSummary(group),
        () -> this.delegate.quartzTriggerGroupSummary(group));
  }

  @ReadOperation
  public WebEndpointResponse<Object> quartzJobOrTrigger(
      SecurityContext securityContext,
      @Selector String jobsOrTriggers,
      @Selector String group,
      @Selector String name)
      throws SchedulerException {
    boolean showUnsanitized = this.showValues.isShown(securityContext, this.roles);
    return handle(
        jobsOrTriggers,
        () -> this.delegate.quartzJob(group, name, showUnsanitized),
        () -> this.delegate.quartzTrigger(group, name, showUnsanitized));
  }

  /** Triggers a Quartz job, that is, runs it now, when {@code state} is {@code running}. */
  @WriteOperation
  public WebEndpointResponse<Object> triggerQuartzJob(
      @Selector String jobs, @Selector String group, @Selector String name, String state)
      throws SchedulerException {
    if ("jobs".equals(jobs) && "running".equals(state)) {
      return handleNull(this.delegate.triggerQuartzJob(group, name));
    }
    return new WebEndpointResponse<>(WebEndpointResponse.STATUS_BAD_REQUEST);
  }

  private <T> WebEndpointResponse<T> handle(
      String jobsOrTriggers, ResponseSupplier<T> jobAction, ResponseSupplier<T> triggerAction)
      throws SchedulerException {
    if ("jobs".equals(jobsOrTriggers)) {
      return handleNull(jobAction.get());
    }
    if ("triggers".equals(jobsOrTriggers)) {
      return handleNull(triggerAction.get());
    }
    return new WebEndpointResponse<>(WebEndpointResponse.STATUS_BAD_REQUEST);
  }

  private <T> WebEndpointResponse<T> handleNull(T value) {
    return value != null
        ? new WebEndpointResponse<>(value)
        : new WebEndpointResponse<>(WebEndpointResponse.STATUS_NOT_FOUND);
  }

  @FunctionalInterface
  private interface ResponseSupplier<T> {

    T get() throws SchedulerException;
  }

  static class QuartzEndpointWebExtensionRuntimeHints implements RuntimeHintsRegistrar {

    private final BindingReflectionHintsRegistrar bindingRegistrar =
        new BindingReflectionHintsRegistrar();

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
      this.bindingRegistrar.registerReflectionHints(
          hints.reflection(),
          QuartzGroupsDescriptor.class,
          QuartzJobDetailsDescriptor.class,
          QuartzJobGroupSummaryDescriptor.class,
          QuartzTriggerGroupSummaryDescriptor.class);
    }
  }
}
