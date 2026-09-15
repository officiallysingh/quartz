package org.quartz.spring.boot;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.simpl.PropertySettingJobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

/**
 * Creates a new job instance per execution and autowires it, equivalent to Spring's {@code
 * SpringBeanJobFactory} without depending on {@code spring-context-support}.
 */
public class AutowireCapableJobFactory extends PropertySettingJobFactory {

  private AutowireCapableBeanFactory beanFactory;

  public void setBeanFactory(AutowireCapableBeanFactory beanFactory) {
    this.beanFactory = beanFactory;
  }

  @Override
  public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
    if (beanFactory == null) {
      return super.newJob(bundle, scheduler);
    }
    Job job = beanFactory.createBean(bundle.getJobDetail().getJobClass());
    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.putAll(scheduler.getContext());
    jobDataMap.putAll(bundle.getJobDetail().getJobDataMap());
    jobDataMap.putAll(bundle.getTrigger().getJobDataMap());
    setBeanProps(job, jobDataMap);
    return job;
  }
}
