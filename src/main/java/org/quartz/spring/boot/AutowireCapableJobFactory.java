package org.quartz.spring.boot;

import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;
import org.quartz.simpl.PropertySettingJobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

/**
 * Creates a new job instance per execution and autowires it, equivalent to Spring's {@code
 * SpringBeanJobFactory} without depending on {@code spring-context-support}.
 *
 * <p>Job classes are loaded from the Spring {@link AutowireCapableBeanFactory} class loader, not
 * from a serialized {@code Class} on a scheduler thread. Spring-created instances are destroyed
 * after the execution shell finishes.
 */
public class AutowireCapableJobFactory extends PropertySettingJobFactory {

  private AutowireCapableBeanFactory beanFactory;
  private ClassLoader classLoader;

  public void setBeanFactory(AutowireCapableBeanFactory beanFactory) {
    this.beanFactory = beanFactory;
  }

  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
    Class<? extends Job> jobClass = loadJobClass(bundle);
    if (beanFactory == null) {
      try {
        return jobClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new SchedulerException("Problem instantiating class '" + jobClass.getName() + "'", e);
      }
    }
    Job job = beanFactory.createBean(jobClass);
    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.putAll(scheduler.getContext());
    jobDataMap.putAll(bundle.getJobDetail().getJobDataMap());
    jobDataMap.putAll(bundle.getTrigger().getJobDataMap());
    setBeanProps(job, jobDataMap);
    return wrap(job);
  }

  private Class<? extends Job> loadJobClass(TriggerFiredBundle bundle) throws SchedulerException {
    Class<? extends Job> declared = bundle.getJobDetail().getJobClass();
    if (declared == null) {
      throw new SchedulerException("Job '" + bundle.getJobDetail().getKey() + "' has no job class");
    }
    String name = declared.getName();
    ClassLoader loader = classLoader;
    if (loader == null) {
      loader = Thread.currentThread().getContextClassLoader();
    }
    if (loader == null) {
      loader = getClass().getClassLoader();
    }
    try {
      return Class.forName(name, false, loader).asSubclass(Job.class);
    } catch (ClassNotFoundException | ClassCastException e) {
      throw new SchedulerException("Could not load job class '" + name + "'", e);
    }
  }

  private Job wrap(Job job) {
    if (job instanceof InterruptableJob interruptable) {
      return new DestroyingInterruptableJob(interruptable, beanFactory);
    }
    return new DestroyingJob(job, beanFactory);
  }

  static class DestroyingJob implements Job, AutoCloseable {
    final Job job;
    private final AutowireCapableBeanFactory beanFactory;

    DestroyingJob(Job job, AutowireCapableBeanFactory beanFactory) {
      this.job = job;
      this.beanFactory = beanFactory;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      job.execute(context);
    }

    @Override
    public void close() {
      beanFactory.destroyBean(job);
    }
  }

  static final class DestroyingInterruptableJob extends DestroyingJob implements InterruptableJob {
    DestroyingInterruptableJob(InterruptableJob job, AutowireCapableBeanFactory beanFactory) {
      super(job, beanFactory);
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
      ((InterruptableJob) job).interrupt();
    }
  }
}
