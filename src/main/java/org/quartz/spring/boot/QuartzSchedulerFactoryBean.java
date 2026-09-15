package org.quartz.spring.boot;

import com.mongodb.client.MongoClient;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.NonNull;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;

/**
 * Lifecycle wrapper around {@link StdSchedulerFactory}, modeled on Spring's {@code
 * SchedulerFactoryBean} without JDBC / {@code DataSource} support.
 */
public class QuartzSchedulerFactoryBean
    implements FactoryBean<Scheduler>,
        InitializingBean,
        DisposableBean,
        SmartLifecycle,
        ApplicationContextAware {

  private Properties quartzProperties = new Properties();
  private JobDetail[] jobDetails = new JobDetail[0];
  private Trigger[] triggers = new Trigger[0];
  private Map<String, Calendar> calendars = Collections.emptyMap();
  private boolean autoStartup = true;
  private int startupDelay;
  private boolean waitForJobsToCompleteOnShutdown = true;
  private boolean overwriteExistingJobs;
  private MongoClient mongoClient;
  private ApplicationContext applicationContext;
  private Scheduler scheduler;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean destroyed = new AtomicBoolean(false);

  public void setQuartzProperties(Properties quartzProperties) {
    this.quartzProperties = quartzProperties;
  }

  public void setJobDetails(JobDetail[] jobDetails) {
    this.jobDetails = jobDetails != null ? jobDetails : new JobDetail[0];
  }

  public void setTriggers(Trigger[] triggers) {
    this.triggers = triggers != null ? triggers : new Trigger[0];
  }

  public void setCalendars(Map<String, Calendar> calendars) {
    this.calendars = calendars != null ? calendars : Collections.emptyMap();
  }

  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  public void setStartupDelay(int startupDelay) {
    this.startupDelay = startupDelay;
  }

  public void setWaitForJobsToCompleteOnShutdown(boolean waitForJobsToCompleteOnShutdown) {
    this.waitForJobsToCompleteOnShutdown = waitForJobsToCompleteOnShutdown;
  }

  public void setOverwriteExistingJobs(boolean overwriteExistingJobs) {
    this.overwriteExistingJobs = overwriteExistingJobs;
  }

  public void setMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  @Override
  public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    QuartzMongoClientHolder.set(mongoClient);
    StdSchedulerFactory factory = new StdSchedulerFactory();
    factory.initialize(quartzProperties);
    this.scheduler = factory.getScheduler();
    if (applicationContext != null) {
      AutowireCapableJobFactory jobFactory = new AutowireCapableJobFactory();
      AutowireCapableBeanFactory beanFactory = applicationContext.getAutowireCapableBeanFactory();
      jobFactory.setBeanFactory(beanFactory);
      scheduler.setJobFactory(jobFactory);
    }
    registerCalendars();
    registerJobsAndTriggers();
  }

  private void registerCalendars() throws SchedulerException {
    for (Map.Entry<String, Calendar> entry : calendars.entrySet()) {
      scheduler.addCalendar(entry.getKey(), entry.getValue(), true, true);
    }
  }

  private void registerJobsAndTriggers() throws SchedulerException {
    for (JobDetail jobDetail : jobDetails) {
      scheduler.addJob(jobDetail, overwriteExistingJobs, true);
    }
    for (Trigger trigger : triggers) {
      if (scheduler.checkExists(trigger.getKey())) {
        if (overwriteExistingJobs) {
          scheduler.rescheduleJob(trigger.getKey(), trigger);
        }
      } else {
        scheduler.scheduleJob(trigger);
      }
    }
  }

  @Override
  public Scheduler getObject() {
    return scheduler;
  }

  @Override
  public Class<?> getObjectType() {
    return Scheduler.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  @Override
  public void start() {
    if (scheduler == null) {
      return;
    }
    try {
      if (startupDelay > 0) {
        scheduler.startDelayed(startupDelay);
      } else {
        scheduler.start();
      }
      running.set(true);
    } catch (SchedulerException ex) {
      throw new IllegalStateException("Could not start Quartz scheduler", ex);
    }
  }

  @Override
  public void stop() {
    destroy();
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public boolean isAutoStartup() {
    return autoStartup;
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE;
  }

  @Override
  public void destroy() {
    if (!destroyed.compareAndSet(false, true)) {
      return;
    }
    running.set(false);
    try {
      if (scheduler != null && !scheduler.isShutdown()) {
        scheduler.shutdown(waitForJobsToCompleteOnShutdown);
      }
    } catch (SchedulerException ex) {
      throw new IllegalStateException("Could not shut down Quartz scheduler", ex);
    } finally {
      QuartzMongoClientHolder.set(null);
    }
  }
}
