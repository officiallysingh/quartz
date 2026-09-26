/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package org.quartz.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessControlException;
import java.time.Duration;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerListener;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.matchers.EverythingMatcher;
import org.quartz.impl.mongodb.MongoJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.InstanceIdGenerator;
import org.quartz.spi.JobFactory;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerPlugin;
import org.quartz.spi.ThreadPool;
import org.quartz.utils.PropertiesParser;
import org.slf4j.Logger;

/**
 * An implementation of <code>{@link org.quartz.SchedulerFactory}</code> that creates a <code>
 * QuartzScheduler</code> from {@link Properties} (Spring Boot) or {@link #defaultProperties()
 * built-in defaults}.
 *
 * <p>Call {@link #initialize(Properties)} before {@link #getScheduler()} when the application
 * supplies config. Otherwise {@link #getScheduler()} uses {@link #defaultProperties()}.
 *
 * <p>The job store is always {@link org.quartz.impl.mongodb.MongoJobStore}. Specified <code>
 * {@link org.quartz.spi.ThreadPool}</code> and other SPI classes are created by name, and then any
 * additional properties specified for them are set on the instance by calling an equivalent 'set'
 * method. For example if the properties contain 'org.quartz.jobStore.myProp = 10' then after the
 * JobStore has been instantiated, 'setMyProp()' will be called on it. Type conversion to primitive
 * Java types (int, long, float, double, boolean, and String) are performed before calling the
 * property's setter method.
 *
 * <p>One property can reference another property's value by specifying a value following the
 * convention of "$@other.property.name", for example, to reference the scheduler's instance name as
 * the value for some other property, you would use "$@org.quartz.scheduler.instanceName".
 *
 * @author James House
 * @author Anthony Eden
 * @author Mohammad Rezaei
 */
@Slf4j
public class StdSchedulerFactory implements SchedulerFactory {

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constants.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  public static final String PROP_SCHED_INSTANCE_NAME = "org.quartz.scheduler.instanceName";

  public static final String PROP_SCHED_INSTANCE_ID = "org.quartz.scheduler.instanceId";

  public static final String PROP_SCHED_INSTANCE_ID_GENERATOR_PREFIX =
      "org.quartz.scheduler.instanceIdGenerator";

  public static final String PROP_SCHED_INSTANCE_ID_GENERATOR_CLASS =
      PROP_SCHED_INSTANCE_ID_GENERATOR_PREFIX + ".class";

  public static final String PROP_SCHED_THREAD_NAME = "org.quartz.scheduler.threadName";

  public static final String PROP_SCHED_BATCH_TIME_WINDOW =
      "org.quartz.scheduler.batchTriggerAcquisitionFireAheadTimeWindow";

  public static final String PROP_SCHED_MAX_BATCH_SIZE =
      "org.quartz.scheduler.batchTriggerAcquisitionMaxCount";

  public static final String PROP_SCHED_IDLE_WAIT_TIME = "org.quartz.scheduler.idleWaitTime";

  public static final String PROP_SCHED_MAKE_SCHEDULER_THREAD_DAEMON =
      "org.quartz.scheduler.makeSchedulerThreadDaemon";

  public static final String PROP_SCHED_JOB_FACTORY_CLASS = "org.quartz.scheduler.jobFactory.class";

  public static final String PROP_SCHED_JOB_FACTORY_PREFIX = "org.quartz.scheduler.jobFactory";

  public static final String PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN =
      "org.quartz.scheduler.interruptJobsOnShutdown";

  public static final String PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN_WITH_WAIT =
      "org.quartz.scheduler.interruptJobsOnShutdownWithWait";

  public static final String PROP_SCHED_CONTEXT_PREFIX = "org.quartz.context.key";

  public static final String PROP_THREAD_POOL_PREFIX = "org.quartz.threadPool";

  public static final String PROP_THREAD_POOL_CLASS = "org.quartz.threadPool.class";

  public static final String PROP_JOB_STORE_PREFIX = "org.quartz.jobStore";

  public static final String PROP_PLUGIN_PREFIX = "org.quartz.plugin";

  public static final String PROP_PLUGIN_CLASS = "class";

  public static final String PROP_JOB_LISTENER_PREFIX = "org.quartz.jobListener";

  public static final String PROP_TRIGGER_LISTENER_PREFIX = "org.quartz.triggerListener";

  public static final String PROP_LISTENER_CLASS = "class";

  public static final String DEFAULT_INSTANCE_ID = "NON_CLUSTERED";

  public static final String AUTO_GENERATE_INSTANCE_ID = "AUTO";

  public static final String SYSTEM_PROPERTY_AS_INSTANCE_ID = "SYS_PROP";

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Data members.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  private SchedulerException initException = null;

  private String propSrc = null;

  private PropertiesParser cfg;

  private MongoClient mongoClient;

  private MongoDatabase mongoDatabase;

  private ClassLoader jobClassLoader;

  private Map<String, SchedulerPlugin> schedulerPlugins = Map.of();

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Constructors.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  /** Create an uninitialized StdSchedulerFactory. */
  public StdSchedulerFactory() {}

  /**
   * Create a StdSchedulerFactory that has been initialized via <code>
   * {@link #initialize(Properties)}</code>.
   *
   * @see #initialize(Properties)
   */
  public StdSchedulerFactory(Properties props) throws SchedulerException {
    initialize(props);
  }

  /**
   * Create a StdSchedulerFactory that has been initialized via <code>{@link #initialize(String)}
   * </code>.
   *
   * @see #initialize(String)
   */
  public StdSchedulerFactory(String fileName) throws SchedulerException {
    initialize(fileName);
  }

  /*
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   *
   * Interface.
   *
   * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   */

  public Logger getLog() {
    return log;
  }

  /**
   * Inject the application's {@link MongoClient}. When set, {@link MongoJobStore} uses it instead
   * of opening a client from {@code mongoUri}.
   */
  public void setMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  /**
   * Use the application's Mongo database (from Spring Data {@code MongoDatabaseFactory}) when no
   * {@link MongoClient} bean is exposed.
   */
  public void setMongoDatabase(MongoDatabase mongoDatabase) {
    this.mongoDatabase = mongoDatabase;
  }

  /**
   * Class loader used to resolve {@code jobClass} FQCNs from Mongo. Spring Boot sets this to the
   * {@code ApplicationContext} class loader.
   */
  public void setJobClassLoader(ClassLoader jobClassLoader) {
    this.jobClassLoader = jobClassLoader;
  }

  /**
   * Already-constructed plugins (for example Spring beans) to initialize with the scheduler. Merged
   * with any {@code org.quartz.plugin.*} property plugins; a bean with the same name replaces the
   * property-constructed instance.
   */
  public void setSchedulerPlugins(Map<String, SchedulerPlugin> schedulerPlugins) {
    this.schedulerPlugins =
        schedulerPlugins == null || schedulerPlugins.isEmpty()
            ? Map.of()
            : Map.copyOf(schedulerPlugins);
  }

  /**
   * Built-in settings used when no {@link Properties} are supplied: clustered Mongo store, AUTO
   * lease owner, ten worker threads.
   */
  public static Properties defaultProperties() {
    Properties props = new Properties();
    props.setProperty(PROP_SCHED_INSTANCE_NAME, "quartzScheduler");
    props.setProperty(PROP_SCHED_INSTANCE_ID, AUTO_GENERATE_INSTANCE_ID);
    props.setProperty(PROP_THREAD_POOL_CLASS, SimpleThreadPool.class.getName());
    props.setProperty(PROP_THREAD_POOL_PREFIX + ".threadCount", "10");
    props.setProperty(PROP_THREAD_POOL_PREFIX + ".threadPriority", "5");
    props.setProperty(PROP_JOB_STORE_PREFIX + ".misfireThreshold", "60000");
    props.setProperty(PROP_JOB_STORE_PREFIX + ".collectionPrefix", "qrtz_");
    props.setProperty(PROP_JOB_STORE_PREFIX + ".isClustered", "true");
    props.setProperty(PROP_JOB_STORE_PREFIX + ".clusterCheckinInterval", "15000");
    return props;
  }

  /**
   * Initialize with {@link #defaultProperties()}, then overlay matching System properties ({@code
   * -Dorg.quartz.*}).
   */
  public void initialize() throws SchedulerException {
    if (cfg != null) {
      return;
    }
    if (initException != null) {
      throw initException;
    }

    propSrc = "built-in defaults";
    initialize(overrideWithSysProps(defaultProperties(), getLog()));
  }

  /**
   * Add all System properties to the given <code>props</code>. Will override any properties that
   * already exist in the given <code>props</code>.
   */
  // Visible for testing
  static Properties overrideWithSysProps(Properties props, Logger log) {
    Properties sysProps = null;
    try {
      sysProps = System.getProperties();
    } catch (AccessControlException e) {
      log.warn(
          "Skipping overriding quartz properties with System properties "
              + "during initialization because of an AccessControlException.  "
              + "This is likely due to not having read/write access for "
              + "java.util.PropertyPermission as required by java.lang.System.getProperties().  "
              + "To resolve this warning, either add this permission to your policy file or "
              + "use a non-default version of initialize().",
          e);
    }

    if (sysProps != null) {
      // Use the propertyNames to iterate to avoid
      // a possible ConcurrentModificationException
      Enumeration<?> en = sysProps.propertyNames();
      while (en.hasMoreElements()) {
        Object name = en.nextElement();
        Object value = sysProps.get(name);
        if (name instanceof String && value instanceof String) {
          // Properties javadoc discourages use of put so we use setProperty
          props.setProperty((String) name, (String) value);
        }
      }
    }

    return props;
  }

  /**
   * Initialize the <code>{@link org.quartz.SchedulerFactory}</code> with the contents of the <code>
   * Properties</code> file with the given name.
   */
  public void initialize(String filename) throws SchedulerException {
    // short-circuit if already initialized
    if (cfg != null) {
      return;
    }

    if (initException != null) {
      throw initException;
    }

    InputStream is;
    Properties props = new Properties();

    is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);

    try {
      if (is != null) {
        is = new BufferedInputStream(is);
        propSrc = "the specified file : '" + filename + "' from the class resource path.";
      } else {
        is = new BufferedInputStream(new FileInputStream(filename));
        propSrc = "the specified file : '" + filename + "'";
      }
      props.load(is);
    } catch (IOException ioe) {
      initException =
          new SchedulerException("Properties file: '" + filename + "' could not be read.", ioe);
      throw initException;
    } finally {
      if (is != null)
        try {
          is.close();
        } catch (IOException ignore) {
        }
    }

    initialize(props);
  }

  /**
   * Initialize the <code>{@link org.quartz.SchedulerFactory}</code> with the contents of the <code>
   * Properties</code> file opened with the given <code>InputStream</code>.
   */
  public void initialize(InputStream propertiesStream) throws SchedulerException {
    // short-circuit if already initialized
    if (cfg != null) {
      return;
    }

    if (initException != null) {
      throw initException;
    }

    Properties props = new Properties();

    if (propertiesStream != null) {
      try {
        props.load(propertiesStream);
        propSrc = "an externally opened InputStream.";
      } catch (IOException e) {
        initException = new SchedulerException("Error loading property data from InputStream", e);
        throw initException;
      }
    } else {
      initException =
          new SchedulerException(
              "Error loading property data from InputStream - InputStream is null.");
      throw initException;
    }

    initialize(props);
  }

  /**
   * Initialize with {@link #defaultProperties()}, then overlay the given {@code Properties}. Caller
   * keys win. Empty or partial maps still get clustered Mongo defaults and a usable thread pool.
   */
  public void initialize(Properties props) {
    if (propSrc == null) {
      propSrc = "an externally provided properties instance.";
    }

    Properties merged = defaultProperties();
    if (props != null) {
      merged.putAll(props);
    }
    this.cfg = new PropertiesParser(merged);
  }

  private Scheduler instantiate() throws SchedulerException {
    if (cfg == null) {
      initialize();
    }

    if (initException != null) {
      throw initException;
    }

    JobStore js;
    ThreadPool tp;
    QuartzScheduler qs = null;
    String instanceIdGeneratorClass = null;
    Properties tProps;
    boolean autoId = false;
    Duration idleWaitTime = null;
    String jobFactoryClass;

    SchedulerRepository schedRep = SchedulerRepository.getInstance();

    // Get Scheduler Properties
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    String schedName = cfg.getStringProperty(PROP_SCHED_INSTANCE_NAME, "QuartzScheduler");

    String threadName =
        cfg.getStringProperty(PROP_SCHED_THREAD_NAME, schedName + "_QuartzSchedulerThread");

    String schedInstId = cfg.getStringProperty(PROP_SCHED_INSTANCE_ID, DEFAULT_INSTANCE_ID);

    if (schedInstId.equals(AUTO_GENERATE_INSTANCE_ID)) {
      autoId = true;
      instanceIdGeneratorClass =
          cfg.getStringProperty(
              PROP_SCHED_INSTANCE_ID_GENERATOR_CLASS, "org.quartz.simpl.SimpleInstanceIdGenerator");
    } else if (schedInstId.equals(SYSTEM_PROPERTY_AS_INSTANCE_ID)) {
      autoId = true;
      instanceIdGeneratorClass = "org.quartz.simpl.SystemPropertyInstanceIdGenerator";
    }

    jobFactoryClass = cfg.getStringProperty(PROP_SCHED_JOB_FACTORY_CLASS, null);

    long idleWaitMillis = cfg.getLongProperty(PROP_SCHED_IDLE_WAIT_TIME, -1);
    if (idleWaitMillis > -1 && idleWaitMillis < 1000) {
      throw new SchedulerException(
          "org.quartz.scheduler.idleWaitTime of less than 1000ms is not legal.");
    }
    if (idleWaitMillis > -1) {
      idleWaitTime = Duration.ofMillis(idleWaitMillis);
    }

    boolean makeSchedulerThreadDaemon =
        cfg.getBooleanProperty(PROP_SCHED_MAKE_SCHEDULER_THREAD_DAEMON);

    Duration batchTimeWindow =
        Duration.ofMillis(cfg.getLongProperty(PROP_SCHED_BATCH_TIME_WINDOW, 0L));
    int maxBatchSize = cfg.getIntProperty(PROP_SCHED_MAX_BATCH_SIZE, 1);

    boolean interruptJobsOnShutdown =
        cfg.getBooleanProperty(PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN, false);
    boolean interruptJobsOnShutdownWithWait =
        cfg.getBooleanProperty(PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN_WITH_WAIT, false);

    rejectRemovedBoolean("org.quartz.scheduler.jmx.export");
    rejectRemovedBoolean("org.quartz.scheduler.jmx.proxy");
    rejectRemovedBoolean("org.quartz.scheduler.rmi.export");
    rejectRemovedBoolean("org.quartz.scheduler.rmi.proxy");
    rejectRemovedBoolean("org.quartz.scheduler.wrapJobExecutionInUserTransaction");
    rejectRemovedBoolean("org.quartz.managementRESTService.enabled");

    Properties schedCtxProps = cfg.getPropertyGroup(PROP_SCHED_CONTEXT_PREFIX, true);

    JobFactory jobFactory = null;
    if (jobFactoryClass != null) {
      try {
        jobFactory = (JobFactory) loadClass(jobFactoryClass).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new SchedulerConfigException(
            "Unable to instantiate JobFactory class: " + e.getMessage(), e);
      }

      tProps = cfg.getPropertyGroup(PROP_SCHED_JOB_FACTORY_PREFIX, true);
      try {
        setBeanProps(jobFactory, tProps);
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "JobFactory class '" + jobFactoryClass + "' props could not be configured.", e);
        throw initException;
      }
    }

    InstanceIdGenerator instanceIdGenerator = null;
    if (instanceIdGeneratorClass != null) {
      try {
        instanceIdGenerator =
            (InstanceIdGenerator)
                loadClass(instanceIdGeneratorClass).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new SchedulerConfigException(
            "Unable to instantiate InstanceIdGenerator class: " + e.getMessage(), e);
      }

      tProps = cfg.getPropertyGroup(PROP_SCHED_INSTANCE_ID_GENERATOR_PREFIX, true);
      try {
        setBeanProps(instanceIdGenerator, tProps);
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "InstanceIdGenerator class '"
                    + instanceIdGeneratorClass
                    + "' props could not be configured.",
                e);
        throw initException;
      }
    }

    // Get ThreadPool Properties
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    String tpClass =
        cfg.getStringProperty(PROP_THREAD_POOL_CLASS, SimpleThreadPool.class.getName());

    if (tpClass == null) {
      initException = new SchedulerException("ThreadPool class not specified. ");
      throw initException;
    }

    try {
      tp = (ThreadPool) loadClass(tpClass).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      initException =
          new SchedulerException(
              "ThreadPool class '" + tpClass + "' could not be instantiated.", e);
      throw initException;
    }
    tProps = cfg.getPropertyGroup(PROP_THREAD_POOL_PREFIX, true);
    try {
      setBeanProps(tp, tProps);
    } catch (Exception e) {
      initException =
          new SchedulerException(
              "ThreadPool class '" + tpClass + "' props could not be configured.", e);
      throw initException;
    }

    // Job store is always MongoDB.
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    MongoJobStore mongoJobStore = new MongoJobStore();
    if (mongoClient != null) {
      mongoJobStore.setMongoClient(mongoClient);
    }
    if (mongoDatabase != null) {
      mongoJobStore.setMongoDatabase(mongoDatabase);
    }
    js = mongoJobStore;

    tProps = cfg.getPropertyGroup(PROP_JOB_STORE_PREFIX, true);
    tProps.remove("class");
    try {
      setBeanProps(js, tProps);
    } catch (Exception e) {
      initException =
          new SchedulerException("MongoJobStore properties could not be configured.", e);
      throw initException;
    }

    // Set up any SchedulerPlugins
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    String[] pluginNames = cfg.getPropertyGroups(PROP_PLUGIN_PREFIX);
    SchedulerPlugin[] plugins = new SchedulerPlugin[pluginNames.length];
    for (int i = 0; i < pluginNames.length; i++) {
      Properties pp = cfg.getPropertyGroup(PROP_PLUGIN_PREFIX + "." + pluginNames[i], true);

      String plugInClass = pp.getProperty(PROP_PLUGIN_CLASS, null);

      if (plugInClass == null) {
        initException =
            new SchedulerException(
                "SchedulerPlugin class not specified for plugin '" + pluginNames[i] + "'");
        throw initException;
      }
      SchedulerPlugin plugin;
      try {
        plugin = (SchedulerPlugin) loadClass(plugInClass).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "SchedulerPlugin class '" + plugInClass + "' could not be instantiated.", e);
        throw initException;
      }
      try {
        setBeanProps(plugin, pp);
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "JobStore SchedulerPlugin '" + plugInClass + "' props could not be configured.", e);
        throw initException;
      }

      plugins[i] = plugin;
    }

    LinkedHashMap<String, SchedulerPlugin> mergedPlugins = new LinkedHashMap<>();
    for (int i = 0; i < pluginNames.length; i++) {
      mergedPlugins.put(pluginNames[i], plugins[i]);
    }
    mergedPlugins.putAll(this.schedulerPlugins);
    pluginNames = mergedPlugins.keySet().toArray(String[]::new);
    plugins = mergedPlugins.values().toArray(SchedulerPlugin[]::new);

    // Set up any JobListeners
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    String[] jobListenerNames = cfg.getPropertyGroups(PROP_JOB_LISTENER_PREFIX);
    JobListener[] jobListeners = new JobListener[jobListenerNames.length];
    for (int i = 0; i < jobListenerNames.length; i++) {
      Properties lp =
          cfg.getPropertyGroup(PROP_JOB_LISTENER_PREFIX + "." + jobListenerNames[i], true);

      String listenerClass = lp.getProperty(PROP_LISTENER_CLASS, null);

      if (listenerClass == null) {
        initException =
            new SchedulerException(
                "JobListener class not specified for listener '" + jobListenerNames[i] + "'");
        throw initException;
      }
      JobListener listener;
      try {
        listener = (JobListener) loadClass(listenerClass).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "JobListener class '" + listenerClass + "' could not be instantiated.", e);
        throw initException;
      }
      try {
        setListenerName(listener, jobListenerNames[i]);
        setBeanProps(listener, lp);
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "JobListener '" + listenerClass + "' props could not be configured.", e);
        throw initException;
      }
      jobListeners[i] = listener;
    }

    // Set up any TriggerListeners
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    String[] triggerListenerNames = cfg.getPropertyGroups(PROP_TRIGGER_LISTENER_PREFIX);
    TriggerListener[] triggerListeners = new TriggerListener[triggerListenerNames.length];
    for (int i = 0; i < triggerListenerNames.length; i++) {
      Properties lp =
          cfg.getPropertyGroup(PROP_TRIGGER_LISTENER_PREFIX + "." + triggerListenerNames[i], true);

      String listenerClass = lp.getProperty(PROP_LISTENER_CLASS, null);

      if (listenerClass == null) {
        initException =
            new SchedulerException(
                "TriggerListener class not specified for listener '"
                    + triggerListenerNames[i]
                    + "'");
        throw initException;
      }
      TriggerListener listener;
      try {
        listener =
            (TriggerListener) loadClass(listenerClass).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "TriggerListener class '" + listenerClass + "' could not be instantiated.", e);
        throw initException;
      }
      try {
        setListenerName(listener, triggerListenerNames[i]);
        setBeanProps(listener, lp);
      } catch (Exception e) {
        initException =
            new SchedulerException(
                "TriggerListener '" + listenerClass + "' props could not be configured.", e);
        throw initException;
      }
      triggerListeners[i] = listener;
    }

    boolean tpInited = false;
    boolean qsInited = false;

    // Fire everything up
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    try {
      JobRunShellFactory jrsf = new StdJobRunShellFactory();

      if (autoId) {
        try {
          schedInstId = instanceIdGenerator.generateInstanceId();
        } catch (Exception e) {
          getLog().error("Couldn't generate instance Id!", e);
          throw new IllegalStateException("Cannot run without an instance id.");
        }
      }

      QuartzSchedulerResources rsrcs = new QuartzSchedulerResources();
      rsrcs.setName(schedName);
      rsrcs.setThreadName(threadName);
      rsrcs.setInstanceId(schedInstId);
      rsrcs.setJobRunShellFactory(jrsf);
      rsrcs.setMakeSchedulerThreadDaemon(makeSchedulerThreadDaemon);
      rsrcs.setBatchTimeWindow(batchTimeWindow);
      rsrcs.setMaxBatchSize(maxBatchSize);
      rsrcs.setInterruptJobsOnShutdown(interruptJobsOnShutdown);
      rsrcs.setInterruptJobsOnShutdownWithWait(interruptJobsOnShutdownWithWait);

      tp.setInstanceName(schedName);
      tp.setInstanceId(schedInstId);

      rsrcs.setThreadPool(tp);
      tp.initialize();
      tpInited = true;

      rsrcs.setJobStore(js);

      // add plugins
      for (SchedulerPlugin plugin : plugins) {
        rsrcs.addSchedulerPlugin(plugin);
      }

      qs = new QuartzScheduler(rsrcs, idleWaitTime);
      qsInited = true;

      // Create Scheduler ref...
      Scheduler scheduler = instantiate(rsrcs, qs);

      // set job factory if specified
      if (jobFactory != null) {
        qs.setJobFactory(jobFactory);
      }

      // Initialize plugins now that we have a Scheduler instance.
      for (int i = 0; i < plugins.length; i++) {
        plugins[i].initialize(pluginNames[i], scheduler);
      }

      // add listeners
      for (JobListener jobListener : jobListeners) {
        qs.getListenerManager().addJobListener(jobListener, EverythingMatcher.allJobs());
      }
      for (TriggerListener triggerListener : triggerListeners) {
        qs.getListenerManager()
            .addTriggerListener(triggerListener, EverythingMatcher.allTriggers());
      }

      // set scheduler context data...
      for (Object key : schedCtxProps.keySet()) {
        String val = schedCtxProps.getProperty((String) key);
        scheduler.getContext().put((String) key, val);
      }

      // fire up job store, and runshell factory

      js.setInstanceId(schedInstId);
      js.setInstanceName(schedName);
      js.setThreadPoolSize(tp.getPoolSize());
      if (jobClassLoader != null && js instanceof MongoJobStore mongoStore) {
        mongoStore.setJobClassLoader(jobClassLoader);
      }
      js.initialize(qs.getSchedulerSignaler());

      jrsf.initialize(scheduler);

      qs.initialize();

      getLog()
          .info("Quartz scheduler '{}' initialized from {}", scheduler.getSchedulerName(), propSrc);

      // prevents the repository from being garbage collected
      qs.addNoGCObject(schedRep);

      schedRep.bind(scheduler);
      return scheduler;
    } catch (SchedulerException | Error | RuntimeException e) {
      shutdownFromInstantiateException(tp, qs, tpInited, qsInited);
      throw e;
    }
  }

  private void shutdownFromInstantiateException(
      ThreadPool tp, QuartzScheduler qs, boolean tpInited, boolean qsInited) {
    try {
      if (qsInited) qs.shutdown(false);
      else if (tpInited) tp.shutdown(false);
    } catch (Exception e) {
      getLog().error("Got another exception while shutting down after instantiation exception", e);
    }
  }

  protected Scheduler instantiate(QuartzSchedulerResources rsrcs, QuartzScheduler qs) {

    return new StdScheduler(qs);
  }

  private void rejectRemovedBoolean(String property) throws SchedulerConfigException {
    if (cfg.getBooleanProperty(property, false)) {
      throw new SchedulerConfigException(property + " is not supported in Quartz 3.0");
    }
  }

  /**
   * Property-configured listeners may expose {@code setName(String)}. Missing setters are ignored
   * so listeners that take the name in a constructor still work.
   */
  private static void setListenerName(Object listener, String name) throws Exception {
    try {
      listener.getClass().getMethod("setName", String.class).invoke(listener, name);
    } catch (NoSuchMethodException ignored) {
      // name is optional on JobListener / TriggerListener
    }
  }

  private void setBeanProps(Object obj, Properties props)
      throws NoSuchMethodException,
          IllegalAccessException,
          java.lang.reflect.InvocationTargetException,
          IntrospectionException,
          SchedulerConfigException {
    props.remove("class");

    BeanInfo bi = Introspector.getBeanInfo(obj.getClass());
    PropertyDescriptor[] propDescs = bi.getPropertyDescriptors();
    PropertiesParser pp = new PropertiesParser(props);

    java.util.Enumeration<Object> keys = props.keys();
    while (keys.hasMoreElements()) {
      String name = (String) keys.nextElement();
      String c = name.substring(0, 1).toUpperCase(Locale.US);
      String methName = "set" + c + name.substring(1);

      java.lang.reflect.Method setMeth = getSetMethod(methName, propDescs);

      try {
        if (setMeth == null) {
          throw new NoSuchMethodException("No setter for property '" + name + "'");
        }

        Class<?>[] params = setMeth.getParameterTypes();
        if (params.length != 1) {
          throw new NoSuchMethodException("No 1-argument setter for property '" + name + "'");
        }

        // does the property value reference another property's value? If so, swap to look at its
        // value
        PropertiesParser refProps = pp;
        String refName = pp.getStringProperty(name);
        if (refName != null && refName.startsWith("$@")) {
          refName = refName.substring(2);
          refProps = cfg;
        } else refName = name;

        if (params[0].equals(int.class)) {
          setMeth.invoke(obj, new Object[] {refProps.getIntProperty(refName)});
        } else if (params[0].equals(long.class)) {
          setMeth.invoke(obj, new Object[] {refProps.getLongProperty(refName)});
        } else if (params[0].equals(float.class)) {
          setMeth.invoke(obj, new Object[] {refProps.getFloatProperty(refName)});
        } else if (params[0].equals(double.class)) {
          setMeth.invoke(obj, new Object[] {refProps.getDoubleProperty(refName)});
        } else if (params[0].equals(boolean.class)) {
          setMeth.invoke(obj, new Object[] {refProps.getBooleanProperty(refName)});
        } else if (params[0].equals(String.class)) {
          setMeth.invoke(obj, new Object[] {refProps.getStringProperty(refName)});
        } else if (params[0].equals(Duration.class)) {
          setMeth.invoke(obj, new Object[] {parseDuration(refProps.getStringProperty(refName))});
        } else {
          throw new NoSuchMethodException("No primitive-type setter for property '" + name + "'");
        }
      } catch (NumberFormatException nfe) {
        throw new SchedulerConfigException(
            "Could not parse property '" + name + "' into correct data type: " + nfe);
      }
    }
  }

  /** Unitless numbers are milliseconds. Values starting with {@code P} are ISO-8601 durations. */
  private static Duration parseDuration(String value) {
    if (value == null || value.isBlank()) {
      throw new NumberFormatException("empty duration");
    }
    String trimmed = value.trim();
    if (trimmed.length() > 1 && (trimmed.charAt(0) == 'P' || trimmed.charAt(0) == 'p')) {
      return Duration.parse(trimmed);
    }
    return Duration.ofMillis(Long.parseLong(trimmed));
  }

  private java.lang.reflect.Method getSetMethod(String name, PropertyDescriptor[] props) {
    for (PropertyDescriptor prop : props) {
      java.lang.reflect.Method wMeth = prop.getWriteMethod();

      if (wMeth != null && wMeth.getName().equals(name)) {
        return wMeth;
      }
    }

    return null;
  }

  private Class<?> loadClass(String className)
      throws ClassNotFoundException, SchedulerConfigException {

    try {
      ClassLoader cl = findClassLoader();
      if (cl != null) return cl.loadClass(className);
      throw new SchedulerConfigException(
          "Unable to find a class loader on the current thread or class.");
    } catch (ClassNotFoundException e) {
      if (getClass().getClassLoader() != null)
        return getClass().getClassLoader().loadClass(className);
      throw e;
    }
  }

  private ClassLoader findClassLoader() {
    // work-around set context loader for windows-service started jvms (QUARTZ-748)
    if (Thread.currentThread().getContextClassLoader() == null
        && getClass().getClassLoader() != null) {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    }
    return Thread.currentThread().getContextClassLoader();
  }

  private String getSchedulerName() {
    return cfg.getStringProperty(PROP_SCHED_INSTANCE_NAME, "QuartzScheduler");
  }

  /**
   * Returns a handle to the Scheduler produced by this factory.
   *
   * <p>If one of the <code>initialize</code> methods has not been previously called, then {@link
   * #initialize()} applies {@link #defaultProperties()}.
   */
  public Scheduler getScheduler() throws SchedulerException {
    if (cfg == null) {
      initialize();
    }

    SchedulerRepository schedRep = SchedulerRepository.getInstance();

    Scheduler sched = schedRep.lookup(getSchedulerName());

    if (sched != null) {
      if (sched.isShutdown()) {
        schedRep.remove(getSchedulerName());
      } else {
        return sched;
      }
    }

    sched = instantiate();

    return sched;
  }

  /**
   * Returns a handle to the default Scheduler, creating it if it does not yet exist.
   *
   * @see #initialize()
   */
  public static Scheduler getDefaultScheduler() throws SchedulerException {
    StdSchedulerFactory fact = new StdSchedulerFactory();

    return fact.getScheduler();
  }

  /**
   * Returns a handle to the Scheduler with the given name, if it exists (if it has already been
   * instantiated).
   */
  public Scheduler getScheduler(String schedName) throws SchedulerException {
    return SchedulerRepository.getInstance().lookup(schedName);
  }

  /** Returns a handle to all known Schedulers (made by any StdSchedulerFactory instance.). */
  public Collection<Scheduler> getAllSchedulers() throws SchedulerException {
    return SchedulerRepository.getInstance().lookupAll();
  }
}
