package org.quartz;

import java.util.Properties;
import org.quartz.impl.StdSchedulerFactory;

public class MongoSchedulerTest extends AbstractSchedulerTest {

  @Override
  protected Scheduler createScheduler(String name, int threadPoolSize) throws SchedulerException {
    Properties config =
        MongoSchedulerSupport.schedulerProperties(name + "Scheduler", threadPoolSize);
    return new StdSchedulerFactory(config).getScheduler();
  }
}
