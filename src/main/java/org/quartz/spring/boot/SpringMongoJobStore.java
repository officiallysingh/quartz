package org.quartz.spring.boot;

import com.mongodb.client.MongoClient;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

/** Mongo job store that prefers an injected Spring {@link MongoClient} and does not close it. */
public class SpringMongoJobStore extends org.quartz.impl.mongodb.MongoJobStore {

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
      throws SchedulerConfigException {
    if (getMongoClient() == null) {
      MongoClient shared = QuartzMongoClientHolder.get();
      if (shared != null) {
        setMongoClient(shared);
      }
    }
    if (getMongoClient() == null && (getMongoUri() == null || getMongoUri().isBlank())) {
      throw new SchedulerConfigException(
          "MongoDB job store needs a MongoClient bean or quartz.scheduler.mongodb.uri");
    }
    super.initialize(loadHelper, signaler);
  }
}
