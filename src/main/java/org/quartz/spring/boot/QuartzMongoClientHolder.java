package org.quartz.spring.boot;

import com.mongodb.client.MongoClient;

/**
 * Lets {@link SpringMongoJobStore} receive the application {@link MongoClient} even though Quartz
 * instantiates the job store by class name.
 */
final class QuartzMongoClientHolder {

  private static volatile MongoClient mongoClient;

  private QuartzMongoClientHolder() {}

  static void set(MongoClient mongoClient) {
    QuartzMongoClientHolder.mongoClient = mongoClient;
  }

  static MongoClient get() {
    return mongoClient;
  }
}
