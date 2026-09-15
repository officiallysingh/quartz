package org.quartz.spring.boot;

/** Job store selection for this Quartz fork. JDBC is not supported. */
public enum JobStoreType {

  /** Use MongoDB when a {@code MongoClient} bean exists, otherwise RAM. */
  AUTO,

  /** In-memory {@code RAMJobStore}. */
  MEMORY,

  /** MongoDB {@code MongoJobStore}. */
  MONGODB
}
