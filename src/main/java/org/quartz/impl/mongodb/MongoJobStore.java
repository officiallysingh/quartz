package org.quartz.impl.mongodb;

/**
 * Public MongoDB job store. Implementation lives in {@link org.quartz.simpl.MongoJobStore}
 * so it can reuse RAM job-store internals. Configure:
 * {@code org.quartz.jobStore.class = org.quartz.impl.mongodb.MongoJobStore}
 */
public class MongoJobStore extends org.quartz.simpl.MongoJobStore {
}
