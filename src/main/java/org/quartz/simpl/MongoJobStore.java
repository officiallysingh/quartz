package org.quartz.simpl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.bson.types.Binary;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;

/**
 * Persistent, cluster-capable {@link org.quartz.spi.JobStore} backed by MongoDB.
 *
 * <p>Inject an existing {@link MongoClient} (Spring Boot) via {@link #setMongoClient(MongoClient)}
 * and this store will not close it on shutdown. Otherwise it creates a client from {@code
 * mongoUri}.
 */
public class MongoJobStore extends RAMJobStore {

  public static final String DEFAULT_URI = "mongodb://localhost:27017";
  public static final String DEFAULT_DB = "quartz";
  public static final String DEFAULT_COLLECTION_PREFIX = "qrtz_";

  private String mongoUri = DEFAULT_URI;
  private String dbName = DEFAULT_DB;
  private String collectionPrefix = DEFAULT_COLLECTION_PREFIX;
  private boolean clustered;
  private long clusterCheckinInterval = 15000L;
  private String instanceId = "NON_CLUSTERED";
  private String instanceName = "QuartzScheduler";

  private MongoClient mongoClient;
  private boolean ownsClient;
  private MongoDatabase database;
  private MongoCollection<Document> jobs;
  private MongoCollection<Document> triggers;
  private MongoCollection<Document> calendars;
  private MongoCollection<Document> pausedTriggerGroupsCol;
  private MongoCollection<Document> pausedJobGroupsCol;
  private MongoCollection<Document> schedulerState;
  private MongoCollection<Document> locks;

  private final ThreadLocal<Boolean> skipPersist = ThreadLocal.withInitial(() -> Boolean.FALSE);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private ClusterManager clusterManager;

  public void setMongoUri(String mongoUri) {
    this.mongoUri = mongoUri;
  }

  public String getMongoUri() {
    return mongoUri;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setCollectionPrefix(String collectionPrefix) {
    this.collectionPrefix =
        (collectionPrefix == null || collectionPrefix.isBlank())
            ? DEFAULT_COLLECTION_PREFIX
            : collectionPrefix;
  }

  public String getCollectionPrefix() {
    return collectionPrefix;
  }

  public void setMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
    this.ownsClient = false;
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }

  public void setIsClustered(boolean clustered) {
    this.clustered = clustered;
  }

  public void setClustered(boolean clustered) {
    this.clustered = clustered;
  }

  public void setClusterCheckinInterval(long clusterCheckinInterval) {
    this.clusterCheckinInterval = clusterCheckinInterval;
  }

  @Override
  public void setInstanceId(String schedInstId) {
    this.instanceId = schedInstId;
  }

  @Override
  public void setInstanceName(String schedName) {
    this.instanceName = schedName;
  }

  @Override
  public boolean supportsPersistence() {
    return true;
  }

  @Override
  public boolean isClustered() {
    return clustered;
  }

  @Override
  public long getEstimatedTimeToReleaseAndAcquireTrigger() {
    return 70;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedSignaler) {
    super.initialize(loadHelper, schedSignaler);
    try {
      if (mongoClient == null) {
        if (mongoUri == null || mongoUri.isBlank()) {
          throw new SchedulerConfigException(
              "mongoUri is required unless a MongoClient is injected");
        }
        mongoClient = MongoClients.create(mongoUri);
        ownsClient = true;
      }
      database = mongoClient.getDatabase(dbName);
      jobs = database.getCollection(collectionPrefix + "jobs");
      triggers = database.getCollection(collectionPrefix + "triggers");
      calendars = database.getCollection(collectionPrefix + "calendars");
      pausedTriggerGroupsCol = database.getCollection(collectionPrefix + "paused_trigger_groups");
      pausedJobGroupsCol = database.getCollection(collectionPrefix + "paused_job_groups");
      schedulerState = database.getCollection(collectionPrefix + "scheduler_state");
      locks = database.getCollection(collectionPrefix + "locks");
      jobs.createIndex(
          new Document("schedName", 1).append("name", 1).append("group", 1),
          new IndexOptions().unique(true));
      triggers.createIndex(
          new Document("schedName", 1).append("name", 1).append("group", 1),
          new IndexOptions().unique(true));
      triggers.createIndex(
          new Document("schedName", 1).append("state", 1).append("nextFireTime", 1));
      locks.createIndex(
          new Document("schedName", 1).append("lockName", 1), new IndexOptions().unique(true));
      reloadFromMongo();
      getLog().info("MongoJobStore initialized on db '{}' (clustered={})", dbName, clustered);
    } catch (SchedulerConfigException e) {
      throw new IllegalStateException(e);
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @Override
  public void schedulerStarted() {
    started.set(true);
    checkin();
    if (clustered) {
      clusterManager = new ClusterManager();
      clusterManager.setDaemon(true);
      clusterManager.start();
    }
    recoverOrphanedTriggers();
  }

  @Override
  public void shutdown() {
    started.set(false);
    if (clusterManager != null) {
      clusterManager.interrupt();
    }
    try {
      locks.deleteMany(
          Filters.and(Filters.eq("schedName", instanceName), Filters.eq("owner", instanceId)));
      schedulerState.deleteOne(
          Filters.and(Filters.eq("schedName", instanceName), Filters.eq("instanceId", instanceId)));
    } catch (RuntimeException ignore) {
      // shutting down
    }
    if (ownsClient && mongoClient != null) {
      mongoClient.close();
    }
    super.shutdown();
  }

  private void withLock(PersistedOp op) throws JobPersistenceException {
    boolean locked = obtainLock();
    try {
      reloadFromMongo();
      op.run();
      persistAll();
    } catch (JobPersistenceException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new JobPersistenceException(e.getMessage(), e);
    } finally {
      if (locked) {
        releaseLock();
      }
    }
  }

  private void withLockUnchecked(PersistedOp op) {
    try {
      withLock(op);
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  private boolean obtainLock() {
    Instant expiry = Instant.now().plusMillis(Math.max(clusterCheckinInterval * 2, 30000));
    Document result =
        locks.findOneAndUpdate(
            Filters.and(
                Filters.eq("schedName", instanceName),
                Filters.eq("lockName", "TRIGGER_ACCESS"),
                Filters.or(
                    Filters.eq("owner", instanceId),
                    Filters.lte("expires", Instant.now()),
                    Filters.exists("owner", false))),
            Updates.combine(Updates.set("owner", instanceId), Updates.set("expires", expiry)),
            new com.mongodb.client.model.FindOneAndUpdateOptions().upsert(true));
    if (result == null) {
      try {
        locks.insertOne(
            new Document("schedName", instanceName)
                .append("lockName", "TRIGGER_ACCESS")
                .append("owner", instanceId)
                .append("expires", expiry));
        return true;
      } catch (RuntimeException e) {
        // another node won
        long waitUntil = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < waitUntil) {
          Document existing =
              locks
                  .find(
                      Filters.and(
                          Filters.eq("schedName", instanceName),
                          Filters.eq("lockName", "TRIGGER_ACCESS"),
                          Filters.or(
                              Filters.eq("owner", instanceId),
                              Filters.lte("expires", Instant.now()))))
                  .first();
          if (existing != null) {
            locks.updateOne(
                Filters.eq("_id", existing.getObjectId("_id")),
                Updates.combine(Updates.set("owner", instanceId), Updates.set("expires", expiry)));
            return true;
          }
          try {
            Thread.sleep(50);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
          }
        }
        return false;
      }
    }
    return true;
  }

  private void releaseLock() {
    locks.updateOne(
        Filters.and(
            Filters.eq("schedName", instanceName),
            Filters.eq("lockName", "TRIGGER_ACCESS"),
            Filters.eq("owner", instanceId)),
        Updates.unset("owner"));
  }

  private void reloadFromMongo() {
    skipPersist.set(true);
    try {
      super.clearAllSchedulingData();
      pausedJobGroups.clear();
      blockedJobs.clear();
      for (Document d : jobs.find(Filters.eq("schedName", instanceName))) {
        JobDetail detail = deserialize(d.get("payload", Binary.class));
        super.storeJob(detail, true);
      }
      for (Document d : triggers.find(Filters.eq("schedName", instanceName))) {
        OperableTrigger trigger = deserialize(d.get("payload", Binary.class));
        int state = d.getInteger("state", TriggerWrapper.STATE_WAITING);
        super.storeTrigger(trigger, true);
        TriggerWrapper tw = triggersByKey.get(trigger.getKey());
        if (tw != null) {
          tw.state = state;
          if (state != TriggerWrapper.STATE_WAITING) {
            timeTriggers.remove(tw);
          }
        }
      }
      for (Document d : calendars.find(Filters.eq("schedName", instanceName))) {
        Calendar cal = deserialize(d.get("payload", Binary.class));
        super.storeCalendar(d.getString("name"), cal, true, false);
      }
      pausedTriggerGroups.clear();
      for (Document d : pausedTriggerGroupsCol.find(Filters.eq("schedName", instanceName))) {
        pausedTriggerGroups.add(d.getString("group"));
      }
      pausedJobGroups.clear();
      for (Document d : pausedJobGroupsCol.find(Filters.eq("schedName", instanceName))) {
        pausedJobGroups.add(d.getString("group"));
      }
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    } finally {
      skipPersist.set(false);
    }
  }

  private void persistAll() {
    if (Boolean.TRUE.equals(skipPersist.get()) || jobs == null) {
      return;
    }
    jobs.deleteMany(Filters.eq("schedName", instanceName));
    triggers.deleteMany(Filters.eq("schedName", instanceName));
    calendars.deleteMany(Filters.eq("schedName", instanceName));
    pausedTriggerGroupsCol.deleteMany(Filters.eq("schedName", instanceName));
    pausedJobGroupsCol.deleteMany(Filters.eq("schedName", instanceName));
    ReplaceOptions upsert = new ReplaceOptions().upsert(true);
    for (JobWrapper jw : jobsByKey.values()) {
      Document doc =
          new Document("schedName", instanceName)
              .append("name", jw.key.getName())
              .append("group", jw.key.getGroup())
              .append("payload", new Binary(serialize(jw.jobDetail)));
      jobs.replaceOne(
          Filters.and(
              Filters.eq("schedName", instanceName),
              Filters.eq("name", jw.key.getName()),
              Filters.eq("group", jw.key.getGroup())),
          doc,
          upsert);
    }
    for (TriggerWrapper tw : triggersByKey.values()) {
      Instant nft = tw.trigger.getNextFireTime();
      Document doc =
          new Document("schedName", instanceName)
              .append("name", tw.key.getName())
              .append("group", tw.key.getGroup())
              .append("jobName", tw.jobKey.getName())
              .append("jobGroup", tw.jobKey.getGroup())
              .append("state", tw.state)
              .append("nextFireTime", nft == null ? null : nft.toEpochMilli())
              .append("priority", tw.trigger.getPriority())
              .append("payload", new Binary(serialize(tw.trigger)));
      triggers.replaceOne(
          Filters.and(
              Filters.eq("schedName", instanceName),
              Filters.eq("name", tw.key.getName()),
              Filters.eq("group", tw.key.getGroup())),
          doc,
          upsert);
    }
    for (Map.Entry<String, Calendar> e : calendarsByName.entrySet()) {
      Document doc =
          new Document("schedName", instanceName)
              .append("name", e.getKey())
              .append("payload", new Binary(serialize(e.getValue())));
      calendars.replaceOne(
          Filters.and(Filters.eq("schedName", instanceName), Filters.eq("name", e.getKey())),
          doc,
          upsert);
    }
    for (String g : pausedTriggerGroups) {
      pausedTriggerGroupsCol.insertOne(new Document("schedName", instanceName).append("group", g));
    }
    for (String g : pausedJobGroups) {
      pausedJobGroupsCol.insertOne(new Document("schedName", instanceName).append("group", g));
    }
  }

  private void checkin() {
    if (schedulerState == null) {
      return;
    }
    schedulerState.replaceOne(
        Filters.and(Filters.eq("schedName", instanceName), Filters.eq("instanceId", instanceId)),
        new Document("schedName", instanceName)
            .append("instanceId", instanceId)
            .append("lastCheckin", Instant.now().toEpochMilli())
            .append("checkinInterval", clusterCheckinInterval),
        new ReplaceOptions().upsert(true));
  }

  private void recoverOrphanedTriggers() {
    try {
      withLock(
          () -> {
            long now = System.currentTimeMillis();
            java.util.Set<String> live = new java.util.HashSet<>();
            for (Document st : schedulerState.find(Filters.eq("schedName", instanceName))) {
              long last = st.getLong("lastCheckin");
              long interval = st.getLong("checkinInterval");
              if (last + interval * 2 >= now) {
                live.add(st.getString("instanceId"));
              }
            }
            live.add(instanceId);
            for (TriggerWrapper tw : List.copyOf(triggersByKey.values())) {
              if (tw.state == TriggerWrapper.STATE_ACQUIRED) {
                tw.state = TriggerWrapper.STATE_WAITING;
                timeTriggers.add(tw);
              }
            }
          });
    } catch (JobPersistenceException e) {
      getLog().error("Failed recovering orphaned triggers", e);
    }
  }

  @FunctionalInterface
  private interface PersistedOp {
    void run() throws JobPersistenceException;
  }

  private static byte[] serialize(Object value) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(value);
      return bos.toByteArray();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to serialize scheduling object", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T deserialize(Binary binary) {
    if (binary == null) {
      return null;
    }
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(binary.getData()))) {
      return (T) ois.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to deserialize scheduling object", e);
    }
  }

  private class ClusterManager extends Thread {
    ClusterManager() {
      super("QuartzMongoCluster_" + instanceName);
    }

    @Override
    public void run() {
      while (started.get()) {
        try {
          Thread.sleep(clusterCheckinInterval);
          checkin();
          recoverOrphanedTriggers();
        } catch (InterruptedException e) {
          interrupt();
          break;
        } catch (RuntimeException e) {
          getLog().error("Cluster check-in failed", e);
        }
      }
    }
  }

  @Override
  public void storeJob(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException {
    try {
      withLock(() -> super.storeJob(newJob, replaceExisting));
    } catch (ObjectAlreadyExistsException e) {
      throw e;
    } catch (JobPersistenceException e) {
      throw new ObjectAlreadyExistsException(e.getMessage());
    }
  }

  @Override
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> super.storeJobAndTrigger(newJob, newTrigger));
  }

  @Override
  public void storeJobsAndTriggers(
      Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> super.storeJobsAndTriggers(triggersAndJobs, replace));
  }

  @Override
  public boolean removeJob(JobKey jobKey) {
    boolean[] found = {false};
    try {
      withLock(() -> found[0] = super.removeJob(jobKey));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
    return found[0];
  }

  @Override
  public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = super.removeJobs(jobKeys));
    return found[0];
  }

  @Override
  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> super.storeTrigger(newTrigger, replaceExisting));
  }

  @Override
  public boolean removeTrigger(TriggerKey triggerKey) {
    boolean[] found = {false};
    try {
      withLock(() -> found[0] = super.removeTrigger(triggerKey));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
    return found[0];
  }

  @Override
  public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = super.removeTriggers(triggerKeys));
    return found[0];
  }

  @Override
  public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
      throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = super.replaceTrigger(triggerKey, newTrigger));
    return found[0];
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    withLock(super::clearAllSchedulingData);
  }

  @Override
  public void storeCalendar(
      String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
      throws ObjectAlreadyExistsException {
    withLockUnchecked(() -> super.storeCalendar(name, calendar, replaceExisting, updateTriggers));
  }

  @Override
  public boolean removeCalendar(String calName) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = super.removeCalendar(calName));
    return found[0];
  }

  @Override
  public void pauseTrigger(TriggerKey triggerKey) {
    try {
      withLock(() -> super.pauseTrigger(triggerKey));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) {
    List<String>[] r = new List[1];
    withLockUnchecked(() -> r[0] = super.pauseTriggers(matcher));
    return r[0];
  }

  @Override
  public void pauseJob(JobKey jobKey) {
    try {
      withLock(() -> super.pauseJob(jobKey));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<String> pauseJobs(GroupMatcher<JobKey> matcher) {
    List<String>[] r = new List[1];
    withLockUnchecked(() -> r[0] = super.pauseJobs(matcher));
    return r[0];
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey) {
    try {
      withLock(() -> super.resumeTrigger(triggerKey));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) {
    List<String>[] r = new List[1];
    withLockUnchecked(() -> r[0] = super.resumeTriggers(matcher));
    return r[0];
  }

  @Override
  public void resumeJob(JobKey jobKey) {
    try {
      withLock(() -> super.resumeJob(jobKey));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) {
    Collection<String>[] r = new Collection[1];
    try {
      withLock(() -> r[0] = super.resumeJobs(matcher));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
    return r[0];
  }

  @Override
  public void pauseAll() {
    try {
      withLock(super::pauseAll);
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void resumeAll() {
    try {
      withLock(super::resumeAll);
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(() -> super.resetTriggerFromErrorState(triggerKey));
  }

  @Override
  public List<OperableTrigger> acquireNextTriggers(
      long noLaterThan, int maxCount, long timeWindow) {
    List<OperableTrigger>[] r = new List[1];
    withLockUnchecked(() -> r[0] = super.acquireNextTriggers(noLaterThan, maxCount, timeWindow));
    return r[0];
  }

  @Override
  public void releaseAcquiredTrigger(OperableTrigger trigger) {
    try {
      withLock(() -> super.releaseAcquiredTrigger(trigger));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers) {
    List<TriggerFiredResult>[] r = new List[1];
    withLockUnchecked(() -> r[0] = super.triggersFired(firedTriggers));
    return r[0];
  }

  @Override
  public void triggeredJobComplete(
      OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
    try {
      withLock(() -> super.triggeredJobComplete(trigger, jobDetail, triggerInstCode));
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }
}
