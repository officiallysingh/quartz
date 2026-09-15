package org.quartz.impl.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.RAMJobStore.TriggerSnapshot;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent, cluster-capable {@link JobStore} backed by MongoDB.
 *
 * <p>Implements {@link JobStore} as a sibling of {@link RAMJobStore}: scheduling state is held in a
 * composed RAM store and written through to MongoDB under a cluster lock.
 *
 * <p>Inject an existing {@link MongoClient} (Spring Boot) via {@link #setMongoClient(MongoClient)}
 * and this store will not close it on shutdown. Otherwise it creates a client from {@code
 * mongoUri}.
 *
 * <p>Configure: {@code org.quartz.jobStore.class = org.quartz.impl.mongodb.MongoJobStore}
 */
public class MongoJobStore implements JobStore {

  public static final String DEFAULT_URI = "mongodb://localhost:27017";
  public static final String DEFAULT_DB = "quartz";
  public static final String DEFAULT_COLLECTION_PREFIX = "qrtz_";

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final RAMJobStore memory = new RAMJobStore();

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

  public void setMisfireThreshold(long misfireThreshold) {
    memory.setMisfireThreshold(misfireThreshold);
  }

  @Override
  public void setInstanceId(String schedInstId) {
    this.instanceId = schedInstId;
    memory.setInstanceId(schedInstId);
  }

  @Override
  public void setInstanceName(String schedName) {
    this.instanceName = schedName;
    memory.setInstanceName(schedName);
  }

  @Override
  public void setThreadPoolSize(int poolSize) {
    memory.setThreadPoolSize(poolSize);
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
  public long getAcquireRetryDelay(int failureCount) {
    return memory.getAcquireRetryDelay(failureCount);
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedSignaler)
      throws SchedulerConfigException {
    memory.initialize(loadHelper, schedSignaler);
    if (mongoClient == null) {
      if (mongoUri == null || mongoUri.isBlank()) {
        throw new SchedulerConfigException("mongoUri is required unless a MongoClient is injected");
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
    triggers.createIndex(new Document("schedName", 1).append("state", 1).append("nextFireTime", 1));
    locks.createIndex(
        new Document("schedName", 1).append("lockName", 1), new IndexOptions().unique(true));
    reloadFromMongo();
    log.info("MongoJobStore initialized on db '{}' (clustered={})", dbName, clustered);
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
  public void schedulerPaused() {
    memory.schedulerPaused();
  }

  @Override
  public void schedulerResumed() {
    memory.schedulerResumed();
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
    memory.shutdown();
  }

  private void withLock(PersistedOp op) throws JobPersistenceException {
    if (!obtainLock()) {
      throw new JobPersistenceException(
          "Could not obtain MongoDB cluster lock for scheduler '" + instanceName + "'");
    }
    try {
      reloadFromMongo();
      op.run();
      persistAll();
    } catch (JobPersistenceException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new JobPersistenceException(e.getMessage(), e);
    } finally {
      releaseLock();
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
            new FindOneAndUpdateOptions().upsert(true));
    if (result == null) {
      try {
        locks.insertOne(
            new Document("schedName", instanceName)
                .append("lockName", "TRIGGER_ACCESS")
                .append("owner", instanceId)
                .append("expires", expiry));
        return true;
      } catch (RuntimeException e) {
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
    try {
      List<JobDetail> jobDetails = new ArrayList<>();
      for (Document d : jobs.find(Filters.eq("schedName", instanceName))) {
        jobDetails.add(deserialize(d.get("payload", Binary.class)));
      }
      List<TriggerSnapshot> triggerSnapshots = new ArrayList<>();
      for (Document d : triggers.find(Filters.eq("schedName", instanceName))) {
        OperableTrigger trigger = deserialize(d.get("payload", Binary.class));
        int state = d.getInteger("state", RAMJobStore.TRIGGER_STATE_WAITING);
        triggerSnapshots.add(new TriggerSnapshot(trigger, state));
      }
      Map<String, Calendar> calMap = new HashMap<>();
      for (Document d : calendars.find(Filters.eq("schedName", instanceName))) {
        Calendar cal = deserialize(d.get("payload", Binary.class));
        calMap.put(d.getString("name"), cal);
      }
      Set<String> pausedTriggers = new HashSet<>();
      for (Document d : pausedTriggerGroupsCol.find(Filters.eq("schedName", instanceName))) {
        pausedTriggers.add(d.getString("group"));
      }
      Set<String> pausedJobs = new HashSet<>();
      for (Document d : pausedJobGroupsCol.find(Filters.eq("schedName", instanceName))) {
        pausedJobs.add(d.getString("group"));
      }
      memory.replaceAllSchedulingData(
          jobDetails, triggerSnapshots, calMap, pausedTriggers, pausedJobs);
    } catch (JobPersistenceException e) {
      throw new IllegalStateException(e);
    }
  }

  private void persistAll() {
    if (jobs == null) {
      return;
    }
    jobs.deleteMany(Filters.eq("schedName", instanceName));
    triggers.deleteMany(Filters.eq("schedName", instanceName));
    calendars.deleteMany(Filters.eq("schedName", instanceName));
    pausedTriggerGroupsCol.deleteMany(Filters.eq("schedName", instanceName));
    pausedJobGroupsCol.deleteMany(Filters.eq("schedName", instanceName));
    ReplaceOptions upsert = new ReplaceOptions().upsert(true);
    for (JobDetail detail : memory.exportJobs()) {
      JobKey key = detail.getKey();
      Document doc =
          new Document("schedName", instanceName)
              .append("name", key.getName())
              .append("group", key.getGroup())
              .append("payload", new Binary(serialize(detail)));
      jobs.replaceOne(
          Filters.and(
              Filters.eq("schedName", instanceName),
              Filters.eq("name", key.getName()),
              Filters.eq("group", key.getGroup())),
          doc,
          upsert);
    }
    for (TriggerSnapshot snap : memory.exportTriggers()) {
      OperableTrigger trigger = snap.trigger();
      Instant nft = trigger.getNextFireTime();
      Document doc =
          new Document("schedName", instanceName)
              .append("name", trigger.getKey().getName())
              .append("group", trigger.getKey().getGroup())
              .append("jobName", trigger.getJobKey().getName())
              .append("jobGroup", trigger.getJobKey().getGroup())
              .append("state", snap.state())
              .append("nextFireTime", nft == null ? null : nft.toEpochMilli())
              .append("priority", trigger.getPriority())
              .append("payload", new Binary(serialize(trigger)));
      triggers.replaceOne(
          Filters.and(
              Filters.eq("schedName", instanceName),
              Filters.eq("name", trigger.getKey().getName()),
              Filters.eq("group", trigger.getKey().getGroup())),
          doc,
          upsert);
    }
    for (Map.Entry<String, Calendar> e : memory.exportCalendars().entrySet()) {
      Document doc =
          new Document("schedName", instanceName)
              .append("name", e.getKey())
              .append("payload", new Binary(serialize(e.getValue())));
      calendars.replaceOne(
          Filters.and(Filters.eq("schedName", instanceName), Filters.eq("name", e.getKey())),
          doc,
          upsert);
    }
    for (String g : memory.exportPausedTriggerGroups()) {
      pausedTriggerGroupsCol.insertOne(new Document("schedName", instanceName).append("group", g));
    }
    for (String g : memory.exportPausedJobGroups()) {
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
      withLock(memory::recoverAcquiredTriggers);
    } catch (JobPersistenceException e) {
      log.error("Failed recovering orphaned triggers", e);
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
          log.error("Cluster check-in failed", e);
        }
      }
    }
  }

  @Override
  public void storeJob(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> memory.storeJob(newJob, replaceExisting));
  }

  @Override
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> memory.storeJobAndTrigger(newJob, newTrigger));
  }

  @Override
  public void storeJobsAndTriggers(
      Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> memory.storeJobsAndTriggers(triggersAndJobs, replace));
  }

  @Override
  public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = memory.removeJob(jobKey));
    return found[0];
  }

  @Override
  public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = memory.removeJobs(jobKeys));
    return found[0];
  }

  @Override
  public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    return memory.retrieveJob(jobKey);
  }

  @Override
  public List<JobDetail> getJobDetails(GroupMatcher<JobKey> matcher)
      throws JobPersistenceException {
    return memory.getJobDetails(matcher);
  }

  @Override
  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> memory.storeTrigger(newTrigger, replaceExisting));
  }

  @Override
  public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = memory.removeTrigger(triggerKey));
    return found[0];
  }

  @Override
  public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = memory.removeTriggers(triggerKeys));
    return found[0];
  }

  @Override
  public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
      throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = memory.replaceTrigger(triggerKey, newTrigger));
    return found[0];
  }

  @Override
  public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    return memory.retrieveTrigger(triggerKey);
  }

  @Override
  public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
    return memory.checkExists(jobKey);
  }

  @Override
  public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
    return memory.checkExists(triggerKey);
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    withLock(memory::clearAllSchedulingData);
  }

  @Override
  public void storeCalendar(
      String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> memory.storeCalendar(name, calendar, replaceExisting, updateTriggers));
  }

  @Override
  public boolean removeCalendar(String calName) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = memory.removeCalendar(calName));
    return found[0];
  }

  @Override
  public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
    return memory.retrieveCalendar(calName);
  }

  @Override
  public int getNumberOfJobs() throws JobPersistenceException {
    return memory.getNumberOfJobs();
  }

  @Override
  public int getNumberOfTriggers() throws JobPersistenceException {
    return memory.getNumberOfTriggers();
  }

  @Override
  public int getNumberOfCalendars() throws JobPersistenceException {
    return memory.getNumberOfCalendars();
  }

  @Override
  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    return memory.getJobKeys(matcher);
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    return memory.getTriggerKeys(matcher);
  }

  @Override
  public List<String> getJobGroupNames() throws JobPersistenceException {
    return memory.getJobGroupNames();
  }

  @Override
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    return memory.getTriggerGroupNames();
  }

  @Override
  public List<String> getCalendarNames() throws JobPersistenceException {
    return memory.getCalendarNames();
  }

  @Override
  public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
    return memory.getTriggersForJob(jobKey);
  }

  @Override
  public List<OperableTrigger> getTriggersByJobAndTriggerGroup(
      GroupMatcher<JobKey> jobMatcher, GroupMatcher<TriggerKey> triggerMatcher)
      throws JobPersistenceException {
    return memory.getTriggersByJobAndTriggerGroup(jobMatcher, triggerMatcher);
  }

  @Override
  public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
    return memory.getTriggerState(triggerKey);
  }

  @Override
  public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(() -> memory.resetTriggerFromErrorState(triggerKey));
  }

  @Override
  public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(() -> memory.pauseTrigger(triggerKey));
  }

  @Override
  public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    Collection<String>[] r = new Collection[1];
    withLock(() -> r[0] = memory.pauseTriggers(matcher));
    return r[0];
  }

  @Override
  public void pauseJob(JobKey jobKey) throws JobPersistenceException {
    withLock(() -> memory.pauseJob(jobKey));
  }

  @Override
  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    Collection<String>[] r = new Collection[1];
    withLock(() -> r[0] = memory.pauseJobs(groupMatcher));
    return r[0];
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(() -> memory.resumeTrigger(triggerKey));
  }

  @Override
  public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    Collection<String>[] r = new Collection[1];
    withLock(() -> r[0] = memory.resumeTriggers(matcher));
    return r[0];
  }

  @Override
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    return memory.getPausedTriggerGroups();
  }

  @Override
  public void resumeJob(JobKey jobKey) throws JobPersistenceException {
    withLock(() -> memory.resumeJob(jobKey));
  }

  @Override
  public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
      throws JobPersistenceException {
    Collection<String>[] r = new Collection[1];
    withLock(() -> r[0] = memory.resumeJobs(matcher));
    return r[0];
  }

  @Override
  public void pauseAll() throws JobPersistenceException {
    withLock(memory::pauseAll);
  }

  @Override
  public void resumeAll() throws JobPersistenceException {
    withLock(memory::resumeAll);
  }

  @Override
  public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
      throws JobPersistenceException {
    List<OperableTrigger>[] r = new List[1];
    withLock(() -> r[0] = memory.acquireNextTriggers(noLaterThan, maxCount, timeWindow));
    return r[0];
  }

  @Override
  public void releaseAcquiredTrigger(OperableTrigger trigger) {
    withLockUnchecked(() -> memory.releaseAcquiredTrigger(trigger));
  }

  @Override
  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers)
      throws JobPersistenceException {
    List<TriggerFiredResult>[] r = new List[1];
    withLock(() -> r[0] = memory.triggersFired(firedTriggers));
    return r[0];
  }

  @Override
  public void triggeredJobComplete(
      OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
    withLockUnchecked(() -> memory.triggeredJobComplete(trigger, jobDetail, triggerInstCode));
  }
}
