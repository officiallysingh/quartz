package org.quartz.impl.mongodb;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
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
import org.quartz.impl.matchers.StringMatcher.StringOperatorName;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

/**
 * Persistent, cluster-capable {@link JobStore} backed by MongoDB.
 *
 * <p>This is the JDBC job store with collections instead of tables: each job, trigger, and calendar
 * is its own document. Unique indexes on {@code (schedName, name, group)} reject duplicates.
 * Acquire uses {@code findOneAndUpdate} on {@code state=WAITING}, the same compare-and-set JDBC
 * used with {@code UPDATE … WHERE TRIGGER_STATE='WAITING'}.
 *
 * <p>Inject an existing {@link MongoClient} (Spring Boot) via {@link #setMongoClient(MongoClient)}
 * and this store will not close it on shutdown. Otherwise it creates a client from {@code
 * mongoUri}.
 */
@Slf4j
public class MongoJobStore implements JobStore {

  public static final String DEFAULT_URI = "mongodb://localhost:27017";
  public static final String DEFAULT_DB = "quartz";
  public static final String DEFAULT_COLLECTION_PREFIX = "qrtz_";

  static final int STATE_WAITING = 0;
  static final int STATE_ACQUIRED = 1;
  static final int STATE_COMPLETE = 3;
  static final int STATE_PAUSED = 4;
  static final int STATE_BLOCKED = 5;
  static final int STATE_PAUSED_BLOCKED = 6;
  static final int STATE_ERROR = 7;

  private static final AtomicLong FIRE_IDS = new AtomicLong(System.currentTimeMillis());

  private final ReentrantLock localLock = new ReentrantLock(true);
  private final AtomicBoolean started = new AtomicBoolean();

  private String mongoUri = DEFAULT_URI;
  private String dbName = DEFAULT_DB;
  private String collectionPrefix = DEFAULT_COLLECTION_PREFIX;
  private boolean clustered;
  private long clusterCheckinInterval = 15000L;
  private long misfireThreshold = 60000L;
  private String instanceId = "NON_CLUSTERED";
  private String instanceName = "QuartzScheduler";
  private boolean ownsClient;
  private MongoClient mongoClient;
  private MongoCollection<Document> jobs;
  private MongoCollection<Document> triggers;
  private MongoCollection<Document> calendars;
  private MongoCollection<Document> pausedTriggerGroupsCol;
  private MongoCollection<Document> pausedJobGroupsCol;
  private MongoCollection<Document> schedulerState;
  private MongoCollection<Document> locks;
  private SchedulerSignaler signaler;
  private ClusterManager clusterManager;

  public String getMongoUri() {
    return mongoUri;
  }

  public void setMongoUri(String mongoUri) {
    this.mongoUri = mongoUri;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setCollectionPrefix(String collectionPrefix) {
    this.collectionPrefix = collectionPrefix;
  }

  public MongoClient getMongoClient() {
    return mongoClient;
  }

  public void setMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
    this.ownsClient = false;
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
    if (misfireThreshold < 1) {
      throw new IllegalArgumentException("misfireThreshold must be >= 1");
    }
    this.misfireThreshold = misfireThreshold;
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
  public void setThreadPoolSize(int poolSize) {
    // unused — same as JDBC JobStore
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
    return 20;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedSignaler)
      throws SchedulerConfigException {
    this.signaler = schedSignaler;
    if (mongoClient == null) {
      if (mongoUri == null || mongoUri.isBlank()) {
        throw new SchedulerConfigException("mongoUri is required unless a MongoClient is injected");
      }
      mongoClient = MongoClients.create(mongoUri);
      ownsClient = true;
    }
    MongoDatabase database = mongoClient.getDatabase(dbName);
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
    triggers.createIndex(new Document("schedName", 1).append("jobName", 1).append("jobGroup", 1));
    locks.createIndex(
        new Document("schedName", 1).append("lockName", 1), new IndexOptions().unique(true));
    log.info("MongoJobStore initialized on db '{}' (clustered={})", dbName, clustered);
  }

  @Override
  public void schedulerStarted() {
    started.set(true);
    checkin();
    recoverAcquiredTriggers();
    if (clustered) {
      clusterManager = new ClusterManager();
      clusterManager.setDaemon(true);
      clusterManager.start();
    }
  }

  @Override
  public void schedulerPaused() {}

  @Override
  public void schedulerResumed() {}

  @Override
  public void shutdown() {
    started.set(false);
    if (clusterManager != null) {
      clusterManager.interrupt();
    }
    try {
      if (locks != null) {
        locks.deleteMany(
            Filters.and(Filters.eq("schedName", instanceName), Filters.eq("owner", instanceId)));
      }
      if (schedulerState != null) {
        schedulerState.deleteOne(
            Filters.and(
                Filters.eq("schedName", instanceName), Filters.eq("instanceId", instanceId)));
      }
    } catch (RuntimeException ignore) {
      // Client may already be closed on shutdown.
    }
    if (ownsClient && mongoClient != null) {
      mongoClient.close();
    }
  }

  @Override
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(
        () -> {
          storeJobLocked(newJob, false);
          storeTriggerLocked(newTrigger, false);
        });
  }

  @Override
  public void storeJob(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> storeJobLocked(newJob, replaceExisting));
  }

  @Override
  public void storeJobsAndTriggers(
      Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(
        () -> {
          if (!replace) {
            for (Map.Entry<JobDetail, Set<? extends Trigger>> e : triggersAndJobs.entrySet()) {
              if (jobDoc(e.getKey().getKey()) != null) {
                throw new ObjectAlreadyExistsException(e.getKey());
              }
              for (Trigger trigger : e.getValue()) {
                if (triggerDoc(trigger.getKey()) != null) {
                  throw new ObjectAlreadyExistsException(trigger);
                }
              }
            }
          }
          for (Map.Entry<JobDetail, Set<? extends Trigger>> e : triggersAndJobs.entrySet()) {
            storeJobLocked(e.getKey(), true);
            for (Trigger trigger : e.getValue()) {
              storeTriggerLocked((OperableTrigger) trigger, true);
            }
          }
        });
  }

  @Override
  public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = removeJobLocked(jobKey));
    return found[0];
  }

  @Override
  public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
    boolean[] all = {true};
    withLock(
        () -> {
          for (JobKey key : jobKeys) {
            all[0] = removeJobLocked(key) && all[0];
          }
        });
    return all[0];
  }

  @Override
  public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    Document doc = jobDoc(jobKey);
    return doc == null ? null : deserialize(doc.get("payload", Binary.class));
  }

  @Override
  public List<JobDetail> getJobDetails(GroupMatcher<JobKey> matcher)
      throws JobPersistenceException {
    List<JobDetail> out = new ArrayList<>();
    for (Document d : jobs.find(andSched(groupFilter(matcher)))) {
      out.add(deserialize(d.get("payload", Binary.class)));
    }
    return out;
  }

  @Override
  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(() -> storeTriggerLocked(newTrigger, replaceExisting));
  }

  @Override
  public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(() -> found[0] = removeTriggerLocked(triggerKey, true));
    return found[0];
  }

  @Override
  public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
    boolean[] all = {true};
    withLock(
        () -> {
          for (TriggerKey key : triggerKeys) {
            all[0] = removeTriggerLocked(key, true) && all[0];
          }
        });
    return all[0];
  }

  @Override
  public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
      throws JobPersistenceException {
    boolean[] found = {false};
    withLock(
        () -> {
          Document existing = triggerDoc(triggerKey);
          if (existing == null) {
            return;
          }
          OperableTrigger old = deserialize(existing.get("payload", Binary.class));
          if (!old.getJobKey().equals(newTrigger.getJobKey())) {
            throw new JobPersistenceException(
                "New trigger is not related to the same job as the old trigger.");
          }
          removeTriggerLocked(triggerKey, false);
          try {
            storeTriggerLocked(newTrigger, false);
          } catch (JobPersistenceException e) {
            storeTriggerLocked(old, true);
            throw e;
          }
          found[0] = true;
        });
    return found[0];
  }

  @Override
  public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    Document doc = triggerDoc(triggerKey);
    if (doc == null) {
      return null;
    }
    OperableTrigger trigger = deserialize(doc.get("payload", Binary.class));
    return (OperableTrigger) trigger.clone();
  }

  @Override
  public boolean checkExists(JobKey jobKey) {
    return jobDoc(jobKey) != null;
  }

  @Override
  public boolean checkExists(TriggerKey triggerKey) {
    return triggerDoc(triggerKey) != null;
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    withLock(
        () -> {
          Bson sched = Filters.eq("schedName", instanceName);
          jobs.deleteMany(sched);
          triggers.deleteMany(sched);
          calendars.deleteMany(sched);
          pausedTriggerGroupsCol.deleteMany(sched);
          pausedJobGroupsCol.deleteMany(sched);
        });
  }

  @Override
  public void storeCalendar(
      String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    withLock(
        () -> {
          Bson filter =
              Filters.and(Filters.eq("schedName", instanceName), Filters.eq("name", name));
          Document existing = calendars.find(filter).first();
          if (existing != null && !replaceExisting) {
            throw new ObjectAlreadyExistsException(
                "Calendar with name '" + name + "' already exists.");
          }
          Calendar stored = (Calendar) calendar.clone();
          calendars.replaceOne(
              filter,
              new Document("schedName", instanceName)
                  .append("name", name)
                  .append("payload", new Binary(serialize(stored))),
              new ReplaceOptions().upsert(true));
          if (existing != null && updateTriggers) {
            for (Document d :
                triggers.find(
                    Filters.and(
                        Filters.eq("schedName", instanceName), Filters.eq("calendarName", name)))) {
              OperableTrigger trigger = deserialize(d.get("payload", Binary.class));
              trigger.updateWithNewCalendar(stored, misfireThreshold);
              replaceTriggerDoc(trigger, d.getInteger("state", STATE_WAITING));
            }
          }
        });
  }

  @Override
  public boolean removeCalendar(String calName) throws JobPersistenceException {
    boolean[] found = {false};
    withLock(
        () -> {
          long refs =
              triggers.countDocuments(
                  Filters.and(
                      Filters.eq("schedName", instanceName), Filters.eq("calendarName", calName)));
          if (refs > 0) {
            throw new JobPersistenceException(
                "Calender cannot be removed if it referenced by a Trigger!");
          }
          found[0] =
              calendars
                      .deleteOne(
                          Filters.and(
                              Filters.eq("schedName", instanceName), Filters.eq("name", calName)))
                      .getDeletedCount()
                  > 0;
        });
    return found[0];
  }

  @Override
  public Calendar retrieveCalendar(String calName) {
    Document doc =
        calendars
            .find(Filters.and(Filters.eq("schedName", instanceName), Filters.eq("name", calName)))
            .first();
    if (doc == null) {
      return null;
    }
    Calendar cal = deserialize(doc.get("payload", Binary.class));
    return (Calendar) cal.clone();
  }

  @Override
  public int getNumberOfJobs() {
    return (int) jobs.countDocuments(Filters.eq("schedName", instanceName));
  }

  @Override
  public int getNumberOfTriggers() {
    return (int) triggers.countDocuments(Filters.eq("schedName", instanceName));
  }

  @Override
  public int getNumberOfCalendars() {
    return (int) calendars.countDocuments(Filters.eq("schedName", instanceName));
  }

  @Override
  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) {
    Set<JobKey> keys = new HashSet<>();
    for (Document d : jobs.find(andSched(groupFilter(matcher)))) {
      keys.add(new JobKey(d.getString("name"), d.getString("group")));
    }
    return keys;
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) {
    Set<TriggerKey> keys = new HashSet<>();
    for (Document d : triggers.find(andSched(groupFilter(matcher)))) {
      keys.add(new TriggerKey(d.getString("name"), d.getString("group")));
    }
    return keys;
  }

  @Override
  public List<String> getJobGroupNames() {
    return jobs.distinct("group", Filters.eq("schedName", instanceName), String.class)
        .into(new ArrayList<>());
  }

  @Override
  public List<String> getTriggerGroupNames() {
    return triggers
        .distinct("group", Filters.eq("schedName", instanceName), String.class)
        .into(new ArrayList<>());
  }

  @Override
  public List<String> getCalendarNames() {
    List<String> names = new ArrayList<>();
    for (Document d : calendars.find(Filters.eq("schedName", instanceName))) {
      names.add(d.getString("name"));
    }
    return names;
  }

  @Override
  public List<OperableTrigger> getTriggersForJob(JobKey jobKey) {
    List<OperableTrigger> list = new ArrayList<>();
    for (Document d :
        triggers.find(
            Filters.and(
                Filters.eq("schedName", instanceName),
                Filters.eq("jobName", jobKey.getName()),
                Filters.eq("jobGroup", jobKey.getGroup())))) {
      OperableTrigger t = deserialize(d.get("payload", Binary.class));
      list.add((OperableTrigger) t.clone());
    }
    return list;
  }

  @Override
  public List<OperableTrigger> getTriggersByJobAndTriggerGroup(
      GroupMatcher<JobKey> jobMatcher, GroupMatcher<TriggerKey> triggerMatcher) {
    Set<JobKey> jobsInGroup = getJobKeys(jobMatcher);
    List<OperableTrigger> list = new ArrayList<>();
    for (Document d : triggers.find(andSched(groupFilter(triggerMatcher)))) {
      JobKey jobKey = new JobKey(d.getString("jobName"), d.getString("jobGroup"));
      if (jobsInGroup.contains(jobKey)) {
        OperableTrigger t = deserialize(d.get("payload", Binary.class));
        list.add((OperableTrigger) t.clone());
      }
    }
    return list;
  }

  @Override
  public TriggerState getTriggerState(TriggerKey triggerKey) {
    Document doc = triggerDoc(triggerKey);
    if (doc == null) {
      return TriggerState.NONE;
    }
    return toPublicState(doc.getInteger("state", STATE_WAITING));
  }

  @Override
  public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(
        () -> {
          Document doc = triggerDoc(triggerKey);
          if (doc == null || doc.getInteger("state", STATE_WAITING) != STATE_ERROR) {
            return;
          }
          OperableTrigger trigger = deserialize(doc.get("payload", Binary.class));
          int state = pausedTriggerGroup(triggerKey.getGroup()) ? STATE_PAUSED : STATE_WAITING;
          replaceTriggerDoc(trigger, state);
        });
  }

  @Override
  public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(() -> pauseTriggerLocked(triggerKey));
  }

  @Override
  public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    List<String>[] groups = new List[1];
    withLock(
        () -> {
          groups[0] = rememberPausedGroups(pausedTriggerGroupsCol, matcher, getTriggerGroupNames());
          for (String group : groups[0]) {
            for (TriggerKey key : getTriggerKeys(GroupMatcher.triggerGroupEquals(group))) {
              pauseTriggerLocked(key);
            }
          }
        });
    return groups[0];
  }

  @Override
  public void pauseJob(JobKey jobKey) throws JobPersistenceException {
    withLock(
        () -> {
          for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
            pauseTriggerLocked(trigger.getKey());
          }
        });
  }

  @Override
  public Collection<String> pauseJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    List<String>[] groups = new List[1];
    withLock(
        () -> {
          groups[0] = rememberPausedGroups(pausedJobGroupsCol, matcher, getJobGroupNames());
          for (String group : groups[0]) {
            for (JobKey jobKey : getJobKeys(GroupMatcher.jobGroupEquals(group))) {
              for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
                pauseTriggerLocked(trigger.getKey());
              }
            }
          }
        });
    return groups[0];
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(() -> resumeTriggerLocked(triggerKey));
  }

  @Override
  public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    Set<String>[] groups = new Set[1];
    withLock(
        () -> {
          groups[0] = new LinkedHashSet<>();
          for (TriggerKey key : getTriggerKeys(matcher)) {
            Document doc = triggerDoc(key);
            if (doc != null && pausedJobGroup(doc.getString("jobGroup"))) {
              continue;
            }
            groups[0].add(key.getGroup());
            resumeTriggerLocked(key);
          }
          clearPausedGroups(pausedTriggerGroupsCol, matcher);
        });
    return groups[0];
  }

  @Override
  public Set<String> getPausedTriggerGroups() {
    Set<String> groups = new HashSet<>();
    for (Document d : pausedTriggerGroupsCol.find(Filters.eq("schedName", instanceName))) {
      groups.add(d.getString("group"));
    }
    return groups;
  }

  @Override
  public void resumeJob(JobKey jobKey) throws JobPersistenceException {
    withLock(
        () -> {
          for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
            resumeTriggerLocked(trigger.getKey());
          }
        });
  }

  @Override
  public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
      throws JobPersistenceException {
    Set<String>[] groups = new Set[1];
    withLock(
        () -> {
          groups[0] = new LinkedHashSet<>();
          for (JobKey jobKey : getJobKeys(matcher)) {
            groups[0].add(jobKey.getGroup());
            for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
              resumeTriggerLocked(trigger.getKey());
            }
          }
          clearPausedGroups(pausedJobGroupsCol, matcher);
        });
    return groups[0];
  }

  @Override
  public void pauseAll() throws JobPersistenceException {
    pauseTriggers(GroupMatcher.anyTriggerGroup());
  }

  @Override
  public void resumeAll() throws JobPersistenceException {
    withLock(
        () -> {
          pausedJobGroupsCol.deleteMany(Filters.eq("schedName", instanceName));
          for (TriggerKey key : getTriggerKeys(GroupMatcher.anyTriggerGroup())) {
            resumeTriggerLocked(key);
          }
          pausedTriggerGroupsCol.deleteMany(Filters.eq("schedName", instanceName));
        });
  }

  @Override
  public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
      throws JobPersistenceException {
    List<OperableTrigger>[] result = new List[1];
    withLock(() -> result[0] = acquireNextTriggersLocked(noLaterThan, maxCount, timeWindow));
    return result[0];
  }

  @Override
  public void releaseAcquiredTrigger(OperableTrigger trigger) {
    withLockUnchecked(
        () -> {
          Document doc = triggerDoc(trigger.getKey());
          if (doc != null && doc.getInteger("state", STATE_WAITING) == STATE_ACQUIRED) {
            OperableTrigger stored = deserialize(doc.get("payload", Binary.class));
            replaceTriggerDoc(stored, STATE_WAITING);
          }
        });
  }

  @Override
  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers)
      throws JobPersistenceException {
    List<TriggerFiredResult>[] result = new List[1];
    withLock(() -> result[0] = triggersFiredLocked(firedTriggers));
    return result[0];
  }

  @Override
  public void triggeredJobComplete(
      OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
    withLockUnchecked(() -> triggeredJobCompleteLocked(trigger, jobDetail, triggerInstCode));
  }

  private void storeJobLocked(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException {
    JobDetail stored = (JobDetail) newJob.clone();
    Document existing = jobDoc(stored.getKey());
    if (existing != null && !replaceExisting) {
      throw new ObjectAlreadyExistsException(newJob);
    }
    Document doc =
        new Document("schedName", instanceName)
            .append("name", stored.getKey().getName())
            .append("group", stored.getKey().getGroup())
            .append("durable", stored.isDurable())
            .append("requestsRecovery", stored.requestsRecovery())
            .append("concurrentDisallowed", stored.isConcurrentExecutionDisallowed())
            .append("persistJobData", stored.isPersistJobDataAfterExecution())
            .append(
                "blocked", existing != null && Boolean.TRUE.equals(existing.getBoolean("blocked")))
            .append("payload", new Binary(serialize(stored)));
    try {
      jobs.replaceOne(jobFilter(stored.getKey()), doc, new ReplaceOptions().upsert(true));
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
        throw new ObjectAlreadyExistsException(newJob);
      }
      throw e;
    }
  }

  private void storeTriggerLocked(OperableTrigger newTrigger, boolean replaceExisting)
      throws JobPersistenceException {
    if (jobDoc(newTrigger.getJobKey()) == null) {
      throw new JobPersistenceException(
          "The job (" + newTrigger.getJobKey() + ") referenced by the trigger does not exist.");
    }
    Document existing = triggerDoc(newTrigger.getKey());
    if (existing != null && !replaceExisting) {
      throw new ObjectAlreadyExistsException(newTrigger);
    }
    if (existing != null) {
      removeTriggerLocked(newTrigger.getKey(), false);
    }
    OperableTrigger stored = (OperableTrigger) newTrigger.clone();
    int state = STATE_WAITING;
    if (pausedTriggerGroup(stored.getKey().getGroup())
        || pausedJobGroup(stored.getJobKey().getGroup())) {
      state = STATE_PAUSED;
      Document job = jobDoc(stored.getJobKey());
      if (job != null && Boolean.TRUE.equals(job.getBoolean("blocked"))) {
        state = STATE_PAUSED_BLOCKED;
      }
    } else {
      Document job = jobDoc(stored.getJobKey());
      if (job != null && Boolean.TRUE.equals(job.getBoolean("blocked"))) {
        state = STATE_BLOCKED;
      }
    }
    insertTrigger(stored, state);
  }

  private boolean removeJobLocked(JobKey jobKey) {
    boolean found = false;
    for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
      removeTriggerLocked(trigger.getKey(), false);
      found = true;
    }
    return jobs.deleteOne(jobFilter(jobKey)).getDeletedCount() > 0 || found;
  }

  private boolean removeTriggerLocked(TriggerKey key, boolean removeOrphanedJob) {
    Document doc = triggerDoc(key);
    if (doc == null) {
      return false;
    }
    JobKey jobKey = new JobKey(doc.getString("jobName"), doc.getString("jobGroup"));
    triggers.deleteOne(triggerFilter(key));
    if (removeOrphanedJob) {
      Document job = jobDoc(jobKey);
      if (job != null
          && !Boolean.TRUE.equals(job.getBoolean("durable"))
          && getTriggersForJob(jobKey).isEmpty()) {
        jobs.deleteOne(jobFilter(jobKey));
        if (signaler != null) {
          signaler.notifySchedulerListenersJobDeleted(jobKey);
        }
      }
    }
    return true;
  }

  private void pauseTriggerLocked(TriggerKey triggerKey) {
    Document doc = triggerDoc(triggerKey);
    if (doc == null) {
      return;
    }
    int state = doc.getInteger("state", STATE_WAITING);
    if (state == STATE_COMPLETE) {
      return;
    }
    OperableTrigger trigger = deserialize(doc.get("payload", Binary.class));
    replaceTriggerDoc(trigger, state == STATE_BLOCKED ? STATE_PAUSED_BLOCKED : STATE_PAUSED);
  }

  private void resumeTriggerLocked(TriggerKey triggerKey) {
    Document doc = triggerDoc(triggerKey);
    if (doc == null) {
      return;
    }
    int state = doc.getInteger("state", STATE_WAITING);
    if (state != STATE_PAUSED && state != STATE_PAUSED_BLOCKED) {
      return;
    }
    OperableTrigger trigger = deserialize(doc.get("payload", Binary.class));
    Document job = jobDoc(trigger.getJobKey());
    int next =
        job != null && Boolean.TRUE.equals(job.getBoolean("blocked"))
            ? STATE_BLOCKED
            : STATE_WAITING;
    replaceTriggerDoc(trigger, next);
    applyMisfire(trigger, next);
  }

  private List<OperableTrigger> acquireNextTriggersLocked(
      long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
    List<OperableTrigger> result = new ArrayList<>();
    Set<JobKey> acquiredJobs = new HashSet<>();
    Set<TriggerKey> skipped = new HashSet<>();
    long batchEnd = noLaterThan;
    int safety = 0;
    while (result.size() < maxCount && safety++ < 10_000) {
      Document doc = nextWaiting(batchEnd, skipped);
      if (doc == null) {
        break;
      }
      OperableTrigger trigger = deserialize(doc.get("payload", Binary.class));
      int state = doc.getInteger("state", STATE_WAITING);
      if (applyMisfire(trigger, state)) {
        continue;
      }
      Instant nft = trigger.getNextFireTime();
      if (nft == null) {
        skipped.add(trigger.getKey());
        continue;
      }
      if (nft.toEpochMilli() > batchEnd) {
        break;
      }
      JobDetail job = retrieveJob(trigger.getJobKey());
      if (job == null) {
        skipped.add(trigger.getKey());
        continue;
      }
      if (job.isConcurrentExecutionDisallowed() && acquiredJobs.contains(job.getKey())) {
        skipped.add(trigger.getKey());
        continue;
      }
      trigger.setFireInstanceId(String.valueOf(FIRE_IDS.incrementAndGet()));
      Document claimed =
          triggers.findOneAndUpdate(
              Filters.and(triggerFilter(trigger.getKey()), Filters.eq("state", STATE_WAITING)),
              Updates.combine(
                  Updates.set("state", STATE_ACQUIRED),
                  Updates.set("acquiredBy", instanceId),
                  Updates.set("payload", new Binary(serialize(trigger))),
                  Updates.set(
                      "nextFireTime",
                      trigger.getNextFireTime() == null
                          ? null
                          : trigger.getNextFireTime().toEpochMilli())),
              new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
      if (claimed == null) {
        continue;
      }
      if (result.isEmpty()) {
        batchEnd = Math.max(nft.toEpochMilli(), System.currentTimeMillis()) + timeWindow;
      }
      result.add((OperableTrigger) trigger.clone());
      if (job.isConcurrentExecutionDisallowed()) {
        acquiredJobs.add(job.getKey());
      }
    }
    return result;
  }

  private List<TriggerFiredResult> triggersFiredLocked(List<OperableTrigger> firedTriggers)
      throws JobPersistenceException {
    List<TriggerFiredResult> results = new ArrayList<>();
    for (OperableTrigger trigger : firedTriggers) {
      Document doc = triggerDoc(trigger.getKey());
      if (doc == null || doc.getInteger("state", STATE_WAITING) != STATE_ACQUIRED) {
        continue;
      }
      OperableTrigger stored = deserialize(doc.get("payload", Binary.class));
      Calendar cal = null;
      if (stored.getCalendarName() != null) {
        cal = retrieveCalendar(stored.getCalendarName());
        if (cal == null) {
          continue;
        }
      }
      Instant prevFireTime = trigger.getPreviousFireTime();
      stored.triggered(cal);
      trigger.triggered(cal);
      replaceTriggerDoc(stored, STATE_WAITING);
      JobDetail job = retrieveJob(stored.getJobKey());
      if (job == null) {
        continue;
      }
      if (job.isConcurrentExecutionDisallowed()) {
        jobs.updateOne(jobFilter(job.getKey()), Updates.set("blocked", true));
        for (OperableTrigger other : getTriggersForJob(job.getKey())) {
          Document otherDoc = triggerDoc(other.getKey());
          if (otherDoc == null) {
            continue;
          }
          int st = otherDoc.getInteger("state", STATE_WAITING);
          OperableTrigger ot = deserialize(otherDoc.get("payload", Binary.class));
          if (st == STATE_WAITING) {
            replaceTriggerDoc(ot, STATE_BLOCKED);
          } else if (st == STATE_PAUSED) {
            replaceTriggerDoc(ot, STATE_PAUSED_BLOCKED);
          }
        }
      }
      results.add(
          new TriggerFiredResult(
              new TriggerFiredBundle(
                  job,
                  trigger,
                  cal,
                  false,
                  Instant.now(),
                  trigger.getPreviousFireTime(),
                  prevFireTime,
                  trigger.getNextFireTime())));
    }
    return results;
  }

  private void triggeredJobCompleteLocked(
      OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction code)
      throws JobPersistenceException {
    Document jobDoc = jobDoc(jobDetail.getKey());
    if (jobDoc != null) {
      JobDetail jd = deserialize(jobDoc.get("payload", Binary.class));
      if (jd.isPersistJobDataAfterExecution()) {
        JobDataMap newData = jobDetail.getJobDataMap();
        if (newData != null) {
          newData = (JobDataMap) newData.clone();
          newData.clearDirtyFlag();
        }
        jd = jd.getJobBuilder().setJobData(newData).build();
        storeJobLocked(jd, true);
      }
      if (jd.isConcurrentExecutionDisallowed()) {
        jobs.updateOne(jobFilter(jd.getKey()), Updates.set("blocked", false));
        for (OperableTrigger other : getTriggersForJob(jd.getKey())) {
          Document otherDoc = triggerDoc(other.getKey());
          if (otherDoc == null) {
            continue;
          }
          int st = otherDoc.getInteger("state", STATE_WAITING);
          OperableTrigger ot = deserialize(otherDoc.get("payload", Binary.class));
          if (st == STATE_BLOCKED) {
            replaceTriggerDoc(ot, STATE_WAITING);
          } else if (st == STATE_PAUSED_BLOCKED) {
            replaceTriggerDoc(ot, STATE_PAUSED);
          }
        }
        if (signaler != null) {
          signaler.signalSchedulingChange(0L);
        }
      }
    } else {
      jobs.updateOne(jobFilter(jobDetail.getKey()), Updates.set("blocked", false));
    }

    Document triggerDoc = triggerDoc(trigger.getKey());
    if (triggerDoc == null) {
      return;
    }
    OperableTrigger stored = deserialize(triggerDoc.get("payload", Binary.class));
    if (code == CompletedExecutionInstruction.DELETE_TRIGGER) {
      if (trigger.getNextFireTime() == null) {
        if (stored.getNextFireTime() == null) {
          removeTriggerLocked(trigger.getKey(), true);
        }
      } else {
        removeTriggerLocked(trigger.getKey(), true);
        if (signaler != null) {
          signaler.signalSchedulingChange(0L);
        }
      }
    } else if (code == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
      replaceTriggerDoc(stored, STATE_COMPLETE);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    } else if (code == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
      log.info("Trigger {} set to ERROR state.", trigger.getKey());
      replaceTriggerDoc(stored, STATE_ERROR);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    } else if (code == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
      log.info("All triggers of Job {} set to ERROR state.", trigger.getJobKey());
      setAllTriggersOfJobToState(trigger.getJobKey(), STATE_ERROR);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    } else if (code == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
      setAllTriggersOfJobToState(trigger.getJobKey(), STATE_COMPLETE);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    }
  }

  private boolean applyMisfire(OperableTrigger trigger, int state) {
    long misfireTime = System.currentTimeMillis();
    if (misfireThreshold > 0) {
      misfireTime -= misfireThreshold;
    }
    Instant tnft = trigger.getNextFireTime();
    if (tnft == null
        || tnft.toEpochMilli() > misfireTime
        || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
      return false;
    }
    Calendar cal =
        trigger.getCalendarName() == null ? null : retrieveCalendar(trigger.getCalendarName());
    if (signaler != null) {
      signaler.notifyTriggerListenersMisfired((OperableTrigger) trigger.clone());
    }
    trigger.updateAfterMisfire(cal);
    if (trigger.getNextFireTime() == null) {
      replaceTriggerDoc(trigger, STATE_COMPLETE);
      if (signaler != null) {
        signaler.notifySchedulerListenersFinalized(trigger);
      }
      return true;
    }
    replaceTriggerDoc(trigger, state == STATE_ACQUIRED ? STATE_WAITING : state);
    return !tnft.equals(trigger.getNextFireTime());
  }

  private void setAllTriggersOfJobToState(JobKey jobKey, int state) {
    for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
      replaceTriggerDoc(trigger, state);
    }
  }

  private void recoverAcquiredTriggers() {
    try {
      withLock(
          () ->
              triggers.updateMany(
                  Filters.and(
                      Filters.eq("schedName", instanceName),
                      Filters.eq("state", STATE_ACQUIRED),
                      clustered
                          ? Filters.eq("acquiredBy", instanceId)
                          : Filters.exists("schedName")),
                  Updates.combine(
                      Updates.set("state", STATE_WAITING), Updates.unset("acquiredBy"))));
    } catch (JobPersistenceException e) {
      log.error("Failed recovering acquired triggers", e);
    }
  }

  private Document nextWaiting(long batchEnd, Set<TriggerKey> skipped) {
    List<Bson> parts = new ArrayList<>();
    parts.add(Filters.eq("schedName", instanceName));
    parts.add(Filters.eq("state", STATE_WAITING));
    parts.add(Filters.ne("nextFireTime", null));
    parts.add(Filters.lte("nextFireTime", batchEnd));
    if (!skipped.isEmpty()) {
      List<Bson> nor = new ArrayList<>();
      for (TriggerKey key : skipped) {
        nor.add(
            Filters.and(Filters.eq("name", key.getName()), Filters.eq("group", key.getGroup())));
      }
      parts.add(Filters.nor(nor));
    }
    return triggers
        .find(Filters.and(parts))
        .sort(Sorts.orderBy(Sorts.ascending("nextFireTime"), Sorts.descending("priority")))
        .first();
  }

  private void insertTrigger(OperableTrigger trigger, int state)
      throws ObjectAlreadyExistsException {
    Instant nft = trigger.getNextFireTime();
    Document doc =
        new Document("schedName", instanceName)
            .append("name", trigger.getKey().getName())
            .append("group", trigger.getKey().getGroup())
            .append("jobName", trigger.getJobKey().getName())
            .append("jobGroup", trigger.getJobKey().getGroup())
            .append("state", state)
            .append("nextFireTime", nft == null ? null : nft.toEpochMilli())
            .append("priority", trigger.getPriority())
            .append("calendarName", trigger.getCalendarName())
            .append("payload", new Binary(serialize(trigger)));
    try {
      triggers.insertOne(doc);
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
        throw new ObjectAlreadyExistsException(trigger);
      }
      throw e;
    }
  }

  private void replaceTriggerDoc(OperableTrigger trigger, int state) {
    Instant nft = trigger.getNextFireTime();
    triggers.replaceOne(
        triggerFilter(trigger.getKey()),
        new Document("schedName", instanceName)
            .append("name", trigger.getKey().getName())
            .append("group", trigger.getKey().getGroup())
            .append("jobName", trigger.getJobKey().getName())
            .append("jobGroup", trigger.getJobKey().getGroup())
            .append("state", state)
            .append("nextFireTime", nft == null ? null : nft.toEpochMilli())
            .append("priority", trigger.getPriority())
            .append("calendarName", trigger.getCalendarName())
            .append("payload", new Binary(serialize(trigger))),
        new ReplaceOptions().upsert(true));
  }

  private Document jobDoc(JobKey key) {
    return jobs.find(jobFilter(key)).first();
  }

  private Document triggerDoc(TriggerKey key) {
    return triggers.find(triggerFilter(key)).first();
  }

  private Bson jobFilter(JobKey key) {
    return Filters.and(
        Filters.eq("schedName", instanceName),
        Filters.eq("name", key.getName()),
        Filters.eq("group", key.getGroup()));
  }

  private Bson triggerFilter(TriggerKey key) {
    return Filters.and(
        Filters.eq("schedName", instanceName),
        Filters.eq("name", key.getName()),
        Filters.eq("group", key.getGroup()));
  }

  private Bson andSched(Bson group) {
    return Filters.and(Filters.eq("schedName", instanceName), group);
  }

  private static Bson groupFilter(GroupMatcher<?> matcher) {
    StringOperatorName op = matcher.getCompareWithOperator();
    String value = matcher.getCompareToValue();
    return switch (op) {
      case EQUALS -> Filters.eq("group", value);
      case STARTS_WITH -> Filters.regex("group", "^" + Pattern.quote(value));
      case ENDS_WITH -> Filters.regex("group", Pattern.quote(value) + "$");
      case CONTAINS -> Filters.regex("group", Pattern.quote(value));
      case ANYTHING -> Filters.exists("group");
    };
  }

  private boolean pausedTriggerGroup(String group) {
    return pausedTriggerGroupsCol
            .find(Filters.and(Filters.eq("schedName", instanceName), Filters.eq("group", group)))
            .first()
        != null;
  }

  private boolean pausedJobGroup(String group) {
    return pausedJobGroupsCol
            .find(Filters.and(Filters.eq("schedName", instanceName), Filters.eq("group", group)))
            .first()
        != null;
  }

  private List<String> rememberPausedGroups(
      MongoCollection<Document> col, GroupMatcher<?> matcher, List<String> known) {
    List<String> paused = new ArrayList<>();
    StringOperatorName op = matcher.getCompareWithOperator();
    String value = matcher.getCompareToValue();
    if (op == StringOperatorName.EQUALS) {
      if (insertPaused(col, value)) {
        paused.add(value);
      }
    } else if (op == StringOperatorName.ANYTHING) {
      for (String group : known) {
        if (insertPaused(col, group)) {
          paused.add(group);
        }
      }
    } else {
      for (String group : known) {
        if (op.evaluate(group, value) && insertPaused(col, group)) {
          paused.add(group);
        }
      }
    }
    return paused;
  }

  private boolean insertPaused(MongoCollection<Document> col, String group) {
    if (col.find(Filters.and(Filters.eq("schedName", instanceName), Filters.eq("group", group)))
            .first()
        != null) {
      return false;
    }
    col.insertOne(new Document("schedName", instanceName).append("group", group));
    return true;
  }

  private void clearPausedGroups(MongoCollection<Document> col, GroupMatcher<?> matcher) {
    col.deleteMany(andSched(groupFilter(matcher)));
  }

  private static TriggerState toPublicState(int state) {
    return switch (state) {
      case STATE_COMPLETE -> TriggerState.COMPLETE;
      case STATE_PAUSED, STATE_PAUSED_BLOCKED -> TriggerState.PAUSED;
      case STATE_BLOCKED -> TriggerState.BLOCKED;
      case STATE_ERROR -> TriggerState.ERROR;
      default -> TriggerState.NORMAL;
    };
  }

  private void withLock(PersistedOp op) throws JobPersistenceException {
    localLock.lock();
    try {
      boolean clusterLock = clustered;
      if (clusterLock && !obtainLock()) {
        throw new JobPersistenceException(
            "Could not obtain MongoDB cluster lock for scheduler '" + instanceName + "'");
      }
      try {
        op.run();
      } catch (JobPersistenceException e) {
        throw e;
      } catch (RuntimeException e) {
        if (isShutdownRace(e)) {
          throw new JobPersistenceException("MongoDB unavailable during scheduler shutdown", e);
        }
        throw new JobPersistenceException(e.getMessage(), e);
      } finally {
        if (clusterLock) {
          releaseLock();
        }
      }
    } finally {
      localLock.unlock();
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
    try {
      locks.updateOne(
          Filters.and(
              Filters.eq("schedName", instanceName),
              Filters.eq("lockName", "TRIGGER_ACCESS"),
              Filters.eq("owner", instanceId)),
          Updates.unset("owner"));
    } catch (RuntimeException e) {
      if (!isShutdownRace(e)) {
        log.warn("Failed to release MongoDB cluster lock for scheduler '{}'", instanceName, e);
      }
    }
  }

  private boolean isShutdownRace(Throwable error) {
    if (!started.get() || Thread.currentThread().isInterrupted()) {
      return true;
    }
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof InterruptedException) {
        return true;
      }
      String name = t.getClass().getName();
      if (name.contains("MongoInterrupted") || name.contains("MongoClientClosed")) {
        return true;
      }
      String message = t.getMessage();
      if (message != null && message.contains("state should be: open")) {
        return true;
      }
    }
    return false;
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
        } catch (InterruptedException e) {
          interrupt();
          break;
        } catch (RuntimeException e) {
          log.error("Cluster check-in failed", e);
        }
      }
    }
  }
}
