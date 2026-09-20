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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
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
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

/**
 * Persistent, cluster-capable {@link JobStore} backed by MongoDB.
 *
 * <p>Jobs and triggers are BSON documents ({@code jobClass} is a string FQCN, fire times are {@link
 * Instant}). Cluster identity is a per-process lease with TTL, not a durable instance id.
 *
 * <p>Inject an existing {@link MongoClient} (Spring Boot) via {@link #setMongoClient(MongoClient)}
 * and this store will not close it on shutdown. Otherwise it creates a client from {@code
 * mongoUri}.
 */
@Slf4j
@Getter
public class MongoJobStore implements JobStore {

  public static final String DEFAULT_URI = "mongodb://localhost:27017";
  public static final String DEFAULT_DB = "quartz";
  public static final String DEFAULT_COLLECTION_PREFIX = "qrtz_";

  private static final AtomicLong FIRE_IDS = new AtomicLong(System.currentTimeMillis());
  private static final String TRIGGER_ACCESS = "TRIGGER_ACCESS";
  private static final String LOCK_FAILURE_MESSAGE = "Could not obtain MongoDB cluster lock";

  private final ReentrantLock localLock = new ReentrantLock(true);
  private final AtomicBoolean started = new AtomicBoolean();
  private int clusterLockDepth;
  private Thread lockHeartbeat;

  @Setter private String mongoUri = DEFAULT_URI;
  @Setter private String dbName = DEFAULT_DB;
  @Setter private String collectionPrefix = DEFAULT_COLLECTION_PREFIX;
  private boolean clustered;
  private Duration clusterCheckinInterval = Duration.ofSeconds(15);
  private Duration clusterLockWait;
  private Duration misfireThreshold = Duration.ofSeconds(60);
  private String instanceId = "NON_CLUSTERED";
  private String instanceName = "QuartzScheduler";
  private boolean ownsClient;
  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private MongoCollection<Document> jobs;
  private MongoCollection<Document> triggers;
  private MongoCollection<Document> calendars;
  private MongoCollection<Document> pausedTriggerGroupsCol;
  private MongoCollection<Document> pausedJobGroupsCol;
  private MongoCollection<Document> leases;
  private MongoCollection<Document> locks;
  private SchedulerSignaler signaler;
  private ClassLoader jobClassLoader;
  private ClusterManager clusterManager;

  public void setMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
    this.ownsClient = false;
  }

  public void setMongoDatabase(MongoDatabase mongoDatabase) {
    this.mongoDatabase = mongoDatabase;
    this.ownsClient = false;
  }

  public void setIsClustered(boolean clustered) {
    this.clustered = clustered;
  }

  public void setClustered(boolean clustered) {
    this.clustered = clustered;
  }

  public void setClusterCheckinInterval(Duration clusterCheckinInterval) {
    if (clusterCheckinInterval == null || clusterCheckinInterval.isNegative()) {
      throw new IllegalArgumentException("clusterCheckinInterval must be >= 0");
    }
    this.clusterCheckinInterval = clusterCheckinInterval;
  }

  /**
   * How long {@code withLock} waits for {@code TRIGGER_ACCESS}. Default is the lock expiry window
   * (at least 30s) so a live node can wait out another node's critical section or steal an expired
   * lock. Tests may shorten this.
   */
  public void setClusterLockWait(Duration clusterLockWait) {
    if (clusterLockWait != null && clusterLockWait.isNegative()) {
      throw new IllegalArgumentException("clusterLockWait must be >= 0");
    }
    this.clusterLockWait = clusterLockWait;
  }

  public void setMisfireThreshold(Duration misfireThreshold) {
    if (misfireThreshold == null || misfireThreshold.isZero() || misfireThreshold.isNegative()) {
      throw new IllegalArgumentException("misfireThreshold must be > 0");
    }
    this.misfireThreshold = misfireThreshold;
  }

  public void setJobClassLoader(ClassLoader jobClassLoader) {
    this.jobClassLoader = jobClassLoader;
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
    // unused — JobStore SPI
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
  public Duration getEstimatedTimeToReleaseAndAcquireTrigger() {
    return Duration.ofMillis(70);
  }

  @Override
  public Duration getAcquireRetryDelay(int failureCount) {
    return clustered ? Duration.ofSeconds(1) : Duration.ofMillis(20);
  }

  @Override
  public void initialize(SchedulerSignaler schedSignaler) throws SchedulerConfigException {
    this.signaler = schedSignaler;
    if (instanceId == null || instanceId.isBlank() || "AUTO".equals(instanceId)) {
      this.instanceId = UUID.randomUUID().toString();
    }
    MongoDatabase database = mongoDatabase;
    if (database == null) {
      if (mongoClient == null) {
        if (mongoUri == null || mongoUri.isBlank()) {
          throw new SchedulerConfigException(
              "mongoUri is required unless a MongoClient or MongoDatabase is injected");
        }
        mongoClient = MongoClients.create(mongoUri);
        ownsClient = true;
      }
      database = mongoClient.getDatabase(dbName);
    } else {
      dbName = database.getName();
    }
    jobs = database.getCollection(collectionPrefix + "jobs");
    triggers = database.getCollection(collectionPrefix + "triggers");
    calendars = database.getCollection(collectionPrefix + "calendars");
    pausedTriggerGroupsCol = database.getCollection(collectionPrefix + "paused_trigger_groups");
    pausedJobGroupsCol = database.getCollection(collectionPrefix + "paused_job_groups");
    leases = database.getCollection(collectionPrefix + "leases");
    locks = database.getCollection(collectionPrefix + "locks");
    ensureUniqueIndex(jobs, new Document("schedName", 1).append("name", 1).append("group", 1));
    ensureUniqueIndex(triggers, new Document("schedName", 1).append("name", 1).append("group", 1));
    triggers.createIndex(new Document("schedName", 1).append("state", 1).append("nextFireTime", 1));
    triggers.createIndex(new Document("schedName", 1).append("jobName", 1).append("jobGroup", 1));
    ensureUniqueIndex(leases, new Document("schedName", 1).append("owner", 1));
    try {
      leases.createIndex(
          new Document("expiresAt", 1),
          new IndexOptions().expireAfter(0L, java.util.concurrent.TimeUnit.SECONDS));
    } catch (RuntimeException ignored) {
      // TTL index is optional; recovery still uses leaseExpiresAt on triggers.
    }
    collapseDuplicateLocks();
    ensureUniqueIndex(locks, new Document("schedName", 1).append("lockName", 1));
    log.info("MongoJobStore initialized on db '{}' (clustered={})", dbName, clustered);
  }

  private void ensureUniqueIndex(MongoCollection<Document> collection, Document keys) {
    try {
      collection.createIndex(keys, new IndexOptions().unique(true));
    } catch (RuntimeException e) {
      if (!isDuplicateKey(e) || collection != locks) {
        throw e;
      }
      collapseDuplicateLocks();
      collection.createIndex(keys, new IndexOptions().unique(true));
    }
  }

  /**
   * Older builds could insert a second {@code TRIGGER_ACCESS} row when the unique index was not
   * present yet. Mongo cannot create that index until extras are gone.
   */
  private void collapseDuplicateLocks() {
    Set<String> seen = new HashSet<>();
    List<Object> extraIds = new ArrayList<>();
    for (Document doc : locks.find()) {
      String key = doc.getString("schedName") + '\0' + doc.getString("lockName");
      if (!seen.add(key)) {
        extraIds.add(doc.get("_id"));
      }
    }
    if (extraIds.isEmpty()) {
      return;
    }
    locks.deleteMany(Filters.in("_id", extraIds));
    log.warn("Removed {} duplicate Quartz lock document(s) before unique index", extraIds.size());
  }

  @Override
  public void schedulerStarted() {
    started.set(true);
    renewLease();
    recoverAcquiredTriggers();
    recoverErrorTriggersIfJobLoads();
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
    stopLockHeartbeat();
    if (clusterManager != null) {
      clusterManager.interrupt();
    }
    try {
      recoverAcquiredOwnedBy(instanceId);
      if (locks != null) {
        locks.deleteMany(
            Filters.and(Filters.eq("schedName", instanceName), Filters.eq("owner", instanceId)));
      }
      if (leases != null) {
        leases.deleteOne(
            Filters.and(Filters.eq("schedName", instanceName), Filters.eq("owner", instanceId)));
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
    if (doc == null) {
      return null;
    }
    try {
      return jobFrom(doc);
    } catch (RuntimeException e) {
      throw new JobPersistenceException("Unable to read job '" + jobKey + "'", e);
    }
  }

  @Override
  public List<JobDetail> getJobDetails(GroupMatcher<JobKey> matcher)
      throws JobPersistenceException {
    List<JobDetail> out = new ArrayList<>();
    for (Document d : jobs.find(andSched(groupFilter(matcher)))) {
      try {
        out.add(jobFrom(d));
      } catch (RuntimeException e) {
        log.warn(
            "Skipping job '{}.{}' that cannot be loaded: {}",
            d.getString("group"),
            d.getString("name"),
            e.getMessage());
      }
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
          OperableTrigger old = triggerFrom(existing);
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
    try {
      OperableTrigger trigger = triggerFrom(doc);
      return (OperableTrigger) trigger.clone();
    } catch (RuntimeException e) {
      throw new JobPersistenceException("Unable to read trigger '" + triggerKey + "'", e);
    }
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
                  .append("calendar", BsonJobStoreCodec.calendarBody(stored)),
              new ReplaceOptions().upsert(true));
          if (existing != null && updateTriggers) {
            for (Document d :
                triggers.find(
                    Filters.and(
                        Filters.eq("schedName", instanceName), Filters.eq("calendarName", name)))) {
              OperableTrigger trigger = triggerFrom(d);
              trigger.updateWithNewCalendar(stored, misfireThreshold);
              replaceTriggerDoc(trigger, storedState(d));
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
  public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
    Document doc =
        calendars
            .find(Filters.and(Filters.eq("schedName", instanceName), Filters.eq("name", calName)))
            .first();
    if (doc == null) {
      return null;
    }
    try {
      Calendar cal = BsonJobStoreCodec.toCalendar(doc.get("calendar", Document.class));
      return cal == null ? null : (Calendar) cal.clone();
    } catch (RuntimeException e) {
      throw new JobPersistenceException("Unable to read calendar '" + calName + "'", e);
    }
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
      OperableTrigger t = triggerFrom(d);
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
        OperableTrigger t = triggerFrom(d);
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
    return toPublicState(storedState(doc));
  }

  @Override
  public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
    withLock(
        () -> {
          Document doc = triggerDoc(triggerKey);
          if (doc == null || storedState(doc) != TriggerState.ERROR) {
            return;
          }
          OperableTrigger trigger = triggerFrom(doc);
          TriggerState state =
              pausedTriggerGroup(triggerKey.getGroup())
                  ? TriggerState.PAUSED
                  : TriggerState.WAITING;
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
          if (doc != null && acquiredByThisInstance(doc)) {
            OperableTrigger stored = triggerFrom(doc);
            replaceTriggerDoc(stored, TriggerState.WAITING);
          }
        },
        "release " + trigger.getKey());
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
    withLockUnchecked(
        () -> triggeredJobCompleteLocked(trigger, jobDetail, triggerInstCode),
        "complete " + trigger.getKey() + " job " + jobDetail.getKey());
  }

  private void storeJobLocked(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException {
    JobDetail stored = (JobDetail) newJob.clone();
    Document existing = jobDoc(stored.getKey());
    if (existing != null && !replaceExisting) {
      throw new ObjectAlreadyExistsException(newJob);
    }
    Document doc = BsonJobStoreCodec.jobBody(stored);
    doc.append("schedName", instanceName)
        .append("durable", stored.isDurable())
        .append("requestsRecovery", stored.requestsRecovery())
        .append("concurrentDisallowed", stored.isConcurrentExecutionDisallowed())
        .append("persistJobData", stored.isPersistJobDataAfterExecution())
        .append("blocked", existing != null && Boolean.TRUE.equals(existing.getBoolean("blocked")));
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
    TriggerState state = TriggerState.WAITING;
    if (pausedTriggerGroup(stored.getKey().getGroup())
        || pausedJobGroup(stored.getJobKey().getGroup())) {
      state = TriggerState.PAUSED;
      Document job = jobDoc(stored.getJobKey());
      if (job != null && Boolean.TRUE.equals(job.getBoolean("blocked"))) {
        state = TriggerState.PAUSED_BLOCKED;
      }
    } else {
      Document job = jobDoc(stored.getJobKey());
      if (job != null && Boolean.TRUE.equals(job.getBoolean("blocked"))) {
        state = TriggerState.BLOCKED;
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
    TriggerState state = storedState(doc);
    if (state == TriggerState.COMPLETE) {
      return;
    }
    OperableTrigger trigger = triggerFrom(doc);
    replaceTriggerDoc(
        trigger, state == TriggerState.BLOCKED ? TriggerState.PAUSED_BLOCKED : TriggerState.PAUSED);
  }

  private void resumeTriggerLocked(TriggerKey triggerKey) throws JobPersistenceException {
    Document doc = triggerDoc(triggerKey);
    if (doc == null) {
      return;
    }
    TriggerState state = storedState(doc);
    if (state != TriggerState.PAUSED && state != TriggerState.PAUSED_BLOCKED) {
      return;
    }
    OperableTrigger trigger = triggerFrom(doc);
    Document job = jobDoc(trigger.getJobKey());
    TriggerState next =
        job != null && Boolean.TRUE.equals(job.getBoolean("blocked"))
            ? TriggerState.BLOCKED
            : TriggerState.WAITING;
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
      TriggerKey key = new TriggerKey(doc.getString("name"), doc.getString("group"));
      OperableTrigger trigger;
      try {
        trigger = triggerFrom(doc);
      } catch (RuntimeException e) {
        log.error("Failed to read trigger '{}', moving to ERROR", key, e);
        markTriggerError(key);
        skipped.add(key);
        continue;
      }
      TriggerState state = storedState(doc);
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
      JobDetail job;
      try {
        job = retrieveJob(trigger.getJobKey());
      } catch (JobPersistenceException e) {
        log.error(
            "Failed to load job '{}' for trigger '{}'; skipping until the class is available",
            trigger.getJobKey(),
            key,
            e);
        skipped.add(key);
        continue;
      }
      if (job == null) {
        skipped.add(trigger.getKey());
        continue;
      }
      Document jobRow = jobDoc(trigger.getJobKey());
      if (jobRow != null && Boolean.TRUE.equals(jobRow.getBoolean("blocked"))) {
        skipped.add(trigger.getKey());
        continue;
      }
      if (job.isConcurrentExecutionDisallowed() && acquiredJobs.contains(job.getKey())) {
        skipped.add(trigger.getKey());
        continue;
      }
      trigger.setFireInstanceId(String.valueOf(FIRE_IDS.incrementAndGet()));
      Instant leaseUntil = leaseExpiry();
      Document claimed =
          triggers.findOneAndUpdate(
              Filters.and(triggerFilter(trigger.getKey()), stateEq(TriggerState.WAITING)),
              Updates.combine(
                  Updates.set("state", TriggerState.ACQUIRED.name()),
                  Updates.set("leaseOwner", instanceId),
                  Updates.set("leaseExpiresAt", BsonJobStoreCodec.instant(leaseUntil)),
                  Updates.set("fireInstanceId", trigger.getFireInstanceId()),
                  Updates.set(
                      "nextFireTime", BsonJobStoreCodec.instant(trigger.getNextFireTime()))),
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
      if (doc == null || !acquiredByThisInstance(doc)) {
        results.add(new TriggerFiredResult((TriggerFiredBundle) null));
        continue;
      }
      OperableTrigger stored = triggerFrom(doc);
      Calendar cal = null;
      if (stored.getCalendarName() != null) {
        cal = retrieveCalendar(stored.getCalendarName());
        if (cal == null) {
          log.error(
              "Calendar '{}' missing for trigger '{}'; moving to ERROR",
              stored.getCalendarName(),
              trigger.getKey());
          replaceTriggerDoc(stored, TriggerState.ERROR);
          results.add(new TriggerFiredResult((TriggerFiredBundle) null));
          continue;
        }
      }
      Instant prevFireTime = trigger.getPreviousFireTime();
      stored.triggered(cal);
      trigger.triggered(cal);
      replaceTriggerDoc(stored, TriggerState.WAITING);
      JobDetail job = retrieveJob(stored.getJobKey());
      if (job == null) {
        results.add(new TriggerFiredResult((TriggerFiredBundle) null));
        continue;
      }
      if (job.isConcurrentExecutionDisallowed()) {
        jobs.updateOne(
            jobFilter(job.getKey()),
            Updates.combine(Updates.set("blocked", true), Updates.set("blockedBy", instanceId)));
        for (OperableTrigger other : getTriggersForJob(job.getKey())) {
          Document otherDoc = triggerDoc(other.getKey());
          if (otherDoc == null) {
            continue;
          }
          TriggerState st = storedState(otherDoc);
          OperableTrigger ot = triggerFrom(otherDoc);
          if (st == TriggerState.WAITING) {
            replaceTriggerDoc(ot, TriggerState.BLOCKED);
          } else if (st == TriggerState.PAUSED) {
            replaceTriggerDoc(ot, TriggerState.PAUSED_BLOCKED);
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
    if (jobDoc != null
        && Boolean.TRUE.equals(jobDoc.getBoolean("blocked"))
        && !instanceId.equals(jobDoc.getString("blockedBy"))
        && jobDoc.getString("blockedBy") != null) {
      log.warn(
          "Ignoring job complete from instance '{}' for job {} owned by '{}'",
          instanceId,
          jobDetail.getKey(),
          jobDoc.getString("blockedBy"));
      return;
    }
    if (jobDoc != null) {
      JobDetail jd = jobFrom(jobDoc);
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
        jobs.updateOne(
            jobFilter(jd.getKey()),
            Updates.combine(Updates.set("blocked", false), Updates.unset("blockedBy")));
        for (OperableTrigger other : getTriggersForJob(jd.getKey())) {
          Document otherDoc = triggerDoc(other.getKey());
          if (otherDoc == null) {
            continue;
          }
          TriggerState st = storedState(otherDoc);
          OperableTrigger ot = triggerFrom(otherDoc);
          if (st == TriggerState.BLOCKED) {
            replaceTriggerDoc(ot, TriggerState.WAITING);
          } else if (st == TriggerState.PAUSED_BLOCKED) {
            replaceTriggerDoc(ot, TriggerState.PAUSED);
          }
        }
        if (signaler != null) {
          signaler.signalSchedulingChange(0L);
        }
      }
    } else {
      jobs.updateOne(
          jobFilter(jobDetail.getKey()),
          Updates.combine(Updates.set("blocked", false), Updates.unset("blockedBy")));
    }

    Document triggerDoc = triggerDoc(trigger.getKey());
    if (triggerDoc == null) {
      return;
    }
    OperableTrigger stored = triggerFrom(triggerDoc);
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
      replaceTriggerDoc(stored, TriggerState.COMPLETE);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    } else if (code == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
      log.info("Trigger {} set to ERROR state.", trigger.getKey());
      replaceTriggerDoc(stored, TriggerState.ERROR);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    } else if (code == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
      log.info("All triggers of Job {} set to ERROR state.", trigger.getJobKey());
      setAllTriggersOfJobToState(trigger.getJobKey(), TriggerState.ERROR);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    } else if (code == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
      setAllTriggersOfJobToState(trigger.getJobKey(), TriggerState.COMPLETE);
      if (signaler != null) {
        signaler.signalSchedulingChange(0L);
      }
    }
  }

  private boolean applyMisfire(OperableTrigger trigger, TriggerState state)
      throws JobPersistenceException {
    long misfireTime = System.currentTimeMillis();
    if (!misfireThreshold.isZero()) {
      misfireTime -= misfireThreshold.toMillis();
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
      replaceTriggerDoc(trigger, TriggerState.COMPLETE);
      if (signaler != null) {
        signaler.notifySchedulerListenersFinalized(trigger);
      }
      return true;
    }
    replaceTriggerDoc(trigger, state == TriggerState.ACQUIRED ? TriggerState.WAITING : state);
    return !tnft.equals(trigger.getNextFireTime());
  }

  private void setAllTriggersOfJobToState(JobKey jobKey, TriggerState state) {
    for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
      replaceTriggerDoc(trigger, state);
    }
  }

  private void recoverAcquiredTriggers() {
    try {
      withLock(
          () -> {
            if (clustered) {
              recoverExpiredAcquisitions();
            } else {
              recoverAcquiredOwnedBy(instanceId);
            }
            recoverOrphanedBlockedJobs(true);
          });
    } catch (JobPersistenceException e) {
      log.error("Failed recovering acquired triggers", e);
    }
  }

  private void recoverAcquiredOwnedBy(String ownerInstanceId) {
    Bson owner =
        ownerInstanceId == null
            ? Filters.exists("schedName")
            : Filters.eq("leaseOwner", ownerInstanceId);
    triggers.updateMany(
        Filters.and(Filters.eq("schedName", instanceName), stateEq(TriggerState.ACQUIRED), owner),
        Updates.combine(
            Updates.set("state", TriggerState.WAITING.name()),
            Updates.unset("leaseOwner"),
            Updates.unset("leaseExpiresAt")));
  }

  /** Reset ACQUIRED triggers whose lease has expired. Does not look up a recycled instance id. */
  private void recoverExpiredAcquisitions() {
    Instant now = Instant.now();
    triggers.updateMany(
        Filters.and(
            Filters.eq("schedName", instanceName),
            stateEq(TriggerState.ACQUIRED),
            Filters.or(
                Filters.lte("leaseExpiresAt", BsonJobStoreCodec.instant(now)),
                Filters.exists("leaseExpiresAt", false),
                Filters.eq("leaseExpiresAt", null))),
        Updates.combine(
            Updates.set("state", TriggerState.WAITING.name()),
            Updates.unset("leaseOwner"),
            Updates.unset("leaseExpiresAt")));
  }

  private void recoverFailedInstances() {
    try {
      withLock(
          () -> {
            recoverExpiredAcquisitions();
            recoverOrphanedBlockedJobs(false);
          });
    } catch (JobPersistenceException e) {
      log.error("Failed recovering acquired or blocked triggers", e);
    }
    recoverErrorTriggersIfJobLoads();
  }

  /**
   * A {@code blocked} flag is orphaned only once the instance that set it is gone, so an execution
   * in flight elsewhere must not be unblocked. Triggers of a running job are BLOCKED rather than
   * ACQUIRED, so the owning instance lease is the only reliable liveness signal; {@code
   * hasLiveAcquisition} is the fallback for documents written before {@code blockedBy} existed.
   *
   * @param startup treats this instance's own id as gone, since nothing it started is running yet
   */
  private void recoverOrphanedBlockedJobs(boolean startup) {
    for (Document job :
        jobs.find(
            Filters.and(Filters.eq("schedName", instanceName), Filters.eq("blocked", true)))) {
      JobKey key = new JobKey(job.getString("name"), job.getString("group"));
      String owner = job.getString("blockedBy");
      if (owner == null ? hasLiveAcquisition(key) : isBlockOwnerLive(owner, startup)) {
        continue;
      }
      jobs.updateOne(
          jobFilter(key),
          Updates.combine(Updates.set("blocked", false), Updates.unset("blockedBy")));
      for (OperableTrigger other : getTriggersForJob(key)) {
        Document otherDoc = triggerDoc(other.getKey());
        if (otherDoc == null) {
          continue;
        }
        TriggerState st = storedState(otherDoc);
        OperableTrigger ot = triggerFrom(otherDoc);
        if (st == TriggerState.BLOCKED) {
          replaceTriggerDoc(ot, TriggerState.WAITING);
        } else if (st == TriggerState.PAUSED_BLOCKED) {
          replaceTriggerDoc(ot, TriggerState.PAUSED);
        }
      }
      log.warn("Cleared orphaned blocked flag for job {}", key);
    }
  }

  private boolean isBlockOwnerLive(String owner, boolean startup) {
    if (instanceId.equals(owner)) {
      return !startup;
    }
    if (leases == null) {
      return false;
    }
    Document lease =
        leases
            .find(Filters.and(Filters.eq("schedName", instanceName), Filters.eq("owner", owner)))
            .first();
    if (lease == null) {
      return false;
    }
    Instant expires;
    try {
      expires = BsonJobStoreCodec.toInstant(lease.get("expiresAt"));
    } catch (RuntimeException e) {
      log.warn("Unreadable lease expiry for instance '{}'; treating it as live", owner, e);
      return true;
    }
    return expires == null || !expires.isBefore(Instant.now());
  }

  private boolean hasLiveAcquisition(JobKey jobKey) {
    Instant now = Instant.now();
    for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
      Document doc = triggerDoc(trigger.getKey());
      if (doc == null || storedState(doc) != TriggerState.ACQUIRED) {
        continue;
      }
      Instant expires;
      try {
        expires = BsonJobStoreCodec.toInstant(doc.get("leaseExpiresAt"));
      } catch (RuntimeException e) {
        log.warn(
            "Unreadable lease expiry on trigger '{}'; leaving job {} blocked",
            trigger.getKey(),
            jobKey,
            e);
        return true;
      }
      // No readable expiry means we cannot prove the job finished, so keep it blocked.
      if (expires == null || !expires.isBefore(now)) {
        return true;
      }
    }
    return false;
  }

  /**
   * ERROR is for unreadable trigger documents, not a restart. If the job class loads now (Spring
   * class loader is in place, deploy mismatch is gone), put the trigger back to WAITING.
   */
  private void recoverErrorTriggersIfJobLoads() {
    try {
      withLock(
          () -> {
            for (Document d :
                triggers.find(
                    Filters.and(
                        Filters.eq("schedName", instanceName), stateEq(TriggerState.ERROR)))) {
              TriggerKey key = new TriggerKey(d.getString("name"), d.getString("group"));
              try {
                JobDetail job =
                    retrieveJob(new JobKey(d.getString("jobName"), d.getString("jobGroup")));
                if (job == null || job.getJobClass() == null) {
                  continue;
                }
                OperableTrigger trigger = triggerFrom(d);
                TriggerState state =
                    pausedTriggerGroup(key.getGroup()) ? TriggerState.PAUSED : TriggerState.WAITING;
                replaceTriggerDoc(trigger, state);
                log.info("Reset trigger '{}' from ERROR after job class became loadable", key);
              } catch (RuntimeException | JobPersistenceException e) {
                log.warn("Leaving trigger '{}' in ERROR; job still cannot be loaded", key, e);
              }
            }
          });
    } catch (JobPersistenceException e) {
      log.error("Failed recovering ERROR triggers", e);
    }
  }

  private Document nextWaiting(long batchEnd, Set<TriggerKey> skipped) {
    List<Bson> parts = new ArrayList<>();
    parts.add(Filters.eq("schedName", instanceName));
    parts.add(stateEq(TriggerState.WAITING));
    parts.add(Filters.ne("nextFireTime", null));
    parts.add(
        Filters.lte("nextFireTime", BsonJobStoreCodec.instant(Instant.ofEpochMilli(batchEnd))));
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

  private void insertTrigger(OperableTrigger trigger, TriggerState state)
      throws ObjectAlreadyExistsException {
    Document doc = triggerDocument(trigger, state);
    try {
      triggers.insertOne(doc);
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
        throw new ObjectAlreadyExistsException(trigger);
      }
      throw e;
    }
  }

  private void markTriggerError(TriggerKey key) {
    triggers.updateOne(triggerFilter(key), Updates.set("state", TriggerState.ERROR.name()));
  }

  private void replaceTriggerDoc(OperableTrigger trigger, TriggerState state) {
    triggers.replaceOne(
        triggerFilter(trigger.getKey()),
        triggerDocument(trigger, state),
        new ReplaceOptions().upsert(true));
  }

  private Document triggerDocument(OperableTrigger trigger, TriggerState state) {
    Document doc = new Document("schedName", instanceName);
    BsonJobStoreCodec.putTriggerBody(doc, trigger);
    doc.append("state", state.name());
    if (state == TriggerState.ACQUIRED) {
      doc.append("leaseOwner", instanceId);
      doc.append("leaseExpiresAt", BsonJobStoreCodec.instant(leaseExpiry()));
    }
    return doc;
  }

  private static TriggerState storedState(Document doc) {
    String raw = doc.getString("state");
    if (raw == null || raw.isBlank()) {
      return TriggerState.WAITING;
    }
    return TriggerState.valueOf(raw);
  }

  private static Bson stateEq(TriggerState state) {
    return Filters.eq("state", state.name());
  }

  private boolean acquiredByThisInstance(Document doc) {
    return storedState(doc) == TriggerState.ACQUIRED
        && instanceId.equals(doc.getString("leaseOwner"));
  }

  private JobDetail jobFrom(Document doc) {
    return BsonJobStoreCodec.toJob(doc, jobClassLoader());
  }

  private OperableTrigger triggerFrom(Document doc) {
    return BsonJobStoreCodec.toTrigger(doc);
  }

  private ClassLoader jobClassLoader() {
    if (jobClassLoader != null) {
      return jobClassLoader;
    }
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    return tccl != null ? tccl : getClass().getClassLoader();
  }

  private Instant leaseExpiry() {
    Duration ttl = clusterCheckinInterval.multipliedBy(2);
    if (ttl.compareTo(Duration.ofSeconds(30)) < 0) {
      ttl = Duration.ofSeconds(30);
    }
    return Instant.now().plus(ttl);
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

  private static TriggerState toPublicState(TriggerState state) {
    return switch (state) {
      case COMPLETE -> TriggerState.COMPLETE;
      case PAUSED, PAUSED_BLOCKED -> TriggerState.PAUSED;
      case BLOCKED -> TriggerState.BLOCKED;
      case ERROR -> TriggerState.ERROR;
      default -> TriggerState.NORMAL;
    };
  }

  private void withLock(PersistedOp op) throws JobPersistenceException {
    localLock.lock();
    try {
      boolean clusterLock = clustered;
      if (clusterLock) {
        boolean entered;
        try {
          entered = enterClusterLock();
        } catch (RuntimeException e) {
          // Interrupting the scheduler thread makes the driver abandon the connection wait, so
          // this has to be classified like any other store failure rather than escape unwrapped.
          throw asPersistenceException(e);
        }
        if (!entered) {
          throw new JobPersistenceException(
              LOCK_FAILURE_MESSAGE + " for scheduler '" + instanceName + "'");
        }
      }
      try {
        op.run();
      } catch (JobPersistenceException e) {
        throw e;
      } catch (RuntimeException e) {
        throw asPersistenceException(e);
      } finally {
        if (clusterLock) {
          exitClusterLock();
        }
      }
    } finally {
      localLock.unlock();
    }
  }

  private JobPersistenceException asPersistenceException(RuntimeException e) {
    if (isShutdownRace(e)) {
      return new JobPersistenceException("MongoDB unavailable during scheduler shutdown", e);
    }
    return new JobPersistenceException(e.getMessage(), e);
  }

  private void withLockUnchecked(PersistedOp op) {
    withLockUnchecked(op, null);
  }

  private void withLockUnchecked(PersistedOp op, String context) {
    try {
      withLock(op);
    } catch (JobPersistenceException e) {
      String detail = context == null ? "" : " (" + context + ")";
      if (isShutdownRace(e)) {
        log.debug("Ignoring MongoDB access after scheduler halt{}", detail, e);
        return;
      }
      if (isClusterLockFailure(e)) {
        try {
          withLock(op);
          return;
        } catch (JobPersistenceException retry) {
          if (isShutdownRace(retry)) {
            log.debug("Ignoring MongoDB access after scheduler halt{}", detail, retry);
            return;
          }
          if (isClusterLockFailure(retry)) {
            log.error(
                "Could not obtain MongoDB cluster lock for scheduler '{}'; will retry later{}",
                instanceName,
                detail);
            return;
          }
          log.error("Job store operation failed for scheduler '{}'{}", instanceName, detail, retry);
          return;
        }
      }
      log.error("Job store operation failed for scheduler '{}'{}", instanceName, detail, e);
    }
  }

  /** Holds {@code TRIGGER_ACCESS} for {@code hold} so tests can observe lock heartbeat renewals. */
  void runLocked(Duration hold) throws JobPersistenceException {
    withLock(
        () -> {
          try {
            Thread.sleep(hold.toMillis());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JobPersistenceException("Interrupted while holding cluster lock", e);
          }
        });
  }

  private boolean obtainLock() {
    long deadlineNanos = System.nanoTime() + lockWait().toNanos();
    int attempt = 0;
    while (true) {
      if (tryObtainLock()) {
        return true;
      }
      long remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000L;
      if (remainingMs <= 0) {
        return false;
      }
      long backoffMs = Math.min(50L << Math.min(attempt, 3), 400L);
      try {
        Thread.sleep(Math.min(remainingMs, backoffMs));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      attempt++;
    }
  }

  private Duration lockWait() {
    if (clusterLockWait != null) {
      return clusterLockWait;
    }
    return Duration.ofMillis(Math.max(clusterCheckinInterval.toMillis() * 2, 30_000));
  }

  private boolean tryObtainLock() {
    Instant now = Instant.now();
    Instant expiry = now.plusMillis(Math.max(clusterCheckinInterval.toMillis() * 2, 30_000));
    Bson canTake =
        Filters.and(
            Filters.eq("schedName", instanceName),
            Filters.eq("lockName", TRIGGER_ACCESS),
            Filters.or(
                Filters.eq("owner", instanceId),
                Filters.lte("expires", now),
                Filters.exists("owner", false)));
    Document taken =
        locks.findOneAndUpdate(
            canTake,
            Updates.combine(Updates.set("owner", instanceId), Updates.set("expires", expiry)));
    if (taken != null) {
      return true;
    }
    try {
      locks.insertOne(
          new Document("schedName", instanceName)
              .append("lockName", TRIGGER_ACCESS)
              .append("owner", instanceId)
              .append("expires", expiry));
      return true;
    } catch (RuntimeException e) {
      if (!isDuplicateKey(e)) {
        throw e;
      }
    }
    taken =
        locks.findOneAndUpdate(
            canTake,
            Updates.combine(Updates.set("owner", instanceId), Updates.set("expires", expiry)));
    return taken != null;
  }

  private static boolean isClusterLockFailure(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof JobPersistenceException
          && t.getMessage() != null
          && t.getMessage().contains(LOCK_FAILURE_MESSAGE)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isDuplicateKey(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof MongoWriteException mwe
          && mwe.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
        return true;
      }
      String message = t.getMessage();
      if (message != null && message.contains("E11000")) {
        return true;
      }
    }
    return false;
  }

  private void releaseLock() {
    try {
      locks.updateOne(
          Filters.and(
              Filters.eq("schedName", instanceName),
              Filters.eq("lockName", TRIGGER_ACCESS),
              Filters.eq("owner", instanceId)),
          Updates.unset("owner"));
    } catch (RuntimeException e) {
      if (!isShutdownRace(e)) {
        log.warn("Failed to release MongoDB cluster lock for scheduler '{}'", instanceName, e);
      }
    }
  }

  private boolean enterClusterLock() {
    if (clusterLockDepth > 0) {
      clusterLockDepth++;
      return true;
    }
    if (!obtainLock()) {
      return false;
    }
    clusterLockDepth = 1;
    startLockHeartbeat();
    return true;
  }

  private void exitClusterLock() {
    clusterLockDepth = Math.max(0, clusterLockDepth - 1);
    if (clusterLockDepth > 0) {
      return;
    }
    try {
      releaseLock();
    } finally {
      stopLockHeartbeat();
    }
  }

  private synchronized void startLockHeartbeat() {
    if (lockHeartbeat != null) {
      return;
    }
    Thread heartbeat =
        Thread.ofVirtual()
            .name("QuartzLockHeartbeat-" + instanceName)
            .unstarted(
                () -> {
                  while (!Thread.currentThread().isInterrupted()) {
                    try {
                      long sleepMs = Math.max(clusterCheckinInterval.toMillis() / 2, 500L);
                      Thread.sleep(sleepMs);
                      renewClusterLockLease();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      return;
                    } catch (RuntimeException e) {
                      if (!isShutdownRace(e)) {
                        log.warn(
                            "Failed to renew MongoDB cluster lock for scheduler '{}'",
                            instanceName,
                            e);
                      }
                    }
                  }
                });
    lockHeartbeat = heartbeat;
    heartbeat.start();
  }

  private synchronized void stopLockHeartbeat() {
    Thread heartbeat = lockHeartbeat;
    lockHeartbeat = null;
    if (heartbeat != null) {
      heartbeat.interrupt();
    }
  }

  private void renewClusterLockLease() {
    if (locks == null) {
      return;
    }
    Instant expiry =
        Instant.now().plusMillis(Math.max(clusterCheckinInterval.toMillis() * 2, 30_000));
    locks.updateOne(
        Filters.and(
            Filters.eq("schedName", instanceName),
            Filters.eq("lockName", TRIGGER_ACCESS),
            Filters.eq("owner", instanceId)),
        Updates.set("expires", expiry));
  }

  private boolean isShutdownRace(Throwable error) {
    if (Thread.currentThread().isInterrupted()) {
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
      if (message != null
          && (message.contains("state should be: open")
              || message.contains("state should be: server session pool is open"))) {
        return true;
      }
    }
    return !started.get() && Thread.currentThread().isInterrupted();
  }

  private void renewLease() {
    if (leases == null) {
      return;
    }
    Instant expiry = leaseExpiry();
    leases.replaceOne(
        Filters.and(Filters.eq("schedName", instanceName), Filters.eq("owner", instanceId)),
        new Document("schedName", instanceName)
            .append("owner", instanceId)
            .append("expiresAt", BsonJobStoreCodec.instant(expiry)),
        new ReplaceOptions().upsert(true));
    triggers.updateMany(
        Filters.and(
            Filters.eq("schedName", instanceName),
            stateEq(TriggerState.ACQUIRED),
            Filters.eq("leaseOwner", instanceId)),
        Updates.set("leaseExpiresAt", BsonJobStoreCodec.instant(expiry)));
  }

  @FunctionalInterface
  private interface PersistedOp {
    void run() throws JobPersistenceException;
  }

  private class ClusterManager extends Thread {
    ClusterManager() {
      super("QuartzMongoCluster_" + instanceName);
    }

    @Override
    public void run() {
      while (started.get()) {
        try {
          Thread.sleep(clusterCheckinInterval.toMillis());
          if (!started.get()) {
            break;
          }
          renewLease();
          recoverFailedInstances();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (RuntimeException e) {
          if (started.get() && !Thread.currentThread().isInterrupted()) {
            log.error("Cluster lease renewal failed", e);
          }
        }
      }
    }
  }
}
