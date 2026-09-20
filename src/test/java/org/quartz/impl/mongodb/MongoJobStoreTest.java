package org.quartz.impl.mongodb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.quartz.AbstractJobStoreTest;
import org.quartz.Calendar;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.impl.calendar.DailyCalendar;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredResult;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Mongo-backed {@link JobStore} tests. Each store gets an isolated database so tests do not share
 * persisted state (JUnit independence). Teardown clears data, drops the DB, then shuts down the
 * client.
 */
@Testcontainers(disabledWithoutDocker = true)
public class MongoJobStoreTest extends AbstractJobStoreTest {

  @Container
  static final MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

  private final Map<String, MongoJobStore> stores = new HashMap<>();

  @Override
  protected JobStore createJobStore(String name) {
    MongoJobStore previous = stores.get(name);
    if (previous != null) {
      release(previous);
    }

    MongoJobStore store = new MongoJobStore();
    store.setMongoUri(mongo.getConnectionString());
    // Unique DB per store instance — never reuse persisted state across tests.
    store.setDbName("quartz_test_" + UUID.randomUUID().toString().replace("-", ""));
    store.setCollectionPrefix("qrtz_");
    store.setInstanceName(name);
    store.setInstanceId("test-node");
    stores.put(name, store);
    return store;
  }

  @Override
  protected void destroyJobStore(String name) {
    MongoJobStore store = stores.remove(name);
    if (store != null) {
      release(store);
    }
  }

  @Override
  protected Map<String, ? extends JobStore> stores() {
    return stores;
  }

  private static void release(MongoJobStore store) {
    try {
      store.clearAllSchedulingData();
    } catch (Exception ignored) {
      // Prefer releasing resources even if clear fails (e.g. never initialized).
    }
    try {
      if (store.getMongoClient() != null && store.getDbName() != null) {
        store.getMongoClient().getDatabase(store.getDbName()).drop();
      }
    } catch (Exception ignored) {
      // Still shut down below.
    }
    store.shutdown();
  }

  @Test
  void storeJobAndTriggerSurvivesConcurrentAcquire() throws Exception {
    JobStore store = createJobStore("concurrentStore");
    try {
      store.initialize(new SampleSignaler());

      AtomicBoolean stop = new AtomicBoolean(false);
      AtomicReference<Throwable> acquireError = new AtomicReference<>();
      Thread acquirer =
          new Thread(
              () -> {
                while (!stop.get()) {
                  try {
                    store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 10, 0);
                    Thread.sleep(1L);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  } catch (Exception e) {
                    acquireError.compareAndSet(null, e);
                    return;
                  }
                }
              },
              "mongo-acquire");
      acquirer.setDaemon(true);
      acquirer.start();
      try {
        Instant start = Instant.now().plusSeconds(60);
        for (int i = 0; i < 40; i++) {
          JobDetail job = newJob(MyJob.class).withIdentity("counter-" + i, "loadtest").build();
          OperableTrigger trigger =
              (OperableTrigger)
                  newTrigger()
                      .withIdentity("counter-trig-" + i, "loadtest")
                      .forJob(job)
                      .startAt(start)
                      .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever())
                      .build();
          trigger.computeFirstFireTime(null);
          store.storeJobAndTrigger(job, trigger);
        }
      } finally {
        stop.set(true);
        acquirer.join(10_000L);
      }
      assertNull(acquireError.get());
      assertEquals(40, store.getNumberOfJobs());
    } finally {
      destroyJobStore("concurrentStore");
    }
  }

  @Test
  void storeManyJobsWritesIncrementallyAndSurvivesReload() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("bulkStore");
    store.initialize(new SampleSignaler());
    int n = 200;
    Instant start = Instant.now().plusSeconds(60);
    long t0 = System.nanoTime();
    try {
      for (int i = 0; i < n; i++) {
        JobDetail job = newJob(MyJob.class).withIdentity("counter-" + i, "loadtest").build();
        OperableTrigger trigger =
            (OperableTrigger)
                newTrigger()
                    .withIdentity("counter-trig-" + i, "loadtest")
                    .forJob(job)
                    .startAt(start)
                    .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever())
                    .build();
        trigger.computeFirstFireTime(null);
        store.storeJobAndTrigger(job, trigger);
      }
      long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
      assertEquals(n, store.getNumberOfJobs());
      assertTrue(
          elapsedMs < 15_000L, "storing " + n + " jobs took " + elapsedMs + " ms, expected < 15s");

      String db = store.getDbName();
      store.shutdown();
      stores.remove("bulkStore");

      MongoJobStore reloaded = new MongoJobStore();
      reloaded.setMongoUri(mongo.getConnectionString());
      reloaded.setDbName(db);
      reloaded.setCollectionPrefix("qrtz_");
      reloaded.setInstanceName("bulkStore");
      reloaded.setInstanceId("test-node");
      reloaded.initialize(new SampleSignaler());
      try {
        assertEquals(n, reloaded.getNumberOfJobs());
        assertEquals(n, reloaded.getNumberOfTriggers());
      } finally {
        reloaded.getMongoClient().getDatabase(db).drop();
        reloaded.shutdown();
      }
    } finally {
      if (stores.containsKey("bulkStore")) {
        destroyJobStore("bulkStore");
      }
    }
  }

  @Test
  void schedulerStartResetsErrorTriggersWhenJobClassLoads() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("errorRecover");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(MyJob.class).withIdentity("err-job", "g").build();
      OperableTrigger trigger = readyTrigger("err-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_triggers")
          .updateOne(Filters.eq("name", "err-trig"), Updates.set("state", "ERROR"));
      assertEquals(Trigger.TriggerState.ERROR, store.getTriggerState(trigger.getKey()));

      store.schedulerStarted();
      assertEquals(Trigger.TriggerState.NORMAL, store.getTriggerState(trigger.getKey()));
      List<OperableTrigger> acquired =
          store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      assertEquals(1, acquired.size());
      assertEquals("err-trig", acquired.get(0).getKey().getName());
    } finally {
      destroyJobStore("errorRecover");
    }
  }

  @Test
  void acquireContinuesWhenJobClassCannotBeLoaded() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("badPayload");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail bad = newJob(MyJob.class).withIdentity("bad", "g").build();
      JobDetail good = newJob(MyJob.class).withIdentity("good", "g").build();
      OperableTrigger badTrigger = readyTrigger("bad-trig", "g", bad, start);
      OperableTrigger goodTrigger = readyTrigger("good-trig", "g", good, start);
      store.storeJobAndTrigger(bad, badTrigger);
      store.storeJobAndTrigger(good, goodTrigger);

      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_jobs")
          .updateOne(
              Filters.and(Filters.eq("name", "bad"), Filters.eq("group", "g")),
              Updates.set("jobClass", "com.example.MissingJob"));

      List<OperableTrigger> acquired =
          store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 10, 0);
      assertEquals(1, acquired.size());
      assertEquals("good-trig", acquired.get(0).getKey().getName());
      assertEquals(Trigger.TriggerState.NORMAL, store.getTriggerState(badTrigger.getKey()));
    } finally {
      destroyJobStore("badPayload");
    }
  }

  @Test
  void storesJobClassAsStringNotSerializedPayload() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("bsonJobs");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().plusSeconds(60);
      JobDetail job = newJob(MyJob.class).withIdentity("bson", "g").build();
      store.storeJobAndTrigger(job, readyTrigger("bson-trig", "g", job, start));
      Document stored =
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_jobs")
              .find(Filters.and(Filters.eq("name", "bson"), Filters.eq("group", "g")))
              .first();
      assertEquals(MyJob.class.getName(), stored.getString("jobClass"));
      assertNull(stored.get("payload"));
      Document trigger =
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_triggers")
              .find(Filters.and(Filters.eq("name", "bson-trig"), Filters.eq("group", "g")))
              .first();
      assertEquals("simple", trigger.getString("type"));
      assertEquals("WAITING", trigger.getString("state"));
      assertNull(trigger.get("payload"));
      assertEquals(MyJob.class, store.retrieveJob(job.getKey()).getJobClass());
    } finally {
      destroyJobStore("bsonJobs");
    }
  }

  @Test
  void recoversAcquiredTriggersWhenLeaseExpiresWithoutRecycledInstanceId() throws Exception {
    MongoJobStore first = (MongoJobStore) createJobStore("leaseRecover");
    first.setClustered(true);
    first.setInstanceId("dead-lease");
    first.initialize(new SampleSignaler());
    MongoJobStore second = null;
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(MyJob.class).withIdentity("lease-job", "g").build();
      OperableTrigger trigger = readyTrigger("lease-trig", "g", job, start);
      first.storeJobAndTrigger(job, trigger);
      List<OperableTrigger> acquired =
          first.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      assertEquals(1, acquired.size());

      first
          .getMongoClient()
          .getDatabase(first.getDbName())
          .getCollection("qrtz_triggers")
          .updateOne(
              Filters.eq("name", "lease-trig"),
              Updates.set("leaseExpiresAt", Instant.now().minusSeconds(1)));

      second = new MongoJobStore();
      second.setMongoUri(mongo.getConnectionString());
      second.setDbName(first.getDbName());
      second.setCollectionPrefix("qrtz_");
      second.setInstanceName("leaseRecover");
      second.setInstanceId("new-lease");
      second.setClustered(true);
      second.initialize(new SampleSignaler());
      second.schedulerStarted();
      List<OperableTrigger> recovered =
          second.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      assertEquals(1, recovered.size());
      assertEquals("lease-trig", recovered.get(0).getKey().getName());
    } finally {
      if (second != null) {
        second.shutdown();
      }
      destroyJobStore("leaseRecover");
    }
  }

  @Test
  void clusteredLockWaitsThenSucceedsWhenHolderReleases() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("lockWait");
    store.setClustered(true);
    store.setClusterLockWait(Duration.ofSeconds(2));
    store.initialize(new SampleSignaler());
    try {
      holdClusterLock(store, "lockWait", Instant.now().plusSeconds(60));
      Thread releaser =
          new Thread(
              () -> {
                try {
                  Thread.sleep(150L);
                  holdClusterLock(store, "lockWait", Instant.now().minusSeconds(1));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              },
              "release-cluster-lock");
      releaser.start();
      assertDoesNotThrow(
          () -> store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0));
      releaser.join(2_000L);
    } finally {
      destroyJobStore("lockWait");
    }
  }

  @Test
  void clusteredLockTimeoutDoesNotCrashReleaseAcquiredTrigger() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("lockTimeout");
    store.setClustered(true);
    store.setClusterLockWait(Duration.ofMillis(200));
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(MyJob.class).withIdentity("lock-job", "g").build();
      OperableTrigger trigger = readyTrigger("lock-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      holdClusterLock(store, "lockTimeout", Instant.now().plusSeconds(60));
      JobPersistenceException thrown =
          assertThrows(
              JobPersistenceException.class,
              () -> store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0));
      assertTrue(thrown.getMessage().contains("Could not obtain MongoDB cluster lock"));
      assertDoesNotThrow(() -> store.releaseAcquiredTrigger(trigger));
    } finally {
      destroyJobStore("lockTimeout");
    }
  }

  @Test
  void acquireSkipsBlockedJobs() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("skipBlocked");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(ExclusiveJob.class).withIdentity("blocked-job", "g").build();
      OperableTrigger trigger = readyTrigger("blocked-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_jobs")
          .updateOne(Filters.eq("name", "blocked-job"), Updates.set("blocked", true));
      assertTrue(store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0).isEmpty());
    } finally {
      destroyJobStore("skipBlocked");
    }
  }

  @Test
  void missingCalendarMovesTriggerToError() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("missingCal");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(MyJob.class).withIdentity("cal-job", "g").build();
      OperableTrigger trigger =
          (OperableTrigger)
              newTrigger()
                  .withIdentity("cal-trig", "g")
                  .forJob(job)
                  .startAt(start)
                  .modifiedByCalendar("missing")
                  .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever())
                  .build();
      trigger.computeFirstFireTime(null);
      store.storeJobAndTrigger(job, trigger);
      List<OperableTrigger> acquired =
          store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      assertEquals(1, acquired.size());
      List<TriggerFiredResult> fired = store.triggersFired(acquired);
      assertEquals(1, fired.size());
      assertNull(fired.get(0).getTriggerFiredBundle());
      assertEquals(Trigger.TriggerState.ERROR, store.getTriggerState(trigger.getKey()));
    } finally {
      destroyJobStore("missingCal");
    }
  }

  @Test
  void ownerMismatchDoesNotFireOrRelease() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("ownerGuard");
    store.setInstanceId("node-a");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(MyJob.class).withIdentity("own-job", "g").build();
      OperableTrigger trigger = readyTrigger("own-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      List<OperableTrigger> acquired =
          store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      assertEquals(1, acquired.size());
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_triggers")
          .updateOne(Filters.eq("name", "own-trig"), Updates.set("leaseOwner", "node-b"));
      List<TriggerFiredResult> fired = store.triggersFired(acquired);
      assertNull(fired.get(0).getTriggerFiredBundle());
      assertEquals(
          "ACQUIRED",
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_triggers")
              .find(Filters.eq("name", "own-trig"))
              .first()
              .getString("state"));
      store.releaseAcquiredTrigger(acquired.get(0));
      assertEquals(
          "ACQUIRED",
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_triggers")
              .find(Filters.eq("name", "own-trig"))
              .first()
              .getString("state"));
    } finally {
      destroyJobStore("ownerGuard");
    }
  }

  @Test
  void completeFromOtherInstanceDoesNotUnblock() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("completeOwner");
    store.setInstanceId("node-a");
    store.initialize(new SampleSignaler());
    MongoJobStore other = null;
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(ExclusiveJob.class).withIdentity("ex-job", "g").build();
      OperableTrigger trigger = readyTrigger("ex-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      List<OperableTrigger> acquired =
          store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      store.triggersFired(acquired);

      other = new MongoJobStore();
      other.setMongoUri(mongo.getConnectionString());
      other.setDbName(store.getDbName());
      other.setCollectionPrefix("qrtz_");
      other.setInstanceName("completeOwner");
      other.setInstanceId("node-b");
      other.initialize(new SampleSignaler());
      other.triggeredJobComplete(acquired.get(0), job, Trigger.CompletedExecutionInstruction.NOOP);

      Document jobRow =
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_jobs")
              .find(Filters.eq("name", "ex-job"))
              .first();
      assertTrue(Boolean.TRUE.equals(jobRow.getBoolean("blocked")));
      assertEquals("node-a", jobRow.getString("blockedBy"));

      store.triggeredJobComplete(acquired.get(0), job, Trigger.CompletedExecutionInstruction.NOOP);
      jobRow =
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_jobs")
              .find(Filters.eq("name", "ex-job"))
              .first();
      assertFalse(Boolean.TRUE.equals(jobRow.getBoolean("blocked")));
    } finally {
      if (other != null) {
        other.shutdown();
      }
      destroyJobStore("completeOwner");
    }
  }

  @Test
  void nonClusteredStartDoesNotResetOtherOwnersAcquired() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("nonClusterOwner");
    store.setClustered(false);
    store.setInstanceId("node-a");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(MyJob.class).withIdentity("nc-job", "g").build();
      OperableTrigger trigger = readyTrigger("nc-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      assertEquals(1, store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0).size());
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_triggers")
          .updateOne(Filters.eq("name", "nc-trig"), Updates.set("leaseOwner", "node-b"));
      store.schedulerStarted();
      assertEquals(
          "ACQUIRED",
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_triggers")
              .find(Filters.eq("name", "nc-trig"))
              .first()
              .getString("state"));
    } finally {
      destroyJobStore("nonClusterOwner");
    }
  }

  @Test
  void recoversOrphanedBlockedJobsOnStart() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("orphanBlocked");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(ExclusiveJob.class).withIdentity("or-job", "g").build();
      OperableTrigger trigger = readyTrigger("or-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_jobs")
          .updateOne(
              Filters.eq("name", "or-job"),
              Updates.combine(Updates.set("blocked", true), Updates.set("blockedBy", "dead-node")));
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_triggers")
          .updateOne(Filters.eq("name", "or-trig"), Updates.set("state", "BLOCKED"));
      store.schedulerStarted();
      assertEquals(Trigger.TriggerState.NORMAL, store.getTriggerState(trigger.getKey()));
      assertFalse(
          Boolean.TRUE.equals(
              store
                  .getMongoClient()
                  .getDatabase(store.getDbName())
                  .getCollection("qrtz_jobs")
                  .find(Filters.eq("name", "or-job"))
                  .first()
                  .getBoolean("blocked")));
    } finally {
      destroyJobStore("orphanBlocked");
    }
  }

  /**
   * A running exclusive job has a BLOCKED trigger and no ACQUIRED one, so cluster check-in must not
   * mistake it for an orphan and strip the flag that enforces @DisallowConcurrentExecution.
   */
  @Test
  void clusterCheckinKeepsRunningExclusiveJobBlocked() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("blockedLive");
    store.setClustered(true);
    store.setClusterCheckinInterval(Duration.ofMillis(200));
    store.setInstanceId("node-a");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(ExclusiveJob.class).withIdentity("live-job", "g").build();
      OperableTrigger trigger = readyTrigger("live-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      store.schedulerStarted();
      List<OperableTrigger> acquired =
          store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      store.triggersFired(acquired);

      Thread.sleep(1_000L);

      Document jobRow =
          store
              .getMongoClient()
              .getDatabase(store.getDbName())
              .getCollection("qrtz_jobs")
              .find(Filters.eq("name", "live-job"))
              .first();
      assertTrue(Boolean.TRUE.equals(jobRow.getBoolean("blocked")));
      assertEquals("node-a", jobRow.getString("blockedBy"));
      assertEquals(Trigger.TriggerState.BLOCKED, store.getTriggerState(trigger.getKey()));
    } finally {
      destroyJobStore("blockedLive");
    }
  }

  @Test
  void clusteredLockTimeoutOnCompleteDoesNotThrowAndRecovers() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("completeLock");
    store.setClustered(true);
    store.setClusterLockWait(Duration.ofMillis(200));
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().minusSeconds(5);
      JobDetail job = newJob(ExclusiveJob.class).withIdentity("cl-job", "g").build();
      OperableTrigger trigger = readyTrigger("cl-trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      List<OperableTrigger> acquired =
          store.acquireNextTriggers(System.currentTimeMillis() + 60_000L, 1, 0);
      store.triggersFired(acquired);
      holdClusterLock(store, "completeLock", Instant.now().plusSeconds(60));
      assertDoesNotThrow(
          () ->
              store.triggeredJobComplete(
                  acquired.get(0), job, Trigger.CompletedExecutionInstruction.NOOP));
      assertTrue(
          Boolean.TRUE.equals(
              store
                  .getMongoClient()
                  .getDatabase(store.getDbName())
                  .getCollection("qrtz_jobs")
                  .find(Filters.eq("name", "cl-job"))
                  .first()
                  .getBoolean("blocked")));
      holdClusterLock(store, "completeLock", Instant.now().minusSeconds(1));
      store.schedulerStarted();
      assertFalse(
          Boolean.TRUE.equals(
              store
                  .getMongoClient()
                  .getDatabase(store.getDbName())
                  .getCollection("qrtz_jobs")
                  .find(Filters.eq("name", "cl-job"))
                  .first()
                  .getBoolean("blocked")));
    } finally {
      destroyJobStore("completeLock");
    }
  }

  @Test
  void lockHeartbeatRenewsExpiresWhileHoldingLock() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("lockBeat");
    store.setClustered(true);
    store.setClusterCheckinInterval(Duration.ofSeconds(1));
    store.initialize(new SampleSignaler());
    try {
      Instant[] before = new Instant[1];
      Instant[] after = new Instant[1];
      Thread reader =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      Thread.sleep(200L);
                      before[0] = lockInstant(store, "lockBeat");
                      Thread.sleep(900L);
                      after[0] = lockInstant(store, "lockBeat");
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  });
      store.runLocked(Duration.ofMillis(1600));
      reader.join(3_000L);
      assertTrue(before[0] != null && after[0] != null);
      assertTrue(after[0].isAfter(before[0]), before[0] + " then " + after[0]);
    } finally {
      destroyJobStore("lockBeat");
    }
  }

  private static Instant lockInstant(MongoJobStore store, String schedName) {
    Document lock =
        store
            .getMongoClient()
            .getDatabase(store.getDbName())
            .getCollection("qrtz_locks")
            .find(
                Filters.and(
                    Filters.eq("schedName", schedName), Filters.eq("lockName", "TRIGGER_ACCESS")))
            .first();
    Object raw = lock.get("expires");
    if (raw instanceof Instant instant) {
      return instant;
    }
    if (raw instanceof Date date) {
      return date.toInstant();
    }
    return Instant.parse(raw.toString());
  }

  private static void holdClusterLock(MongoJobStore store, String schedName, Instant expires) {
    store
        .getMongoClient()
        .getDatabase(store.getDbName())
        .getCollection("qrtz_locks")
        .updateOne(
            Filters.and(
                Filters.eq("schedName", schedName), Filters.eq("lockName", "TRIGGER_ACCESS")),
            Updates.combine(Updates.set("owner", "other-node"), Updates.set("expires", expires)),
            new com.mongodb.client.model.UpdateOptions().upsert(true));
  }

  @Test
  void initializeCollapsesDuplicateLocksThenCreatesUniqueIndex() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("dupLocks");
    store.initialize(new SampleSignaler());
    var locks = store.getMongoClient().getDatabase(store.getDbName()).getCollection("qrtz_locks");
    locks.dropIndex("schedName_1_lockName_1");
    locks.insertOne(
        new Document("schedName", "dupLocks")
            .append("lockName", "TRIGGER_ACCESS")
            .append("owner", "stale")
            .append("expires", Instant.now().minusSeconds(60)));
    locks.insertOne(
        new Document("schedName", "dupLocks")
            .append("lockName", "TRIGGER_ACCESS")
            .append("owner", "stale-2")
            .append("expires", Instant.now().minusSeconds(30)));
    assertTrue(locks.countDocuments() >= 2);

    MongoJobStore again = new MongoJobStore();
    again.setMongoUri(mongo.getConnectionString());
    again.setDbName(store.getDbName());
    again.setCollectionPrefix("qrtz_");
    again.setInstanceName("dupLocks");
    again.setInstanceId("test-node-2");
    again.initialize(new SampleSignaler());
    try {
      assertEquals(1, locks.countDocuments(Filters.eq("lockName", "TRIGGER_ACCESS")));
    } finally {
      again.shutdown();
      destroyJobStore("dupLocks");
    }
  }

  @Test
  void jobDataMapRoundTripsTypedValues() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("jobDataTypes");
    store.initialize(new SampleSignaler());
    try {
      Duration duration = Duration.ofMinutes(5);
      Instant instant = Instant.parse("2020-01-02T03:04:05Z");
      Date date = new Date(1_700_000_000_000L);
      UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
      Map<String, Object> nested = new LinkedHashMap<>();
      nested.put("inner", 7);
      JobDetail job = newJob(MyJob.class).withIdentity("typed", "g").build();
      job.getJobDataMap().put("duration", duration);
      job.getJobDataMap().put("instant", instant);
      job.getJobDataMap().put("date", date);
      job.getJobDataMap().put("uuid", uuid);
      job.getJobDataMap().put("color", SampleColor.RED);
      job.getJobDataMap().put("nested", nested);
      store.storeJob(job, false);

      JobDataMap loaded = store.retrieveJob(job.getKey()).getJobDataMap();
      assertEquals(duration, loaded.get("duration"));
      assertEquals(instant, loaded.get("instant"));
      assertEquals(date, loaded.get("date"));
      assertEquals(uuid, loaded.get("uuid"));
      assertEquals(SampleColor.RED, loaded.get("color"));
      Object nestedLoaded = loaded.get("nested");
      assertInstanceOf(Map.class, nestedLoaded);
      assertFalse(nestedLoaded instanceof JobDataMap);
      assertEquals(7, ((Map<?, ?>) nestedLoaded).get("inner"));
    } finally {
      destroyJobStore("jobDataTypes");
    }
  }

  @Test
  void unsupportedJobDataMapTypeIsRejected() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("badJobData");
    store.initialize(new SampleSignaler());
    try {
      JobDetail job = newJob(MyJob.class).withIdentity("bad-data", "g").build();
      job.getJobDataMap().put("pojo", new Object());
      JobPersistenceException thrown =
          assertThrows(JobPersistenceException.class, () -> store.storeJob(job, false));
      assertTrue(thrown.getMessage().contains("cannot be stored") || thrown.getCause() != null);
    } finally {
      destroyJobStore("badJobData");
    }
  }

  @Test
  void getJobDetailsSkipsJobsWhoseClassCannotBeLoaded() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("listSkip");
    store.initialize(new SampleSignaler());
    try {
      JobDetail bad = newJob(MyJob.class).withIdentity("bad", "g").build();
      JobDetail good = newJob(MyJob.class).withIdentity("good", "g").build();
      store.storeJob(bad, false);
      store.storeJob(good, false);
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_jobs")
          .updateOne(
              Filters.and(Filters.eq("name", "bad"), Filters.eq("group", "g")),
              Updates.set("jobClass", "com.example.MissingJob"));

      List<JobDetail> listed = store.getJobDetails(GroupMatcher.anyGroup());
      assertEquals(1, listed.size());
      assertEquals("good", listed.get(0).getKey().getName());
    } finally {
      destroyJobStore("listSkip");
    }
  }

  @Test
  void truncatedDailyCalendarStillLoads() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("dailyCal");
    store.initialize(new SampleSignaler());
    try {
      store.storeCalendar("hours", new DailyCalendar("08:00:00:000", "17:00:00:000"), false, false);
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_calendars")
          .updateOne(
              Filters.eq("name", "hours"), Updates.set("calendar", new Document("type", "daily")));

      Calendar loaded = store.retrieveCalendar("hours");
      DailyCalendar daily = assertInstanceOf(DailyCalendar.class, loaded);
      long start =
          ((daily.getRangeStartingHourOfDay() * 60L + daily.getRangeStartingMinute()) * 60
                      + daily.getRangeStartingSecond())
                  * 1000
              + daily.getRangeStartingMillis();
      long end =
          ((daily.getRangeEndingHourOfDay() * 60L + daily.getRangeEndingMinute()) * 60
                      + daily.getRangeEndingSecond())
                  * 1000
              + daily.getRangeEndingMillis();
      assertTrue(end > start);
    } finally {
      destroyJobStore("dailyCal");
    }
  }

  @Test
  void retrieveTriggerWrapsDecodeErrors() throws Exception {
    MongoJobStore store = (MongoJobStore) createJobStore("badTrigger");
    store.initialize(new SampleSignaler());
    try {
      Instant start = Instant.now().plusSeconds(60);
      JobDetail job = newJob(MyJob.class).withIdentity("job", "g").build();
      OperableTrigger trigger = readyTrigger("trig", "g", job, start);
      store.storeJobAndTrigger(job, trigger);
      store
          .getMongoClient()
          .getDatabase(store.getDbName())
          .getCollection("qrtz_triggers")
          .updateOne(Filters.eq("name", "trig"), Updates.set("type", "not-a-trigger"));
      assertThrows(JobPersistenceException.class, () -> store.retrieveTrigger(trigger.getKey()));
    } finally {
      destroyJobStore("badTrigger");
    }
  }

  @DisallowConcurrentExecution
  public static class ExclusiveJob extends MyJob {}

  enum SampleColor {
    RED
  }

  private static OperableTrigger readyTrigger(
      String name, String group, JobDetail job, Instant start) {
    OperableTrigger trigger =
        (OperableTrigger)
            newTrigger()
                .withIdentity(name, group)
                .forJob(job)
                .startAt(start)
                .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever())
                .build();
    trigger.computeFirstFireTime(null);
    return trigger;
  }
}
