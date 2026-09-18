package org.quartz.impl.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.quartz.AbstractJobStoreTest;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
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
      ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
      loadHelper.initialize();
      store.initialize(loadHelper, new SampleSignaler());

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
    ClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();
    store.initialize(loadHelper, new SampleSignaler());
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
      reloaded.initialize(loadHelper, new SampleSignaler());
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
}
