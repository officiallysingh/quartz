package org.quartz.impl.mongodb;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.quartz.AbstractJobStoreTest;
import org.quartz.spi.JobStore;
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
}
