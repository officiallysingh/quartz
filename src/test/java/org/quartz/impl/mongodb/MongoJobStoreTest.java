package org.quartz.impl.mongodb;

import java.util.HashMap;
import java.util.Map;
import org.quartz.AbstractJobStoreTest;
import org.quartz.spi.JobStore;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
public class MongoJobStoreTest extends AbstractJobStoreTest {

  @Container
  static final MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

  private final Map<String, MongoJobStore> stores = new HashMap<>();

  @Override
  protected JobStore createJobStore(String name) {
    MongoJobStore store = new MongoJobStore();
    store.setMongoUri(mongo.getConnectionString());
    store.setDbName("quartz_test");
    store.setCollectionPrefix(name.replaceAll("[^A-Za-z0-9]", "_") + "_");
    store.setInstanceName(name);
    store.setInstanceId("test-node");
    stores.put(name, store);
    return store;
  }

  @Override
  protected void destroyJobStore(String name) {
    MongoJobStore store = stores.remove(name);
    if (store != null) {
      store.shutdown();
    }
  }

  @Override
  protected Map<String, ? extends JobStore> stores() {
    return stores;
  }
}
