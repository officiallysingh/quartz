/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.quartz.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerConfigException;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.mongodb.MongoJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.SchedulerPlugin;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.ThreadPool;

class DirectSchedulerFactoryTest {
  @Test
  void testPlugins() throws Exception {
    final StringBuffer result = new StringBuffer();

    SchedulerPlugin testPlugin =
        new SchedulerPlugin() {
          public void initialize(String name, org.quartz.Scheduler scheduler)
              throws org.quartz.SchedulerException {
            result.append(name).append("|").append(scheduler.getSchedulerName());
          }
          ;

          public void start() {
            result.append("|start");
          }
          ;

          public void shutdown() {
            result.append("|shutdown");
          }
          ;
        };

    ThreadPool threadPool = new SimpleThreadPool(1, 5);
    threadPool.initialize();
    DirectSchedulerFactory.getInstance()
        .createScheduler(
            "MyScheduler",
            "Instance1",
            threadPool,
            uninitializedMongoStore(),
            Collections.singletonMap("TestPlugin", testPlugin),
            java.time.Duration.ZERO);

    Scheduler scheduler = DirectSchedulerFactory.getInstance().getScheduler("MyScheduler");
    scheduler.start();
    scheduler.shutdown();

    assertEquals("TestPlugin|MyScheduler|start|shutdown", result.toString());
  }

  @Test
  void testInstanceNameAndIdAreSetOnPoolAndStore() throws Exception {
    AtomicReference<String> poolName = new AtomicReference<>();
    AtomicReference<String> poolId = new AtomicReference<>();
    AtomicReference<String> storeName = new AtomicReference<>();
    AtomicReference<String> storeId = new AtomicReference<>();

    SimpleThreadPool threadPool =
        new SimpleThreadPool(1, Thread.NORM_PRIORITY) {
          @Override
          public void setInstanceName(String schedName) {
            super.setInstanceName(schedName);
            poolName.set(schedName);
          }

          @Override
          public void setInstanceId(String schedInstId) {
            super.setInstanceId(schedInstId);
            poolId.set(schedInstId);
          }
        };
    MongoJobStore jobStore =
        new MongoJobStore() {
          @Override
          public void initialize(SchedulerSignaler signaler) {
            // Instance-id test only; skip Mongo.
          }

          @Override
          public void setInstanceName(String schedName) {
            super.setInstanceName(schedName);
            storeName.set(schedName);
          }

          @Override
          public void setInstanceId(String schedInstId) {
            super.setInstanceId(schedInstId);
            storeId.set(schedInstId);
          }
        };

    DirectSchedulerFactory.getInstance()
        .createScheduler("DirectSetterScheduler", "lease-owner-1", threadPool, jobStore);
    Scheduler scheduler =
        DirectSchedulerFactory.getInstance().getScheduler("DirectSetterScheduler");
    try {
      assertEquals("DirectSetterScheduler", poolName.get());
      assertEquals("lease-owner-1", poolId.get());
      assertEquals("DirectSetterScheduler", storeName.get());
      assertEquals("lease-owner-1", storeId.get());
    } finally {
      scheduler.shutdown(true);
    }
  }

  @Test
  void testThreadName() throws Throwable {
    SimpleThreadPool threadPool = new SimpleThreadPool(4, Thread.NORM_PRIORITY);
    DirectSchedulerFactory.getInstance().createScheduler(threadPool, uninitializedMongoStore());
    Scheduler scheduler = DirectSchedulerFactory.getInstance().getScheduler();
    try {
      QuartzScheduler qs = getField(scheduler, "sched");
      QuartzSchedulerResources qsr = getField(qs, "resources");
      ThreadPool tp = qsr.getThreadPool();
      List<?> list = getField(tp, "workers");
      Object workerThread = list.get(0);
      String workerThreadName = workerThread.toString();
      assertFalse(workerThreadName.contains("null"));
      assertTrue(workerThreadName.contains(scheduler.getSchedulerName()));
    } finally {
      scheduler.shutdown(true);
    }
  }

  private static MongoJobStore uninitializedMongoStore() {
    return new MongoJobStore() {
      @Override
      public void initialize(SchedulerSignaler signaler) throws SchedulerConfigException {
        // Plugin / thread-name tests do not persist jobs.
      }
    };
  }

  <T> T getField(Object obj, String fieldName) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(obj);
  }
}
