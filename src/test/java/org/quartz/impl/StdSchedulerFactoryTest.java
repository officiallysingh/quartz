/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.quartz.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.NOPLogger;

class StdSchedulerFactoryTest {

  @Test
  void defaultPropertiesDoNotNeedAPropertiesFile() {
    Properties defaults = StdSchedulerFactory.defaultProperties();
    assertEquals(
        "quartzScheduler", defaults.getProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME));
    assertEquals(
        StdSchedulerFactory.AUTO_GENERATE_INSTANCE_ID,
        defaults.getProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID));
    assertEquals("10", defaults.getProperty("org.quartz.threadPool.threadCount"));
    assertEquals("true", defaults.getProperty("org.quartz.jobStore.isClustered"));
    assertEquals("qrtz_", defaults.getProperty("org.quartz.jobStore.collectionPrefix"));
  }

  @Test
  void initializePropertiesMergesDefaults() throws Exception {
    Properties overlay = new Properties();
    overlay.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "custom");
    StdSchedulerFactory factory = new StdSchedulerFactory();
    factory.initialize(overlay);
    java.lang.reflect.Field cfg = StdSchedulerFactory.class.getDeclaredField("cfg");
    cfg.setAccessible(true);
    org.quartz.utils.PropertiesParser parser = (org.quartz.utils.PropertiesParser) cfg.get(factory);
    Properties merged = parser.getUnderlyingProperties();
    assertEquals("custom", merged.getProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME));
    assertEquals("10", merged.getProperty("org.quartz.threadPool.threadCount"));
    assertEquals("true", merged.getProperty("org.quartz.jobStore.isClustered"));
  }

  @Test
  void testOverrideSystemProperties() {
    Properties p = new Properties();
    p.setProperty("nonsense1", "hello1");
    p.setProperty("nonsense2", "hello2");
    System.setProperty("nonsense1", "boo1");
    String osName = System.getProperty("os.name");
    Properties q = StdSchedulerFactory.overrideWithSysProps(p, NOPLogger.NOP_LOGGER);
    assertEquals("boo1", q.get("nonsense1"));
    assertEquals(osName, q.get("os.name"));
  }
}
