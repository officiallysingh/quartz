package org.quartz.simpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.MongoSchedulerSupport;
import org.quartz.Scheduler;
import org.quartz.SchedulerConfigException;
import org.quartz.impl.StdSchedulerFactory;

class VirtualThreadPoolTest {

  @Test
  void initializeRequiresPositiveConcurrency() {
    VirtualThreadPool pool = new VirtualThreadPool();
    assertThrows(SchedulerConfigException.class, pool::initialize);
  }

  @Test
  void blockAndRunRespectMaxConcurrency() throws Exception {
    VirtualThreadPool pool = new VirtualThreadPool();
    pool.setInstanceName("test");
    pool.setThreadCount(2);
    pool.initialize();

    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger running = new AtomicInteger();
    AtomicInteger maxRunning = new AtomicInteger();

    Runnable blocker =
        () -> {
          running.incrementAndGet();
          maxRunning.accumulateAndGet(running.get(), Math::max);
          started.countDown();
          try {
            release.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            running.decrementAndGet();
          }
        };

    assertEquals(2, pool.blockForAvailableThreads());
    assertTrue(pool.runInThread(blocker));
    assertTrue(pool.runInThread(blocker));
    assertTrue(started.await(5, TimeUnit.SECONDS));

    // Third job should wait for a permit; start it on a helper thread.
    CountDownLatch thirdStarted = new CountDownLatch(1);
    Thread submitter =
        Thread.ofPlatform()
            .start(
                () ->
                    pool.runInThread(
                        () -> {
                          thirdStarted.countDown();
                          try {
                            release.await(5, TimeUnit.SECONDS);
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                          }
                        }));

    assertTrue(
        !thirdStarted.await(200, TimeUnit.MILLISECONDS),
        "third job must wait until a concurrency slot frees");
    release.countDown();
    assertTrue(thirdStarted.await(5, TimeUnit.SECONDS));
    submitter.join(5000);
    pool.shutdown(true);

    assertEquals(2, maxRunning.get());
  }

  @Test
  void jobsRunOnVirtualThreads() throws Exception {
    Properties config = new Properties();
    config.setProperty("org.quartz.scheduler.instanceName", "VirtualThreadScheduler");
    config.setProperty("org.quartz.threadPool.class", VirtualThreadPool.class.getName());
    config.setProperty("org.quartz.threadPool.threadCount", "4");
    MongoSchedulerSupport.applyJobStore(config);

    AtomicReference<Boolean> virtual = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);
    VirtualProbeJob.virtual = virtual;
    VirtualProbeJob.done = done;

    Scheduler scheduler = new StdSchedulerFactory(config).getScheduler();
    try {
      scheduler.start();
      scheduler.scheduleJob(
          newJob(VirtualProbeJob.class).withIdentity("vjob").build(),
          newTrigger().withIdentity("vtrig").startNow().build());
      assertTrue(done.await(10, TimeUnit.SECONDS));
      assertEquals(Boolean.TRUE, virtual.get());
    } finally {
      scheduler.shutdown(true);
    }
  }

  @Test
  void acceptsSimpleThreadPoolOnlyProperties() throws Exception {
    Properties config = new Properties();
    config.setProperty("org.quartz.scheduler.instanceName", "VirtualThreadPoolProps");
    config.setProperty("org.quartz.threadPool.class", VirtualThreadPool.class.getName());
    config.setProperty("org.quartz.threadPool.threadCount", "2");
    config.setProperty("org.quartz.threadPool.threadPriority", "5");
    config.setProperty("org.quartz.threadPool.makeThreadsDaemons", "true");
    config.setProperty("org.quartz.threadPool.threadsInheritGroupOfInitializingThread", "true");
    MongoSchedulerSupport.applyJobStore(config);

    Scheduler scheduler = new StdSchedulerFactory(config).getScheduler();
    try {
      assertTrue(scheduler.getMetaData().getThreadPoolClass().equals(VirtualThreadPool.class));
    } finally {
      scheduler.shutdown(true);
    }
  }

  public static class VirtualProbeJob implements Job {
    static AtomicReference<Boolean> virtual;
    static CountDownLatch done;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      virtual.set(Thread.currentThread().isVirtual());
      done.countDown();
    }
  }
}
