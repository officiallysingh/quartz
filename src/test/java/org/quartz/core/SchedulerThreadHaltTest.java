package org.quartz.core;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quartz.TriggerBuilder.newTrigger;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.quartz.JobPersistenceException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.mongodb.MongoJobStore;
import org.quartz.listeners.SchedulerListenerSupport;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;

class SchedulerThreadHaltTest {

  /**
   * Shutting down interrupts the scheduler thread while a store call is in flight. That is expected
   * and already handled, so it must not be reported to scheduler listeners as a firing failure.
   */
  @Test
  void haltDuringTriggersFiredIsNotReportedAsError() throws Exception {
    List<String> errors = new CopyOnWriteArrayList<>();
    HaltingJobStore store = new HaltingJobStore();
    DirectSchedulerFactory.getInstance()
        .createScheduler(
            "HaltDuringFire", "node-a", new SimpleThreadPool(2, Thread.NORM_PRIORITY), store);
    Scheduler scheduler = DirectSchedulerFactory.getInstance().getScheduler("HaltDuringFire");
    try {
      scheduler
          .getListenerManager()
          .addSchedulerListener(
              new SchedulerListenerSupport() {
                @Override
                public void schedulerError(String msg, SchedulerException cause) {
                  errors.add(msg);
                }
              });
      scheduler.start();
      assertTrue(
          store.firing.await(10, TimeUnit.SECONDS), "scheduler thread never reached triggersFired");
    } finally {
      scheduler.shutdown(true);
    }
    assertTrue(
        errors.stream().noneMatch(m -> m.contains("An error occurred while firing triggers")),
        () -> "halt was reported as a firing error: " + errors);
  }

  /** Hands out one overdue trigger, then blocks in {@code triggersFired} until halt interrupts. */
  private static final class HaltingJobStore extends MongoJobStore {

    private final CountDownLatch firing = new CountDownLatch(1);

    @Override
    public void initialize(SchedulerSignaler signaler) {
      // No Mongo: this test only drives the scheduler thread's halt handling.
    }

    @Override
    public void schedulerStarted() {}

    @Override
    public void shutdown() {}

    @Override
    public List<OperableTrigger> acquireNextTriggers(
        long noLaterThan, int maxCount, long timeWindow) {
      OperableTrigger trigger =
          (OperableTrigger)
              newTrigger()
                  .withIdentity("halt-trig", "g")
                  .forJob("halt-job", "g")
                  .startAt(Instant.now().minusSeconds(5))
                  .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever())
                  .build();
      trigger.computeFirstFireTime(null);
      return List.of(trigger);
    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers)
        throws JobPersistenceException {
      firing.countDown();
      try {
        Thread.sleep(30_000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new JobPersistenceException("MongoDB unavailable during scheduler shutdown", e);
      }
      return List.of();
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) {}
  }
}
