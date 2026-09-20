package org.quartz.spring.boot.actuate.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.integrations.tests.HelloJob;
import org.quartz.spi.OperableTrigger;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint.QuartzJobDetailsDescriptor;
import org.quartz.spring.boot.actuate.endpoint.QuartzEndpoint.QuartzJobTriggerDescriptor;
import org.springframework.boot.actuate.endpoint.SanitizingFunction;

class QuartzEndpointTests {

  private static final JobKey JOB_KEY = JobKey.jobKey("nightly", "reports");

  private static final Instant START = Instant.parse("2026-01-01T10:00:00Z");

  private final Scheduler scheduler = mock(Scheduler.class);

  private final QuartzEndpoint endpoint = new QuartzEndpoint(this.scheduler, List.of());

  @Test
  void reportListsJobAndTriggerGroups() throws Exception {
    when(this.scheduler.getJobGroupNames()).thenReturn(List.of("reports", "DEFAULT"));
    when(this.scheduler.getTriggerGroupNames()).thenReturn(List.of("reports"));

    QuartzEndpoint.QuartzDescriptor report = this.endpoint.quartzReport();

    assertEquals(Set.of("reports", "DEFAULT"), report.getJobs().getGroups());
    assertEquals(Set.of("reports"), report.getTriggers().getGroups());
  }

  @Test
  void jobDetailsListTriggersByNextFireTime() throws Exception {
    Trigger later = simpleTrigger("later", START.plusSeconds(600));
    Trigger sooner = simpleTrigger("sooner", START.plusSeconds(60));
    when(this.scheduler.getJobDetail(JOB_KEY)).thenReturn(jobDetail());
    doReturn(List.of(later, sooner)).when(this.scheduler).getTriggersOfJob(JOB_KEY);

    QuartzJobDetailsDescriptor job = this.endpoint.quartzJob("reports", "nightly", true);

    assertEquals("reports", job.getGroup());
    assertEquals("nightly", job.getName());
    assertEquals(HelloJob.class.getName(), job.getClassName());
    assertEquals(
        List.of("sooner", "later"), job.getTriggers().stream().map(t -> t.get("name")).toList());
    assertEquals(START.plusSeconds(60), job.getTriggers().get(0).get("nextFireTime"));
  }

  @Test
  void jobDataMapIsSanitized() throws Exception {
    SanitizingFunction sanitizeAll = data -> data.withValue("******");
    QuartzEndpoint endpoint = new QuartzEndpoint(this.scheduler, List.of(sanitizeAll));
    when(this.scheduler.getJobDetail(JOB_KEY)).thenReturn(jobDetail());
    doReturn(List.of()).when(this.scheduler).getTriggersOfJob(JOB_KEY);

    QuartzJobDetailsDescriptor job = endpoint.quartzJob("reports", "nightly", false);

    assertEquals("******", job.getData().get("token"));
  }

  @Test
  void unknownJobIsNull() throws Exception {
    assertNull(this.endpoint.quartzJob("reports", "missing", false));
  }

  @Test
  void unknownJobGroupIsNull() throws Exception {
    when(this.scheduler.getJobKeys(GroupMatcher.jobGroupEquals("missing"))).thenReturn(Set.of());
    when(this.scheduler.getJobGroupNames()).thenReturn(List.of("reports"));

    assertNull(this.endpoint.quartzJobGroupSummary("missing"));
  }

  @Test
  void simpleTriggerDetailsReportTheIntervalInMillis() throws Exception {
    TriggerKey triggerKey = TriggerKey.triggerKey("sooner", "reports");
    when(this.scheduler.getTrigger(triggerKey)).thenReturn(simpleTrigger("sooner", START));
    when(this.scheduler.getTriggerState(triggerKey)).thenReturn(TriggerState.NORMAL);

    Map<String, Object> details = this.endpoint.quartzTrigger("reports", "sooner", true);

    assertEquals("simple", details.get("type"));
    assertEquals(TriggerState.NORMAL, details.get("state"));
    assertEquals(START, details.get("startTime"));
    Map<String, Object> simple = triggerDetails(details, "simple");
    assertEquals(Duration.ofSeconds(30).toMillis(), simple.get("interval"));
    assertEquals(-1, simple.get("repeatCount"));
    assertEquals(0, simple.get("timesTriggered"));
  }

  @Test
  void cronTriggerDetailsReportTheExpression() throws Exception {
    TriggerKey triggerKey = TriggerKey.triggerKey("hourly", "reports");
    Trigger trigger =
        TriggerBuilder.newTrigger()
            .withIdentity(triggerKey)
            .forJob(JOB_KEY)
            .withSchedule(CronScheduleBuilder.cronSchedule("0 0 * * * ?"))
            .build();
    when(this.scheduler.getTrigger(triggerKey)).thenReturn(trigger);
    when(this.scheduler.getTriggerState(triggerKey)).thenReturn(TriggerState.NORMAL);

    Map<String, Object> details = this.endpoint.quartzTrigger("reports", "hourly", true);

    assertEquals("cron", details.get("type"));
    assertEquals("0 0 * * * ?", triggerDetails(details, "cron").get("expression"));
  }

  @Test
  void triggeringAJobRunsItNow() throws Exception {
    when(this.scheduler.getJobDetail(JOB_KEY)).thenReturn(jobDetail());

    QuartzJobTriggerDescriptor descriptor = this.endpoint.triggerQuartzJob("reports", "nightly");

    verify(this.scheduler).triggerJob(JOB_KEY);
    assertEquals("reports", descriptor.getGroup());
    assertEquals("nightly", descriptor.getName());
    assertTrue(descriptor.getTriggerTime().isAfter(START));
  }

  private static JobDetail jobDetail() {
    return JobBuilder.newJob(HelloJob.class)
        .withIdentity(JOB_KEY)
        .withDescription("Nightly report")
        .usingJobData("token", "secret")
        .storeDurably()
        .build();
  }

  private static Trigger simpleTrigger(String name, Instant nextFireTime) {
    OperableTrigger trigger =
        (OperableTrigger)
            TriggerBuilder.newTrigger()
                .withIdentity(name, "reports")
                .forJob(JOB_KEY)
                .startAt(nextFireTime)
                .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(30))
                .build();
    trigger.computeFirstFireTime(null);
    return trigger;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> triggerDetails(Map<String, Object> details, String type) {
    return (Map<String, Object>) details.get(type);
  }
}
