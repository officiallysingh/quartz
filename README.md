# Scheduling jobs in Spring Boot with Quartz and MongoDB

**A Quartz scheduler for Spring Boot applications that persists jobs only in MongoDB.** Requires **Java 21+**, **Spring Boot 4+**, and a MongoDB database the application already uses. There is no JDBC job store, no in-memory job store, and no Terracotta job store. The official `spring-boot-starter-quartz` must stay off the classpath.

## Table of Contents

1. [What stock Quartz makes you live with](#what-stock-quartz-makes-you-live-with)
2. [What this library gives you](#what-this-library-gives-you)
3. [Installation](#installation)
4. [MongoDB](#mongodb)
5. [Scheduler configuration](#scheduler-configuration)
6. [Your first job](#your-first-job)
7. [Schedules you can copy](#schedules-you-can-copy)
8. [Job data](#job-data)
9. [One job, many triggers, and firing on demand](#one-job-many-triggers-and-firing-on-demand)
10. [Calendars](#calendars)
11. [Misfires](#misfires)
12. [Clustering](#clustering)
13. [Threads, including virtual threads](#threads-including-virtual-threads)
14. [History logs](#history-logs)
15. [Listeners and plugins](#listeners-and-plugins)
16. [Pause, resume, reschedule, delete](#pause-resume-reschedule-delete)
17. [How jobs are created](#how-jobs-are-created)
18. [What is stored in MongoDB](#what-is-stored-in-mongodb)
19. [Time: Instant, ZoneId, and cron](#time-instant-zoneid-and-cron)
20. [Running without Spring Boot auto-configuration](#running-without-spring-boot-auto-configuration)

## What stock Quartz makes you live with

Quartz is a solid scheduler. The parts that hurt in a Spring Boot service that already runs on MongoDB are the storage model and the Spring integration, not the trigger DSL.

* **A durable schedule means a relational database.** `JobStoreTX` / `JobStoreCMT` expect a DataSource and the `QRTZ_*` table script (jobs, triggers, calendars, fired triggers, locks, scheduler state, and more). An application whose only database is MongoDB ends up running Postgres or MySQL for the scheduler alone.
* **The in-memory store does not survive a restart.** `RAMJobStore` is the default when you do not configure JDBC. Every job and trigger disappears when the process stops, and two JVMs cannot share it.
* **Spring Boot's Quartz starter assumes JDBC.** `spring-boot-starter-quartz` auto-configures a scheduler against a `DataSource`. It does not know about MongoDB. This jar uses the same `org.quartz` packages, so the two artifacts cannot sit on one classpath.
* **Cluster recovery is tied to a recycled instance id.** In the JDBC store, a dead node is recognised by `instanceId` in `QRTZ_SCHEDULER_STATE`. A new process that reuses that id, or clocks that disagree about "how long ago", leaves triggers stuck in `ACQUIRED` or jobs stuck `BLOCKED`.
* **Fire times are `java.util.Date`.** `Trigger.getNextFireTime()` returns a `Date`. `DateBuilder` takes a `java.util.TimeZone`. Job data in the JDBC store is a Java serialization blob, so a class rename breaks every stored map.
* **A job is not a Spring bean.** Quartz calls a public no-arg constructor. `@Autowired` collaborators stay null unless you wire `SpringBeanJobFactory` from `spring-context-support` yourself.

## What this library gives you

* **One job store: MongoDB.** `MongoJobStore` is the only `JobStore`. Jobs, triggers, and calendars are BSON documents. Acquire is a `findOneAndUpdate`. Unique indexes keep one document per job and per trigger. JDBC and `RAMJobStore` are not in this build.
* **The application's Mongo client.** Auto-configuration uses the `MongoClient` Spring Boot already created. If that bean is absent and Spring Data's `MongoDatabaseFactory` is present, the job store uses that database. The store does not close a client it does not own, and it does not take a second Quartz connection string.
* **Leases instead of recycled instance names.** Each process gets a fresh id (a UUID when `instance-id` is left empty). A trigger is owned until `leaseExpiresAt`. Recovery looks for an expired lease. It does not look up a hostname that a new pod reused.
* **`java.time.Instant` on the public fire-time API.** `startAt`, `endAt`, `getNextFireTime`, `getPreviousFireTime`, and `getFinalFireTime` use `Instant`. `DateBuilder` builds those instants and takes `ZoneId`. `scheduler.startDelayed` takes a `Duration`.
* **Spring jobs.** Each fire constructs a new job instance, autowires it from the application context, copies `JobDataMap` entries onto matching setters, and destroys the instance when the fire finishes.
* **Optional virtual-thread pool.** `quartz.scheduler.thread-pool.virtual=true` runs each job on a virtual thread. `thread-count` is still the concurrency cap, so the scheduler does not acquire an unbounded number of triggers.
* **History plugins that log ISO-8601 instants.** They are off until you declare them as beans.

> **Important**
>
> Do not add `org.springframework.boot:spring-boot-starter-quartz` or `org.quartz-scheduler:quartz`. Both define `org.quartz.*`. If another dependency pulls the official jar in, exclude it.

```xml
<exclusion>
    <groupId>org.quartz-scheduler</groupId>
    <artifactId>quartz</artifactId>
</exclusion>
```

## Installation

> **Current version: 0.0.1-SNAPSHOT.** The jar is built on Java 21. Spring Boot auto-configuration targets Spring Boot 4 (the Mongo auto-configuration package is `org.springframework.boot.mongodb.autoconfigure`).

Add the jar. That registers `QuartzAutoConfiguration`. The scheduler starts after the application context is ready, provided a Mongo client or a Spring Data `MongoDatabaseFactory` exists.

**Maven**

```xml
<dependency>
    <groupId>io.github.officiallysingh</groupId>
    <artifactId>spring-boot-mongodb-quartz-scheduler</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

**Gradle**

```groovy
implementation 'io.github.officiallysingh:spring-boot-mongodb-quartz-scheduler:0.0.1-SNAPSHOT'
```

The application also needs a MongoDB driver on the classpath. `spring-boot-starter-data-mongodb` is the usual choice, because this library will reuse that client.

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-mongodb</artifactId>
</dependency>
```

```groovy
implementation 'org.springframework.boot:spring-boot-starter-data-mongodb'
```

`QuartzAutoConfiguration` is active when `quartz.scheduler.enabled` is missing or `true`, and when `org.quartz.Scheduler` and `com.mongodb.client.MongoClient` are on the classpath. Declare `JobDetail`, `Trigger`, `org.quartz.Calendar`, and `SchedulerPlugin` beans. They are registered on startup. No `SchedulerFactoryBean` from Spring's JDBC support is required.

## MongoDB

Point Spring Boot at the database your application already uses. Quartz reads the same database. It does not have a `quartz.mongodb.uri` of its own.

**Spring Boot 4**

```yaml
spring:
  mongodb:
    uri: mongodb://localhost:27017/appdb
    database: appdb
```

```properties
spring.mongodb.uri=mongodb://localhost:27017/appdb
spring.mongodb.database=appdb
```

The database name is resolved in this order:

1. `spring.mongodb.database`
2. `spring.data.mongodb.database`
3. The database path in `spring.mongodb.uri` or `spring.data.mongodb.uri`
4. `test`, which is Spring Boot's default when nothing is set

The Mongo user must be allowed to create the Quartz collections and their indexes. Indexes are created when the scheduler starts.

> **Note**
>
> A single local `mongod` that is not a replica set is fine for one application process. Set `quartz.scheduler.clustered: false` in that case. Clustering is on by default and expects a replica set. See [Clustering](#clustering).

## Scheduler configuration

All settings live under `quartz.scheduler`. Durations accept Spring Boot's duration format. A unitless number is milliseconds (`120000` and `120s` are the same value).

```yaml
quartz:
  scheduler:
    enabled: true
    name: quartzScheduler
    auto-startup: true
    startup-delay: 0s
    wait-for-jobs-to-complete-on-shutdown: true
    overwrite-existing-jobs: false
    fail-fast-on-start: true
    clustered: true
    cluster-checkin-interval: 15s
    misfire-threshold: 60s
    idle-wait-time: 30s
    batch-time-window: 0s
    collection-prefix: qrtz_
    thread-pool:
      thread-count: 10
      thread-priority: 5
      virtual: false
    # instance-id:           # empty -> a new UUID on every process start
    # properties:
    #   org.quartz.scheduler.idleWaitTime: "30000"
```

The same file in properties form:

```properties
quartz.scheduler.enabled=true
quartz.scheduler.name=quartzScheduler
quartz.scheduler.auto-startup=true
quartz.scheduler.startup-delay=0s
quartz.scheduler.wait-for-jobs-to-complete-on-shutdown=true
quartz.scheduler.overwrite-existing-jobs=false
quartz.scheduler.fail-fast-on-start=true
quartz.scheduler.clustered=true
quartz.scheduler.cluster-checkin-interval=15s
quartz.scheduler.misfire-threshold=60s
quartz.scheduler.idle-wait-time=30s
quartz.scheduler.batch-time-window=0s
quartz.scheduler.collection-prefix=qrtz_
quartz.scheduler.thread-pool.thread-count=10
quartz.scheduler.thread-pool.thread-priority=5
quartz.scheduler.thread-pool.virtual=false
```

| Property | Default | Meaning |
| --- | --- | --- |
| `quartz.scheduler.enabled` | `true` | Turn auto-configuration off without removing the jar. |
| `quartz.scheduler.name` | `quartzScheduler` | Scheduler name. Documents are scoped by this name, so two schedulers can share one database if the names differ. |
| `quartz.scheduler.instance-id` | empty (`AUTO`) | Leave empty. Each process then gets a new UUID. Do not pin the same id on every pod. |
| `quartz.scheduler.auto-startup` | `true` | Start scheduling after the context is up. |
| `quartz.scheduler.startup-delay` | `0s` | Wait before `start()`, so the rest of the application can finish booting. |
| `quartz.scheduler.wait-for-jobs-to-complete-on-shutdown` | `true` | On context close, wait for the job that is already running. |
| `quartz.scheduler.overwrite-existing-jobs` | `false` | When `false`, a trigger that is already stored is left as it is. Set `true` when the bean definition should replace the stored trigger on every deploy. |
| `quartz.scheduler.fail-fast-on-start` | `true` | A failed `scheduler.start()` fails the Spring context. `false` logs the error and leaves the scheduler thread to retry. |
| `quartz.scheduler.clustered` | `true` | Several JVMs share one Mongo database and only one of them fires a given trigger. |
| `quartz.scheduler.cluster-checkin-interval` | `15s` | How often this process renews its lease. |
| `quartz.scheduler.misfire-threshold` | `60s` | How late a fire may be before Quartz treats it as a misfire. |
| `quartz.scheduler.idle-wait-time` | `30s` | How long the scheduler thread sleeps when nothing is due. Minimum `1s`. |
| `quartz.scheduler.batch-time-window` | `0s` | Look-ahead window when acquiring a batch of triggers. |
| `quartz.scheduler.collection-prefix` | `qrtz_` | Prefix for every Quartz collection. |
| `quartz.scheduler.thread-pool.thread-count` | `10` | Platform worker threads, or the max number of virtual-thread jobs in flight. |
| `quartz.scheduler.thread-pool.thread-priority` | `5` | Used by the platform pool. Ignored when `virtual` is `true`. |
| `quartz.scheduler.thread-pool.virtual` | `false` | `true` selects `VirtualThreadPool`. |
| `quartz.scheduler.properties` | empty | Extra `org.quartz.*` keys. Applied first. The typed fields above win if both set the same thing. |

> **Important**
>
> `overwrite-existing-jobs` defaults to `false`. The first boot stores the trigger. A later change to the cron expression in code does not update MongoDB until you set this flag to `true`, or you reschedule from the `Scheduler` API. Use `true` while the schedule is owned by the bean definition. Use `false` when operators or the application change triggers at runtime and a redeploy must not put the old cron back.

To disable the scheduler in a test profile:

```yaml
quartz:
  scheduler:
    enabled: false
```

## Your first job

A job implements `org.quartz.Job`. It needs a public no-arg constructor. Collaborators are `@Autowired`. Quartz builds a **new instance for every fire**, autowires it, runs `execute`, then destroys it. Do not annotate the job with `@Component` and expect that singleton to be the instance that runs. Do not keep state in fields and expect the next fire to see it. Put that state in the `JobDataMap` or in MongoDB.

```java
package com.example.jobs;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class DailyReportJob implements Job {

  private static final Logger log = LoggerFactory.getLogger(DailyReportJob.class);

  @Autowired
  private ReportService reportService;

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    String region = context.getMergedJobDataMap().getString("region");
    log.info("Building report for {}, scheduled fire {}", region, context.getScheduledFireTime());
    reportService.build(region);
    context.setResult("ok:" + region);
  }
}
```

`getScheduledFireTime()`, `getFireTime()`, and `getNextFireTime()` return `Instant`.

Register the job and its trigger as beans. The bean method name is not the Quartz name. `withIdentity` is.

```java
package com.example.jobs;

import static org.quartz.CronScheduleBuilder.dailyAtHourAndMinute;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ReportSchedule {

  @Bean
  JobDetail dailyReportJob() {
    return newJob(DailyReportJob.class)
        .withIdentity("daily-report", "reports")
        .withDescription("Nightly report")
        .usingJobData("region", "APAC")
        .storeDurably()
        .requestRecovery(true)
        .build();
  }

  @Bean
  Trigger dailyReportTrigger() {
    return newTrigger()
        .forJob("daily-report", "reports")
        .withIdentity("daily-report-2am", "reports")
        .withSchedule(dailyAtHourAndMinute(2, 0))
        .build();
  }
}
```

`storeDurably()` keeps the job when it has no trigger. `requestRecovery(true)` asks the cluster to run the job again if this node dies mid-execution.

Static imports used in the snippets below:

```java
import static org.quartz.CalendarIntervalScheduleBuilder.calendarIntervalSchedule;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.CronScheduleBuilder.dailyAtHourAndMinute;
import static org.quartz.DateBuilder.IntervalUnit.MINUTES;
import static org.quartz.DateBuilder.futureDate;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;
```

## Schedules you can copy

### Cron

Six or seven fields, same syntax as Quartz: seconds, minutes, hours, day-of-month, month, day-of-week, optional year.

```java
@Bean
Trigger everyWeekdayAtEightFifteen() {
  return newTrigger()
      .forJob("daily-report", "reports")
      .withIdentity("weekdays-0815", "reports")
      .withSchedule(cronSchedule("0 15 8 ? * MON-FRI"))
      .build();
}
```

Helpers when you do not want to write the expression:

```java
dailyAtHourAndMinute(2, 0);
CronScheduleBuilder.weeklyOnDayAndHourAndMinute(DateBuilder.MONDAY, 9, 30);
CronScheduleBuilder.monthlyOnDayAndHourAndMinute(1, 0, 5);
```

Cron fields are interpreted in a time zone. The default is the JVM zone. Pin it when the schedule is "2:00 in Kolkata" no matter where the pod runs:

```java
.withSchedule(
    cronSchedule("0 0 2 * * ?")
        .inTimeZone(TimeZone.getTimeZone("Asia/Kolkata")))
```

`inTimeZone` still takes `java.util.TimeZone`. Fire times you read back are `Instant`.

### Fixed interval

`simpleSchedule` repeats every N milliseconds from the previous fire. It does not know about calendar months.

```java
@Bean
Trigger everyFiveMinutes() {
  return newTrigger()
      .forJob("daily-report", "reports")
      .withIdentity("every-5-min", "reports")
      .startAt(futureDate(1, MINUTES))
      .withSchedule(
          simpleSchedule()
              .withIntervalInMinutes(5)
              .repeatForever())
      .build();
}
```

A finite count includes the first fire. `withRepeatCount(9)` runs ten times.

```java
.withSchedule(
    simpleSchedule()
        .withIntervalInSeconds(30)
        .withRepeatCount(9))
```

### Calendar interval

Use this when the interval is "every month" or "every day" and the length of the month matters. An interval of one month from 31 January lands on the last valid day of February, not 2 or 3 March.

```java
@Bean
Trigger firstOfEachMonth() {
  return newTrigger()
      .forJob("daily-report", "reports")
      .withIdentity("monthly", "reports")
      .withSchedule(
          calendarIntervalSchedule()
              .withIntervalInMonths(1))
      .build();
}
```

`withInterval(1, DateBuilder.IntervalUnit.DAY)`, `withInterval(Period.ofWeeks(2))`, and `withInterval(Duration.ofHours(6))` are the same builder.

### Start and end

```java
newTrigger()
    .startNow()
    .endAt(Instant.parse("2026-12-31T18:30:00Z"))
    .withSchedule(simpleSchedule().withIntervalInHours(1).repeatForever())
    .build();
```

`startAt` and `endAt` take `Instant`. `futureDate(10, MINUTES)` returns an `Instant` ten minutes from now. `DateBuilder.tomorrowAt(6, 30, 0)` is 06:30:00 tomorrow in the JVM zone.

## Job data

Trigger data overlays job data. `context.getMergedJobDataMap()` is that merge. The job's own map is `context.getJobDetail().getJobDataMap()`.

Values are stored as BSON, not as Java serialization. These types round-trip:

* `String`, `Boolean`, numbers, `byte[]`
* `Instant` (comes back as `Instant`)
* `Duration`, `UUID`, enums
* nested `Map`, `List`, and arrays of those values

Anything else throws `IllegalArgumentException` when the job is saved. Do not put an entity, a Spring bean, or a lambda in the map. Pass an id and load the entity inside `execute`.

```java
@Bean
JobDetail invoiceJob() {
  return newJob(InvoiceJob.class)
      .withIdentity("invoice", "billing")
      .usingJobData("customerId", "C-100")
      .usingJobData("dryRun", Boolean.FALSE)
      .storeDurably()
      .build();
}
```

To remember something for the next fire, write the job map and annotate the class with `@PersistJobDataAfterExecution`. Quartz writes the map back after a successful execution.

```java
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class InvoiceJob implements Job {

  @Override
  public void execute(JobExecutionContext context) {
    JobDataMap data = context.getJobDetail().getJobDataMap();
    int run = data.containsKey("run") ? data.getInt("run") : 0;
    data.put("run", run + 1);
  }
}
```

`@DisallowConcurrentExecution` means a second fire waits until the first execution of **this job** finishes. Other jobs are not blocked. In a cluster the block is stored in MongoDB, so another node will not run the same job in parallel either.

Setter injection also works. A map entry whose key matches a setter is applied after autowiring:

```java
public class InvoiceJob implements Job {
  public void setCustomerId(String customerId) { /* ... */ }
}
```

with `usingJobData("customerId", "C-100")`.

## One job, many triggers, and firing on demand

A durable job can be scheduled later, or fired immediately, from any Spring bean. Inject `Scheduler`. That is safe in services and controllers. It is not safe inside a `SchedulerPlugin` bean, because the plugin is created while the scheduler is still being built.

```java
@Service
public class ReportScheduler {

  private final Scheduler scheduler;

  public ReportScheduler(Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  public void scheduleOnce(String region, Instant when) throws SchedulerException {
    JobDetail job = newJob(DailyReportJob.class)
        .withIdentity("report-" + region, "adhoc")
        .usingJobData("region", region)
        .build();

    Trigger trigger = newTrigger()
        .forJob(job)
        .withIdentity("report-" + region + "-once", "adhoc")
        .startAt(when)
        .build();

    scheduler.scheduleJob(job, trigger);
  }

  public void runNow(String region) throws SchedulerException {
    JobDataMap data = new JobDataMap();
    data.put("region", region);
    scheduler.triggerJob(jobKey("daily-report", "reports"), data);
  }
}
```

`triggerJob` runs the durable job once, now, without changing its cron trigger.

## Calendars

A Quartz calendar is a set of times to **exclude**. The Spring bean name is the calendar name.

```java
@Bean
org.quartz.Calendar bankHolidays() {
  HolidayCalendar calendar = new HolidayCalendar();
  calendar.addExcludedDate(
      LocalDate.of(2026, 1, 26).atStartOfDay(ZoneId.of("Asia/Kolkata")).toInstant());
  return calendar;
}

@Bean
Trigger reportSkippingHolidays() {
  return newTrigger()
      .forJob("daily-report", "reports")
      .withIdentity("daily-report-2am-holidays", "reports")
      .withSchedule(dailyAtHourAndMinute(2, 0))
      .modifiedByCalendar("bankHolidays")
      .build();
}
```

`HolidayCalendar`, `AnnualCalendar`, `WeeklyCalendar`, `MonthlyCalendar`, `DailyCalendar`, and `CronCalendar` are included. Excluded dates are `Instant`.

## Misfires

A trigger misfires when it is still waiting after `quartz.scheduler.misfire-threshold` (default 60 seconds) past its fire time. That happens during downtime, a long garbage collection, or when every worker is busy.

Attach the instruction on the schedule. The usual choice for cron is "do nothing and wait for the next time", so a job that should run at 02:00 does not also run at 02:07 after a short outage.

```java
.withSchedule(
    cronSchedule("0 0 2 * * ?")
        .withMisfireHandlingInstructionDoNothing())
```

| Instruction | On a cron, calendar, or daily-time trigger |
| --- | --- |
| `withMisfireHandlingInstructionFireAndProceed` | Fire once now, then continue. Smart policy for cron. |
| `withMisfireHandlingInstructionDoNothing` | Skip the missed fire. Wait for the next scheduled time. |
| `withMisfireHandlingInstructionIgnoreMisfires` | Fire every missed time as fast as the pool allows. |

Simple triggers also have `withMisfireHandlingInstructionFireNow` and the "next / now, with existing count / remaining count" variants. Those matter when `withRepeatCount` is set, because a misfire can consume repeat counts.

## Clustering

`quartz.scheduler.clustered` defaults to **true**.

Several application instances share one Mongo database and one scheduler name. A trigger is acquired with `findOneAndUpdate`, so only one instance fires it. Each process renews a lease every `cluster-checkin-interval`. If the process stops renewing, the lease expires and another instance may recover triggers that were acquired or jobs that were blocked.

Two requirements when clustering is on:

* **MongoDB is a replica set.** That includes Atlas and a single-node set created with `rs.initiate()`. The scheduler has to keep working when one Mongo node is gone. A standalone `mongod` is the right target only for a single local process, and then set `clustered: false`.
* **Clocks are synced with NTP.** Lease expiry is an `Instant` compared across JVMs. A node whose clock is minutes ahead will look dead, or will treat a live node as dead.

```yaml
# One laptop, standalone mongod
quartz:
  scheduler:
    clustered: false

# Two or more pods, Atlas or any replica set
quartz:
  scheduler:
    clustered: true
    cluster-checkin-interval: 15s
```

Leave `instance-id` unset. A fixed id shared by every replica defeats lease recovery.

`@DisallowConcurrentExecution` is cluster-wide. The `blocked` flag is in the job document, not in a JVM lock.

## Threads, including virtual threads

`thread-count` is how many jobs may run at once. The scheduler will not acquire more triggers than free slots.

Platform threads, the default:

```yaml
quartz:
  scheduler:
    thread-pool:
      thread-count: 10
      thread-priority: 5
      virtual: false
```

Virtual threads, for jobs that spend their time on HTTP or MongoDB:

```yaml
quartz:
  scheduler:
    thread-pool:
      thread-count: 50
      virtual: true
```

`thread-count` is still a cap. Without it, `blockForAvailableThreads()` could not tell the scheduler how many triggers to fetch, and a slow downstream would be called without limit. Thread priority is ignored for virtual threads.

## History logs

`LoggingJobHistoryPlugin` and `LoggingTriggerHistoryPlugin` log fire, success, failure, veto, misfire, and completion through SLF4J. Timestamps in the messages are ISO-8601 `Instant` strings. They are not registered unless you declare the beans. The Spring bean name is the plugin name passed to `initialize`.

```java
@Bean
LoggingJobHistoryPlugin jobHistory() {
  return new LoggingJobHistoryPlugin();
}

@Bean
LoggingTriggerHistoryPlugin triggerHistory() {
  return new LoggingTriggerHistoryPlugin();
}
```

Do not inject `Scheduler` into these beans. The factory calls `initialize(name, scheduler)` after the scheduler exists.

Message templates use `java.text.MessageFormat`. A single quote in a template must be written as `''`. Job messages use `{0}` job name, `{1}` job group, `{2}` now, `{3}` trigger name, `{4}` trigger group, `{5}` previous fire, `{6}` next fire, `{7}` refire count, `{8}` result or exception message. Trigger messages use `{0}` trigger name, `{1}` trigger group, `{2}` previous fire, `{3}` next fire, `{4}` now, `{5}` job name, `{6}` job group, `{7}` refire count, and on completion `{8}` the instruction enum and `{9}` a short label.

```java
@Bean
LoggingJobHistoryPlugin jobHistory() {
  LoggingJobHistoryPlugin plugin = new LoggingJobHistoryPlugin();
  plugin.setJobSuccessMessage("Job {1}.{0} finished at {2}: {8}");
  return plugin;
}
```

## Listeners and plugins

A plugin is the supported way to attach a listener at startup. Implement `SchedulerPlugin` and `JobListener` (or `TriggerListener`). Register a bean. The bean name becomes `getName()`.

```java
public class MetricsJobListener implements SchedulerPlugin, JobListener {

  private static final Logger log = LoggerFactory.getLogger(MetricsJobListener.class);

  private String name;

  @Override
  public void initialize(String pluginName, Scheduler scheduler) throws SchedulerException {
    this.name = pluginName;
    scheduler.getListenerManager().addJobListener(this, EverythingMatcher.allJobs());
  }

  @Override
  public void start() {}

  @Override
  public void shutdown() {}

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void jobToBeExecuted(JobExecutionContext context) {}

  @Override
  public void jobExecutionVetoed(JobExecutionContext context) {}

  @Override
  public void jobWasExecuted(JobExecutionContext context, JobExecutionException exception) {
    if (exception != null) {
      log.warn("Job {} failed", context.getJobDetail().getKey(), exception);
    }
  }
}
```

```java
@Bean
MetricsJobListener metricsJobListener() {
  return new MetricsJobListener();
}
```

`EverythingMatcher.allJobs()` and `KeyMatcher.keyEquals(jobKey("daily-report", "reports"))` are the usual matchers. `GroupMatcher.groupEquals("reports")` selects a group.

From a service, after the context is up, the same registration is `scheduler.getListenerManager().addJobListener(...)`.

`QuartzSchedulerCustomizer` is the hook when you need to change the factory before it starts, in the same role as Spring Boot's `SchedulerFactoryBeanCustomizer`.

```java
@Bean
QuartzSchedulerCustomizer quartzSchedulerCustomizer() {
  return factory -> factory.setStartupDelay(Duration.ofSeconds(15));
}
```

## Pause, resume, reschedule, delete

```java
scheduler.pauseJob(jobKey("daily-report", "reports"));
scheduler.resumeJob(jobKey("daily-report", "reports"));

scheduler.pauseTrigger(triggerKey("daily-report-2am", "reports"));
scheduler.resumeTrigger(triggerKey("daily-report-2am", "reports"));

Trigger replacement = newTrigger()
    .forJob("daily-report", "reports")
    .withIdentity("daily-report-2am", "reports")
    .withSchedule(cronSchedule("0 0 3 * * ?"))
    .build();
scheduler.rescheduleJob(triggerKey("daily-report-2am", "reports"), replacement);

scheduler.unscheduleJob(triggerKey("daily-report-2am", "reports"));
scheduler.deleteJob(jobKey("daily-report", "reports"));

scheduler.interrupt(jobKey("daily-report", "reports"));
```

`interrupt` only works when the job implements `InterruptableJob` and the execution is on this JVM.

```java
public class DailyReportJob implements InterruptableJob {
  private final AtomicBoolean stop = new AtomicBoolean();

  @Override
  public void execute(JobExecutionContext context) {
    while (!stop.get()) {
      // work that can notice the flag
    }
  }

  @Override
  public void interrupt() {
    stop.set(true);
  }
}
```

`scheduler.checkExists(jobKey(...))` and `scheduler.getCurrentlyExecutingJobs()` are the read APIs. Currently executing jobs are those running in **this** process, not every node in the cluster.

## How jobs are created

`AutowireCapableJobFactory` replaces Spring's `SpringBeanJobFactory`.

1. The job class name is stored as a string (`jobClass`), not as a serialized `Class`.
2. On fire, the class is loaded with the Spring application class loader.
3. `AutowireCapableBeanFactory.createBean` builds the instance, so `@Autowired` and `@Value` work.
4. Merged job data is applied to setters.
5. After `execute` returns, the instance is destroyed (`DisposableBean` and `@PreDestroy` run).

Because the instance is thrown away, `@Scope("prototype")` on the job class is optional. You still should not make the job a singleton you also inject elsewhere and expect Quartz to reuse that singleton. Quartz will not. It always calls `createBean`.

A job class must be public, with a public no-arg constructor, and it must be loadable by the application that fires it. In a cluster every node needs the same class on the classpath. If one node stores a job whose class the other node cannot load, that trigger is moved to `ERROR` until a node that can load the class recovers it.

## What is stored in MongoDB

Collections are created in the application database. The default prefix is `qrtz_`. Change it with `quartz.scheduler.collection-prefix` when several schedulers must not share collections. Documents are also filtered by scheduler name, so the prefix is a second isolation knob, not the only one.

| Collection | Contents |
| --- | --- |
| `qrtz_jobs` | One document per job. `jobClass` is the fully qualified class name. |
| `qrtz_triggers` | One document per trigger. Next, previous, start, and end fire times are instants. Acquired triggers carry `leaseOwner` and `leaseExpiresAt`. |
| `qrtz_calendars` | Excluded time ranges. |
| `qrtz_paused_trigger_groups` | Trigger groups that are paused. |
| `qrtz_paused_job_groups` | Job groups that are paused. |
| `qrtz_leases` | One lease per live process. A TTL index on `expiresAt` drops stale leases. Recovery also reads `leaseExpiresAt` on the trigger, so a missing TTL index does not disable recovery. |
| `qrtz_locks` | The cluster lock (`TRIGGER_ACCESS`), taken with `findOneAndUpdate`. |

Unique indexes:

* jobs: `schedName + name + group`
* triggers: `schedName + name + group`
* leases: `schedName + owner`
* locks: `schedName + lockName`

There is nothing to create by hand. Starting the application creates the collections and indexes.

## Time: Instant, ZoneId, and cron

Public scheduler times are `java.time`:

| API | Type |
| --- | --- |
| `Trigger.getNextFireTime`, `getPreviousFireTime`, `getFinalFireTime` | `Instant` |
| `TriggerBuilder.startAt`, `endAt` | `Instant` |
| `JobExecutionContext.getFireTime`, `getScheduledFireTime` | `Instant` |
| `Scheduler.startDelayed` | `Duration` |
| `DateBuilder.futureDate`, `tomorrowAt`, `evenMinuteDate` | returns `Instant` |
| `DateBuilder.inTimeZone` | `ZoneId` |
| `HolidayCalendar.addExcludedDate` | `Instant` |
| `CronScheduleBuilder.inTimeZone` | `TimeZone` |
| `CalendarIntervalScheduleBuilder.inTimeZone` | `TimeZone` |

`DateBuilder` is the helper that turns a wall-clock time into an `Instant`. `tomorrowAt` and `todayAt` use the JVM zone. A zoned instant is easiest to build with `java.time` itself, then pass to `startAt`.

```java
Instant tomorrowMorning = DateBuilder.tomorrowAt(6, 30, 0);

ZoneId kolkata = ZoneId.of("Asia/Kolkata");
Instant tomorrowInKolkata = ZonedDateTime.now(kolkata)
    .plusDays(1)
    .withHour(6)
    .withMinute(30)
    .withSecond(0)
    .withNano(0)
    .toInstant();
```

`DateBuilder.newDateInTimezone(zone)` works when every field is set. `build()` fills any missing field from the current time, and that fill replaces the hour, minute, and second as well. Set year, month, day, and time together:

```java
Instant republicDayMorning = DateBuilder.newDateInTimezone(ZoneId.of("Asia/Kolkata"))
    .inYear(2026)
    .inMonth(1)
    .onDay(26)
    .atHourMinuteAndSecond(6, 30, 0)
    .build();
```

Cron expressions answer "which wall-clock fields match". That answer depends on a zone, so cron keeps `TimeZone`. An `Instant` is the result after the zone is applied.

## Running without Spring Boot auto-configuration

Set `quartz.scheduler.enabled=false` and build the scheduler yourself when you want full control. You still only get `MongoJobStore`.

```java
@Bean
Scheduler scheduler(MongoClient mongoClient) throws SchedulerException {
  Properties props = new Properties();
  props.setProperty("org.quartz.scheduler.instanceName", "quartzScheduler");
  props.setProperty("org.quartz.scheduler.instanceId", "AUTO");
  props.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
  props.setProperty("org.quartz.threadPool.threadCount", "10");
  props.setProperty("org.quartz.jobStore.class", "org.quartz.impl.mongodb.MongoJobStore");
  props.setProperty("org.quartz.jobStore.dbName", "appdb");
  props.setProperty("org.quartz.jobStore.collectionPrefix", "qrtz_");
  props.setProperty("org.quartz.jobStore.isClustered", "false");
  props.setProperty("org.quartz.jobStore.misfireThreshold", "60000");

  StdSchedulerFactory factory = new StdSchedulerFactory();
  factory.setMongoClient(mongoClient);
  factory.initialize(props);
  Scheduler scheduler = factory.getScheduler();
  scheduler.start();
  return scheduler;
}
```

`org.quartz.jobStore.class` is accepted and then ignored if it names something else. The factory always constructs `MongoJobStore`. Passing `setMongoClient` means shutdown will not close that client.

If you do use auto-configuration, prefer `quartz.scheduler.*` over raw `org.quartz.*` keys. Raw keys belong in `quartz.scheduler.properties` and lose to the typed fields when both are set.

## Licence

Apache License 2.0. Quartz Scheduler is the work of Terracotta and later contributors, including IBM. This tree keeps that license and removes every job store except MongoDB.

## Authors and acknowledgment

**Rajveer Singh**. If you find a bug or need a hand getting a job to fire, email raj14.1984@gmail.com. A star on the repo helps other people find a Quartz that already speaks MongoDB.

## Credits and references

* Quartz Scheduler, https://www.quartz-scheduler.org/
* This repository, https://github.com/officiallysingh/quartz
