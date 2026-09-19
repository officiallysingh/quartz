# Test Strategy: Quartz Fork Parity (vs Official Quartz)

This document is a **black-box / integration test strategy** for confirming that this fork (`quartz-scheduler` with Instant APIs, Mongo job store, in-tree Spring Boot auto-config) behaves like **official Quartz** for scheduling semantics, APIs, and lifecycle — **except persistence** (Mongo replaces JDBC; collection/schema shape is different by design). RAM / in-memory job store is not supported.

Use it against a **Spring Boot app** that depends on this jar and has **removed** `org.quartz-scheduler:quartz` and `spring-boot-starter-quartz`.

---

## 1. Goals and non-goals

### Goals

1. Prove **behavioral parity** with upstream Quartz for: job/trigger construction, fire timing, misfires, calendars, listeners, concurrency annotations, interrupt, pause/resume, standby/shutdown, clustering *semantics*, and Spring Boot wiring.
2. Prove **this fork’s intentional differences** work correctly: `java.time.Instant` public fire times, `Duration`/`Period`/`DayOfWeek` builders, `VirtualThreadPool`, Mongo job store, Spring `quartz.scheduler.*` auto-config.
3. Catch regressions early with a **scenario matrix** that can be executed as unit, integration, and multi-node tests.

### Non-goals (persistence exception)

Do **not** require byte-for-byte or schema parity with JDBC Quartz:

| Official JDBC | This fork (Mongo) | Verdict |
|---|---|---|
| ~11 normalized tables | Fewer collections (`jobs`, `triggers`, `calendars`, pause groups, `scheduler_state`, `locks`) | Expected |
| Column-level trigger fields | Serialized trigger payloads | Expected |
| `fired_triggers` table | No equivalent table (in-process / different design) | Expected |
| `JobStoreTX` / CMT / datasources | N/A | Out of scope |

**Still in scope for Mongo:** durable jobs survive restart; clustered failover; recovery jobs; pause groups; calendars; overwrite/reschedule; check-in / locks. Assert **scheduler behavior**, not table layout.

### Explicitly out of scope (removed / rejected in this fork)

Do not test as product features:

- JDBC / RDBMS job stores and `spring-boot-starter-quartz` JDBC wiring
- JMX export/proxy
- RMI export/proxy
- JTA / `wrapJobExecutionInUserTransaction` / `@ExecuteInJTATransaction`
- Management REST service
- Built-in plugins package (`XMLSchedulingDataProcessor`, History, ShutdownHook, etc.)
- XML job scheduling plugins
- Terracotta / non-Mongo clustering backends
- `@Durable` annotation (durability is `JobBuilder.storeDurably()` / `JobDetail.isDurable()` only)

**Custom `SchedulerPlugin` SPI** *is* in scope (interface + property wiring), even though stock plugins are gone.

---

## 2. Test environments

Run the same scenario packs against:

| Env | Job store | Cluster | Thread pool | Purpose |
|---|---|---|---|---|
| A | `MONGODB` (standalone) | No | Simple | Fast functional parity |
| B | `MONGODB` (standalone) | No | Virtual (`quartz.scheduler.thread-pool.virtual=true`) | VT pool parity |
| C | `MONGODB` (standalone) | `clustered=false` | Simple | Persistence across restart |
| D | `MONGODB` replica set | `clustered=true`, 2+ app instances | Simple | Cluster / failover |
| E | Spring Boot auto-config (app `MongoClient`) | As configured | Both | Boot bean registration + DI jobs |

**Baseline for “works like original”:** Env A/B cover scheduling semantics on Mongo. Env C/D replace JDBC persistence tests with Mongo behavioral equivalents.

**Infrastructure requirements:**

- JDK matching the fork (Java 26 toolchain in this repo).
- MongoDB for C/D (replica set + roughly synced clocks for clustering).
- Two JVMs / containers for Env D (distinct `instanceId`, same `quartz.scheduler.name` / instance name, same DB).
- Optional: NTP check on cluster hosts (document clock skew failures separately).

---

## 3. Pass / fail criteria

For each scenario:

1. **Functional:** Observed fire times, counts, order, JobDataMap, and API return values match expected Quartz rules (within documented tolerance).
2. **Timing tolerance:** Prefer asserting **ordering and counts** over exact wall-clock millis. Where exactness matters (cron next fire, calendar exclusion), assert Instant equality or “within misfireThreshold”.
3. **API surface:** Methods that exist on `Scheduler` / builders return without unexpected exceptions; Instant-based getters are non-null when a Date-based equivalent would have been.
4. **Spring:** Jobs receive Spring beans via `AutowireCapableJobFactory`; lifecycle follows `auto-startup`, `startup-delay`, `wait-for-jobs-to-complete-on-shutdown`.
5. **Persistence (Mongo only):** After process kill/restart (Env C) or node failure (Env D), durable jobs/triggers and recovery semantics hold — **not** collection-count parity with JDBC.

Record: scenario ID, env, steps, expected, actual, evidence (logs, counters, Mongo docs if useful).

---

## 4. How to construct jobs and triggers (must cover every path)

Official Quartz apps build schedules many ways. Exercise **all** construction paths below in Env A, then re-run persistence-sensitive ones in Env C.

### 4.1 Programmatic builders (primary)

| ID | Construction | Assert |
|---|---|---|
| J-01 | `JobBuilder.newJob(T.class).withIdentity(...).build()` | JobDetail type, key, durability default |
| J-02 | `.withDescription`, `.requestRecovery()`, `.storeDurably()`, `.usingJobData` / `.setJobData` | Fields round-trip via `getJobDetail` |
| J-03 | Job without Trigger via `scheduler.addJob(..., true)` then later `scheduleJob(trigger)` | Durable / non-durable awaiting rules |
| T-01 | `TriggerBuilder.newTrigger().startNow().withSchedule(...).forJob(...).build()` | Schedule + association |
| T-02 | `.startAt(Instant)` / `.endAt(Instant)` / `.withPriority` / `.modifiedByCalendar` / `.usingJobData` | Instant fire window, priority order, calendar name |
| T-03 | `scheduleJob(job, trigger)` single call | Both stored, first fire correct |
| T-04 | `scheduleJob(job, Set<Trigger>, replace)` | Multiple triggers per job |
| T-05 | `scheduleJobs(Map<JobDetail, Set<Trigger>>, replace)` | Batch schedule |
| T-06 | Pre-built `OperableTrigger` / impl classes (if used) | Same as builder path |

### 4.2 `DateBuilder` / `Instants` helpers

| ID | API | Assert |
|---|---|---|
| DB-01 | `evenMinuteDate`, `evenSecondDate`, `evenHourDate` (Instant) | Alignment |
| DB-02 | `futureDate(int, IntervalUnit)` all units: MS, SEC, MIN, HOUR, DAY, WEEK, MONTH, YEAR | Relative Instant |
| DB-03 | `futureDate(Duration)` / `futureDate(Period)` / `futureDate(TemporalAmount)` | Same semantics as unit overloads |
| DB-04 | `todayAt` / `tomorrowAt` / `dateOf` with `LocalTime` / `Month` | Zone-correct Instant |
| DB-05 | `inTimeZone(ZoneId)` / `newDateInTimezone` | ZoneId path (not only TimeZone) |
| DB-06 | DOW constants + `toQuartzDayOfWeek` / `toDayOfWeek` | Mapping 1=Sunday … 7=Saturday |
| DB-07 | `Instants.now`, `ofEpochMilli`, `toDate`/`fromDate`, `plusMillis` | Bridge consistency |

### 4.3 Schedule builders (see §5 for type-specific matrices)

- `SimpleScheduleBuilder`
- `CronScheduleBuilder`
- `CalendarIntervalScheduleBuilder`
- `DailyTimeIntervalScheduleBuilder`
- Including `withInterval(Duration)` / `withInterval(Period)` overloads where present

### 4.4 Spring Boot bean registration

| ID | Pattern | Assert |
|---|---|---|
| SB-01 | `@Bean JobDetail` + `@Bean Trigger` referencing job | Auto-registered on startup |
| SB-02 | Trigger-only bean with `forJob` key matching JobDetail bean | Linked correctly |
| SB-03 | Multiple triggers → one job | Both fire |
| SB-04 | `@Bean Calendar` + trigger `modifiedByCalendar` | Calendar applied |
| SB-05 | `overwrite-existing-jobs=true` vs `false` | Reschedule vs leave existing (Mongo Env C especially) |
| SB-06 | Job class extending nothing / implementing `Job` with constructor DI | Autowired dependencies non-null |
| SB-07 | `QuartzSchedulerCustomizer` bean | Customizations applied before start |
| SB-08 | `quartz.scheduler.properties` map overlay (`org.quartz.*`) | Applied first; typed Duration fields win |
| SB-09 | `quartz.scheduler.enabled=false` | No scheduler bean / no scheduling |
| SB-10 | No `MongoClient` bean | Auto-config does not create a scheduler |

### 4.5 Factory paths (non-Boot or hybrid)

| ID | Path | Assert |
|---|---|---|
| F-01 | `StdSchedulerFactory` default / `quartz.properties` on classpath | Scheduler starts |
| F-02 | `StdSchedulerFactory(Properties)` programmatic | Same |
| F-03 | `DirectSchedulerFactory` create with explicit JobStore / ThreadPool / plugins map | Works |
| F-04 | `SchedulerRepository` lookup by name | Same instance |

### 4.6 Imperative scheduling after start

| ID | API | Assert |
|---|---|---|
| API-01 | `triggerJob(JobKey)` / with JobDataMap | Immediate fire |
| API-02 | `rescheduleJob` | New next fire; old schedule gone |
| API-03 | `unscheduleJob` / `unscheduleJobs` | No further fires; job may remain |
| API-04 | `deleteJob` / `deleteJobs` | Job + triggers removed |
| API-05 | `addJob` + `checkExists` | Existence checks |
| API-06 | `clear()` | Empty scheduler |
| API-07 | Query APIs: `getJobKeys`/`getTriggerKeys` with `GroupMatcher`, `getTriggersOfJob`, `getCurrentlyExecutingJobs`, `getMetaData` | Correct sets |

---

## 5. Trigger types (full matrix)

For **each** trigger type, cover: create → schedule → fire → query next/previous Instant → misfire → endAt → unschedule.

### 5.1 SimpleTrigger

| ID | Scenario | Notes |
|---|---|---|
| ST-01 | One-shot (`repeatCount=0`) | Single fire |
| ST-02 | `withIntervalInMilliseconds/Seconds/Minutes/Hours` | Interval accuracy |
| ST-03 | `withInterval(Duration)` | Parity with unit API |
| ST-04 | `repeatForever` | Stops only on unschedule/shutdown/endAt |
| ST-05 | Finite `withRepeatCount(n)` | Exactly n+1 fires (initial + repeats) |
| ST-06 | Priority: two simples, same start, different priority | Higher priority first when pool contended |
| ST-07 | `startAt` in past (with/without misfire policies) | See §7 |
| ST-08 | `endAt` before remaining repeats | Stops at end |
| ST-09 | JobDataMap on trigger merged into execution context | Merge rules |

**Misfire instructions (each):** `SMART_POLICY`, `IGNORE_MISFIRE_POLICY`, `FIRE_NOW`, `RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT`, `RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT`, `RESCHEDULE_NEXT_WITH_EXISTING_COUNT`, `RESCHEDULE_NEXT_WITH_REMAINING_COUNT`.

### 5.2 CronTrigger

| ID | Scenario |
|---|---|
| CR-01 | Standard 6-field expressions (sec min hour dom mon dow) |
| CR-02 | Special chars: `*`, `?`, `-`, `,`, `/`, `L`, `W`, `#` |
| CR-03 | `CronScheduleBuilder.cronSchedule` / `dailyAtHourAndMinute` / `atHourAndMinuteOnGivenDaysOfWeek` / `weeklyOnDayAndHourAndMinute` / `monthlyOnDayAndHourAndMinute` |
| CR-04 | `inTimeZone(TimeZone)` — DST spring-forward and fall-back days |
| CR-05 | Invalid expression → clear error at build/schedule |
| CR-06 | `CronExpression` next/previous Instant helpers used by app code |
| CR-07 | End time / start time window |

**Misfire:** `SMART_POLICY`, `IGNORE_MISFIRE_POLICY`, `FIRE_ONCE_NOW`, `DO_NOTHING`.

### 5.3 CalendarIntervalTrigger

| ID | Scenario |
|---|---|
| CI-01 | Interval unit DAY / WEEK / MONTH / YEAR / HOUR / MINUTE / SECOND |
| CI-02 | `withInterval(Duration)` and `withInterval(Period)` |
| CI-03 | `preserveHourOfDayAcrossDaylightSavings` true/false |
| CI-04 | `skipDayIfHourDoesNotExist` true/false (DST gap) |
| CI-05 | `inTimeZone(TimeZone)` month-end edge (Jan 31 + 1 month) |
| CI-06 | Repeat count / forever / endAt |

**Misfire:** `FIRE_ONCE_NOW`, `DO_NOTHING`, smart/ignore.

### 5.4 DailyTimeIntervalTrigger

| ID | Scenario |
|---|---|
| DT-01 | `startingDailyAt` / `endingDailyAt` (`TimeOfDay`) |
| DT-02 | `endingDailyAfterCount` |
| DT-03 | `onMondayThroughFriday` / `onSaturdayAndSunday` / `onEveryDay` |
| DT-04 | `onDaysOfTheWeek(DayOfWeek...)` java.time overload |
| DT-05 | Interval seconds/minutes/hours + `withInterval(Duration)` |
| DT-06 | `withRepeatCount` |
| DT-07 | Days crossing midnight / timezone |

**Misfire:** same family as cron/calendar interval.

### 5.5 Cross-cutting trigger behavior

| ID | Scenario |
|---|---|
| X-01 | Same job, mixed trigger types (Simple + Cron) |
| X-02 | Trigger state transitions: `NORMAL`, `PAUSED`, `COMPLETE`, `ERROR`, `BLOCKED`, `NONE` |
| X-03 | `resetTriggerFromErrorState` |
| X-04 | `getPreviousFireTime` / `getNextFireTime` / `getFireTimeAfter` as Instant |
| X-05 | Trigger without job (must fail clearly) |
| X-06 | Duplicate key → `ObjectAlreadyExistsException` (or overwrite when configured) |

---

## 6. Jobs, annotations, JobDataMap, interrupt

| ID | Scenario | Assert |
|---|---|---|
| JOB-01 | Stateless `Job` execute | Invoked with correct context |
| JOB-02 | `@DisallowConcurrentExecution` | Second fire waits / blocked state while first runs |
| JOB-03 | `@PersistJobDataAfterExecution` | Mutated JobDataMap persisted for next fire (Mongo: after restart in Env C) |
| JOB-04 | Both annotations together (and deprecated `StatefulJob`) | Combined semantics |
| JOB-05 | Durable job with no triggers survives restart (Env C) | Still in store |
| JOB-06 | Non-durable job without triggers | Not retained after complete |
| JOB-07 | `requestRecovery=true` + kill mid-execution (Env C/D) | Recovering execution after restart/failover |
| JOB-08 | `InterruptableJob` + `scheduler.interrupt(JobKey)` / `interrupt(fireInstanceId)` | `interrupt()` called; `UnableToInterruptJobException` path |
| JOB-09 | Job throws `JobExecutionException` with refire / unschedule flags | Flags honored |
| JOB-10 | Unchecked exception from job | Listener error callbacks; trigger error handling |
| JOB-11 | JobDataMap types: String, int, long, float, double, boolean, JobDataMap nested, Serializable custom | Round-trip (Mongo serialization critical) |
| JOB-12 | Concurrent modification of JobDataMap without persist annotation | Does not leak to next fire |
| JOB-13 | Long-running job with small thread pool | Queueing / no lost triggers under load |
| JOB-14 | `getCurrentlyExecutingJobs` during run | Non-empty; cleared after |

---

## 7. Misfire and timing / idle behavior

| ID | Scenario |
|---|---|
| MF-01 | Force misfire: `standby` or stop acquire long enough past `misfireThreshold`, then resume |
| MF-02 | Each misfire instruction per trigger type (§5) — count fires after recovery |
| MF-03 | `misfireThreshold` property change effect |
| MF-04 | Scheduler `idleWaitTime` / batch acquisition props (if set via `quartz.scheduler.properties`) |
| MF-05 | System clock jump forward / backward (document; optional chaos) |
| MF-06 | Dense schedule (many triggers due) with `threadCount=1` | No silent drop; eventually catches up per policy |

---

## 8. Calendars

Register via API and via Spring `@Bean Calendar`.

| ID | Calendar type | Scenario |
|---|---|---|
| CAL-01 | `HolidayCalendar` | Excluded dates skipped |
| CAL-02 | `WeeklyCalendar` | Weekend (or custom) excluded |
| CAL-03 | `MonthlyCalendar` | Days of month excluded |
| CAL-04 | `AnnualCalendar` | Annual dates excluded |
| CAL-05 | `DailyCalendar` | Time-of-day window invert/exclude |
| CAL-06 | `CronCalendar` | Cron-based exclusions |
| CAL-07 | `BaseCalendar` base / nesting (base calendar chain) | Combined exclusions |
| CAL-08 | `addCalendar(..., updateTriggers=true/false)` | Next fire updates |
| CAL-09 | `deleteCalendar` / `getCalendarNames` | Lifecycle |
| CAL-10 | Trigger `modifiedByCalendar` missing calendar | Error or documented behavior |
| CAL-11 | Persist calendars across restart (Env C) | Still applied |

---

## 9. Pause, resume, standby, shutdown

| ID | Scenario |
|---|---|
| LC-01 | `pauseJob` / `resumeJob` | No fires while paused |
| LC-02 | `pauseJobs(GroupMatcher)` / `resumeJobs` | Group scope |
| LC-03 | `pauseTrigger` / `resumeTrigger` / `pauseTriggers` / `resumeTriggers` | Trigger scope |
| LC-04 | `pauseAll` / `resumeAll` | Global |
| LC-05 | `getPausedTriggerGroups` | Accurate |
| LC-06 | Pause while job executing | Current finishes; no new until resume |
| LC-07 | `standby` / `isInStandbyMode` / `start` again | No acquisition in standby |
| LC-08 | `startDelayed` | No fires before delay |
| LC-09 | `shutdown(false)` mid-job | May interrupt per config |
| LC-10 | `shutdown(true)` / Boot `wait-for-jobs-to-complete-on-shutdown=true` | In-flight complete |
| LC-11 | `interruptJobsOnShutdown` / `WithWait` via properties | Documented interrupt behavior |
| LC-12 | Double `start` / `shutdown` | Idempotent / safe |
| LC-13 | `isStarted` / `isShutdown` flags | Correct |

---

## 10. Listeners and matchers

### 10.1 Registration

| ID | Path |
|---|---|
| L-01 | Runtime: `scheduler.getListenerManager().addJobListener(...)` |
| L-02 | Runtime: `addTriggerListener`, `addSchedulerListener` |
| L-03 | With matcher: `addJobListener(listener, matcher)` / multiple matchers |
| L-04 | Config-time: `org.quartz.jobListener.NAME.class` / `triggerListener` via properties |
| L-05 | Remove / get listeners by name |

### 10.2 Callback coverage

**JobListener:** `jobToBeExecuted`, `jobExecutionVetoed`, `jobWasExecuted` (with exception).

**TriggerListener:** `triggerFired`, `vetoJobExecution` (true/false), `triggerMisfired`, `triggerComplete`.

**SchedulerListener:** job/trigger scheduled, unscheduled, finalized, paused/resumed, schduler error, starting, started, in standby, shutting down, shutdown, scheduling data cleared, etc. — hit each by driving the corresponding API.

| ID | Scenario |
|---|---|
| L-10 | Veto via TriggerListener → job not run; JobListener `jobExecutionVetoed` |
| L-11 | `JobChainingJobListener` chains job B after A |
| L-12 | Broadcast listeners (`BroadcastJobListener`, etc.) fan-out |
| L-13 | *Support base classes used as adapters |

### 10.3 Matcher matrix

| ID | Matcher |
|---|---|
| M-01 | `EverythingMatcher.allJobs` / `allTriggers` |
| M-02 | `KeyMatcher.keyEquals` |
| M-03 | `NameMatcher` equals / startsWith / endsWith / contains |
| M-04 | `GroupMatcher` same operators + `anyJobGroup` / `anyTriggerGroup` |
| M-05 | `AndMatcher` / `OrMatcher` / `NotMatcher` combinations |
| M-06 | Listener only fires for matched keys (negative tests) |

---

## 11. Plugins (SPI only)

| ID | Scenario |
|---|---|
| P-01 | Custom `SchedulerPlugin`: `initialize` / `start` / `shutdown` called in order |
| P-02 | Wire via `org.quartz.plugin.myPlugin.class=...` + bean-style properties |
| P-03 | Wire via `DirectSchedulerFactory` plugin map |
| P-04 | Plugin failure on initialize → scheduler start fails clearly |
| P-05 | Confirm **absence** of XML/History stock plugins (negative: ClassNotFound / not on classpath) |

Do not plan tests for removed XML job-file loading; migrate those apps to builders or Spring beans.

---

## 12. Thread pools

| ID | Scenario |
|---|---|
| TP-01 | `SimpleThreadPool`: `threadCount`, priority, name prefix, daemon, context classloader inherit |
| TP-02 | Saturate pool: N long jobs, M waiting triggers — eventually all run |
| TP-03 | `VirtualThreadPool`: concurrent cap via `threadCount`; many short jobs |
| TP-04 | Spring `thread-pool.virtual=true` selects VirtualThreadPool |
| TP-05 | `thread-pool.virtual=false` + count/priority → SimpleThreadPool |
| TP-06 | `ZeroSizeThreadPool` edge (if used): no execution / documented behavior |
| TP-07 | Compare throughput/ordering under load Simple vs Virtual (soft assert) |

---

## 13. Clustering (Mongo) — Env D

Parity target: **same operational semantics as JDBC clustered Quartz**, not same tables.

| ID | Scenario | Assert |
|---|---|---|
| CL-01 | Two nodes, same scheduler name, `instanceId=AUTO`, `clustered=true` | Both check in; distinct instance IDs |
| CL-02 | Schedule on node A; fires on exactly one node per fire | No double execution |
| CL-03 | Kill node holding a long `@DisallowConcurrentExecution` job | Failover / recovery per recovery flag |
| CL-04 | Pause on one node | Visible / honored cluster-wide (pause groups persisted) |
| CL-05 | `clusterCheckinInterval` effect (shorten for test) | Failed node detected within bound |
| CL-06 | Split-brain / network partition (optional chaos) | Document lock behavior; no silent dual-run if locks hold |
| CL-07 | Rolling restart both nodes | No lost durable triggers |
| CL-08 | Non-clustered Mongo (`clustered=false`) two processes same DB | Document risk (expect conflicts) — negative control |
| CL-09 | Locks + `scheduler_state` collections populated | Presence OK; don’t require JDBC shape |
| CL-10 | Hostname / `SimpleInstanceIdGenerator` / system-property generator | Instance ID sources |

---

## 14. Persistence (Mongo) — behavioral only

| ID | Scenario |
|---|---|
| PS-01 | Cold start creates expected collections with prefix |
| PS-02 | Durable job + triggers survive JVM restart |
| PS-03 | Calendar + pause groups survive restart |
| PS-04 | `@PersistJobDataAfterExecution` survives restart |
| PS-05 | `overwrite-existing-jobs` on Boot restart |
| PS-06 | Custom `collection-prefix` isolation between apps |
| PS-07 | `MongoJobStore` uses Boot `MongoClient` (does not close it) |
| PS-08 | Missing `MongoClient` bean → auto-config does not start a scheduler |
| PS-09 | Large JobDataMap / many jobs (scale smoke) |
| PS-10 | Serialize all trigger types to Mongo and reload next fire Instant correctly |

**Do not fail** because collection count ≠ JDBC table count.

---

## 15. Spring Boot integration (full)

| ID | Scenario |
|---|---|
| BOOT-01 | App starts with only this dependency (no starter-quartz) |
| BOOT-02 | `quartz.scheduler.*` YAML binding (kebab-case) |
| BOOT-03 | `auto-startup=false` → manual `scheduler.start()` |
| BOOT-04 | `startup-delay` |
| BOOT-05 | `name` / `instance-id` |
| BOOT-06 | Feature toggle `enabled` |
| BOOT-07 | Mongo props: uri, database, prefix; cluster props at `quartz.scheduler.clustered` / `cluster-checkin-interval`; `misfire-threshold` as Duration |
| BOOT-08 | Fallback database from `spring.mongodb` / `spring.data.mongodb` |
| BOOT-09 | Job autowiring: services, `@Value`, optional `@Transactional` on collaborators (not Quartz JTA) |
| BOOT-10 | Context shutdown order with Spring |
| BOOT-11 | Multiple `JobDetail`/`Trigger` `@Configuration` classes |
| BOOT-12 | Actuator / health (if you expose custom indicators) — optional |

---

## 16. Concurrency, load, and correctness under stress

| ID | Scenario |
|---|---|
| LD-01 | 100–1000 one-shot jobs (LoadExample-style) | All complete; count matches |
| LD-02 | High-frequency cron with small pool | Misfire policy behavior stable |
| LD-03 | Many listeners + dense fires | No deadlock; acceptable latency |
| LD-04 | Parallel `scheduleJob` from many threads | Thread-safe API use |
| LD-05 | Mix pause/resume while scheduling | Consistent state |

---

## 17. Negative / rejection tests (fork-specific)

Confirm removed features fail fast if someone copies old config:

| ID | Config / API | Expected |
|---|---|---|
| NEG-01 | `org.quartz.scheduler.jmx.export=true` | Rejected at factory |
| NEG-02 | RMI export/proxy props | Rejected |
| NEG-03 | JTA wrap prop | Rejected |
| NEG-04 | Management REST enabled | Rejected |
| NEG-05 | JDBC job store class name | Fail or unsupported |
| NEG-06 | XML plugin class | ClassNotFound |

---

## 18. Instant / java.time regression suite (fork differentiators)

These are **must-pass** even if classic Date tests pass:

| ID | Focus |
|---|---|
| JT-01 | All public fire-time getters return `Instant` (and consistent epoch millis) |
| JT-02 | Builder `startAt`/`endAt(Instant)` across all schedule builders |
| JT-03 | `Duration` intervals on Simple / Calendar / Daily |
| JT-04 | `Period` on CalendarInterval + DateBuilder |
| JT-05 | `DayOfWeek` on DailyTimeInterval |
| JT-06 | `ZoneId` on DateBuilder vs `TimeZone` on Cron/CalendarInterval — same wall time when equivalent |
| JT-07 | Serialization Instant through Mongo and back |

---

## 19. Suggested test layers and order

1. **Library unit / module tests** (already in this repo): builders, cron, matchers, VirtualThreadPool, MongoJobStore, auto-config — run `mvn test` as gate.
2. **App integration (Env A/B):** §4–§12, §15 without Mongo.
3. **App persistence (Env C):** §6 durable/recovery, §8 calendars, §14.
4. **Cluster (Env D):** §13 before production.
5. **Load (Env A then C):** §16.
6. **Sign-off checklist:** every ID marked Pass/Fail/NA with env letter.

### Minimal “smoke” subset (day-one)

If time-boxed, run at least: J-01, T-01–T-02, ST-01, ST-04, CR-01, CR-04, CI-01, DT-01, JOB-02, JOB-03, JOB-07, MF-01 + one misfire per type, CAL-01, LC-01, LC-07, LC-10, L-10, M-04, TP-03, BOOT-01, BOOT-06, BOOT-09, PS-02, CL-01, CL-02, JT-01.

### Full parity sign-off

All IDs in §§4–18 except optional chaos (MF-05, CL-06) and optional actuator (BOOT-12).

---

## 20. Evidence to capture

- Scheduler logs (fire, misfire, cluster check-in).
- Per-scenario execution counters (use a thread-safe listener or JobDataMap counter).
- For cluster: which `instanceId` executed each fire.
- For Mongo: sample documents for one job/trigger after schedule and after restart (for debugging only).
- Wall-clock notes when asserting timing.

---

## 21. Mapping to “original Quartz except persistence”

| Area | Parity expectation |
|---|---|
| Builders / DSL / four trigger types | Full |
| Misfire policies | Full |
| Calendars | Full |
| Listeners / matchers | Full |
| Pause/resume/standby/shutdown/interrupt | Full |
| Annotations concurrency/persist | Full |
| Clustering *behavior* | Full (Mongo implementation) |
| Spring job beans + DI | Full (this fork’s auto-config, not starter-quartz) |
| JDBC schema / JobStoreTX / JTA / JMX / RMI / XML plugins | N/A — out of scope |
| Public `Date` fire times | Replaced by `Instant` — assert Instant equivalence |

---

## 22. Traceability template

```text
Scenario-ID | Env | Result | Notes / bug link
------------|-----|--------|-----------------
ST-01       | A   | PASS   |
ST-01       | C   | PASS   | survived restart
CL-02       | D   | ...    |
```

Keep this file updated when new APIs land (new schedule builders, plugin helpers, etc.).
