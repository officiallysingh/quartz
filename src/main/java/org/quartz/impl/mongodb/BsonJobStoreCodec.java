package org.quartz.impl.mongodb;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.bson.Document;
import org.bson.types.Binary;
import org.quartz.Calendar;
import org.quartz.CalendarIntervalTrigger;
import org.quartz.CronTrigger;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SimpleTrigger;
import org.quartz.TimeOfDay;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.calendar.AnnualCalendar;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.calendar.CronCalendar;
import org.quartz.impl.calendar.DailyCalendar;
import org.quartz.impl.calendar.HolidayCalendar;
import org.quartz.impl.calendar.MonthlyCalendar;
import org.quartz.impl.calendar.WeeklyCalendar;
import org.quartz.impl.triggers.AbstractTrigger;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

/** BSON documents for jobs, triggers, and calendars — no Java serialization. */
final class BsonJobStoreCodec {

  static final String TYPE_SIMPLE = "simple";
  static final String TYPE_CRON = "cron";
  static final String TYPE_CALENDAR_INTERVAL = "calendarInterval";
  static final String TYPE_DAILY_TIME_INTERVAL = "dailyTimeInterval";

  private BsonJobStoreCodec() {}

  static Document jobBody(JobDetail job) {
    JobKey key = job.getKey();
    Document doc = new Document();
    doc.append("name", key.getName());
    doc.append("group", key.getGroup());
    doc.append("description", job.getDescription());
    doc.append("jobClass", job.getJobClass() == null ? null : job.getJobClass().getName());
    doc.append("durable", job.isDurable());
    doc.append("requestsRecovery", job.requestsRecovery());
    doc.append("jobData", jobData(job.getJobDataMap()));
    return doc;
  }

  static JobDetail toJob(Document doc, ClassLoader classLoader) {
    JobDetailImpl job = new JobDetailImpl();
    job.setName(doc.getString("name"));
    job.setGroup(doc.getString("group"));
    job.setDescription(doc.getString("description"));
    String jobClassName = doc.getString("jobClass");
    if (jobClassName != null && !jobClassName.isBlank()) {
      job.setJobClass(loadJobClass(jobClassName, classLoader));
    }
    job.setDurability(bool(doc, "durable"));
    job.setRequestsRecovery(bool(doc, "requestsRecovery"));
    job.setJobDataMap(toJobData(doc.get("jobData")));
    return job;
  }

  static void putTriggerBody(Document doc, OperableTrigger trigger) {
    doc.append("name", trigger.getKey().getName());
    doc.append("group", trigger.getKey().getGroup());
    doc.append("jobName", trigger.getJobKey().getName());
    doc.append("jobGroup", trigger.getJobKey().getGroup());
    doc.append("description", trigger.getDescription());
    doc.append("calendarName", trigger.getCalendarName());
    doc.append("priority", trigger.getPriority());
    doc.append("misfireInstruction", trigger.getMisfireInstruction());
    doc.append("fireInstanceId", trigger.getFireInstanceId());
    doc.append("startTime", instant(trigger.getStartTime()));
    doc.append("endTime", instant(trigger.getEndTime()));
    doc.append("nextFireTime", instant(trigger.getNextFireTime()));
    doc.append("previousFireTime", instant(trigger.getPreviousFireTime()));
    doc.append("jobData", jobData(trigger.getJobDataMap()));
    doc.append("type", triggerType(trigger));
    if (trigger instanceof SimpleTrigger simple) {
      doc.append("repeatCount", simple.getRepeatCount());
      doc.append("repeatInterval", duration(simple.getRepeatInterval()));
      doc.append("timesTriggered", simple.getTimesTriggered());
    } else if (trigger instanceof CronTrigger cron) {
      doc.append("cronExpression", cron.getCronExpression());
      doc.append("timeZone", timeZoneId(cron.getTimeZone()));
    } else if (trigger instanceof CalendarIntervalTrigger cal) {
      doc.append("repeatInterval", cal.getRepeatInterval());
      doc.append("repeatIntervalUnit", cal.getRepeatIntervalUnit().name());
      doc.append("timesTriggered", cal.getTimesTriggered());
      doc.append("timeZone", timeZoneId(cal.getTimeZone()));
      doc.append(
          "preserveHourOfDayAcrossDaylightSavings", cal.isPreserveHourOfDayAcrossDaylightSavings());
      doc.append("skipDayIfHourDoesNotExist", cal.isSkipDayIfHourDoesNotExist());
    } else if (trigger instanceof DailyTimeIntervalTrigger daily) {
      doc.append("repeatCount", daily.getRepeatCount());
      doc.append("repeatInterval", daily.getRepeatInterval());
      doc.append("repeatIntervalUnit", daily.getRepeatIntervalUnit().name());
      doc.append("timesTriggered", daily.getTimesTriggered());
      if (daily.getDaysOfWeek() != null) {
        doc.append("daysOfWeek", new ArrayList<>(daily.getDaysOfWeek()));
      }
      doc.append("startTimeOfDay", timeOfDay(daily.getStartTimeOfDay()));
      doc.append("endTimeOfDay", timeOfDay(daily.getEndTimeOfDay()));
    } else {
      throw new IllegalStateException("Unsupported trigger type: " + trigger.getClass().getName());
    }
  }

  static OperableTrigger toTrigger(Document doc) {
    String type = doc.getString("type");
    if (type == null) {
      throw new IllegalStateException("Trigger document is missing type");
    }
    AbstractTrigger<?> trigger =
        switch (type) {
          case TYPE_SIMPLE -> {
            SimpleTriggerImpl simple = new SimpleTriggerImpl();
            simple.setRepeatCount(integer(doc, "repeatCount", 0));
            simple.setRepeatInterval(toDuration(doc.get("repeatInterval")));
            simple.setTimesTriggered(integer(doc, "timesTriggered", 0));
            yield simple;
          }
          case TYPE_CRON -> {
            CronTriggerImpl cron = new CronTriggerImpl();
            try {
              cron.setCronExpression(doc.getString("cronExpression"));
            } catch (ParseException e) {
              throw new IllegalStateException("Invalid cron expression in trigger document", e);
            }
            TimeZone tz = timeZone(doc.getString("timeZone"));
            if (tz != null) {
              cron.setTimeZone(tz);
            }
            yield cron;
          }
          case TYPE_CALENDAR_INTERVAL -> {
            CalendarIntervalTriggerImpl cal = new CalendarIntervalTriggerImpl();
            cal.setRepeatInterval(integer(doc, "repeatInterval", 1));
            cal.setRepeatIntervalUnit(
                org.quartz.DateBuilder.IntervalUnit.valueOf(doc.getString("repeatIntervalUnit")));
            cal.setTimesTriggered(integer(doc, "timesTriggered", 0));
            TimeZone tz = timeZone(doc.getString("timeZone"));
            if (tz != null) {
              cal.setTimeZone(tz);
            }
            cal.setPreserveHourOfDayAcrossDaylightSavings(
                bool(doc, "preserveHourOfDayAcrossDaylightSavings"));
            cal.setSkipDayIfHourDoesNotExist(bool(doc, "skipDayIfHourDoesNotExist"));
            yield cal;
          }
          case TYPE_DAILY_TIME_INTERVAL -> {
            DailyTimeIntervalTriggerImpl daily = new DailyTimeIntervalTriggerImpl();
            daily.setRepeatCount(
                integer(doc, "repeatCount", DailyTimeIntervalTrigger.REPEAT_INDEFINITELY));
            daily.setRepeatInterval(integer(doc, "repeatInterval", 1));
            daily.setRepeatIntervalUnit(
                org.quartz.DateBuilder.IntervalUnit.valueOf(doc.getString("repeatIntervalUnit")));
            daily.setTimesTriggered(integer(doc, "timesTriggered", 0));
            List<Integer> days = doc.getList("daysOfWeek", Integer.class);
            if (days != null) {
              daily.setDaysOfWeek(new HashSet<>(days));
            }
            TimeOfDay startTod = toTimeOfDay(doc.get("startTimeOfDay"));
            if (startTod != null) {
              daily.setStartTimeOfDay(startTod);
            }
            TimeOfDay endTod = toTimeOfDay(doc.get("endTimeOfDay"));
            if (endTod != null) {
              daily.setEndTimeOfDay(endTod);
            }
            yield daily;
          }
          default -> throw new IllegalStateException("Unknown trigger type '" + type + "'");
        };
    trigger.setName(doc.getString("name"));
    trigger.setGroup(doc.getString("group"));
    trigger.setJobName(doc.getString("jobName"));
    trigger.setJobGroup(doc.getString("jobGroup"));
    trigger.setDescription(doc.getString("description"));
    trigger.setCalendarName(doc.getString("calendarName"));
    if (doc.get("priority") != null) {
      trigger.setPriority(integer(doc, "priority", trigger.getPriority()));
    }
    if (doc.get("misfireInstruction") != null) {
      trigger.setMisfireInstruction(integer(doc, "misfireInstruction", 0));
    }
    trigger.setFireInstanceId(doc.getString("fireInstanceId"));
    Instant start = readInstant(doc, "startTime");
    if (start != null) {
      trigger.setStartTime(start);
    }
    trigger.setEndTime(readInstant(doc, "endTime"));
    trigger.setNextFireTime(readInstant(doc, "nextFireTime"));
    trigger.setPreviousFireTime(readInstant(doc, "previousFireTime"));
    trigger.setJobDataMap(toJobData(doc.get("jobData")));
    return trigger;
  }

  static Document calendarBody(Calendar calendar) {
    Document doc = new Document();
    doc.append("type", calendarType(calendar));
    doc.append("description", calendar.getDescription());
    if (calendar.getBaseCalendar() != null) {
      doc.append("baseCalendar", calendarBody(calendar.getBaseCalendar()));
    }
    if (calendar instanceof BaseCalendar base && base.getTimeZone() != null) {
      doc.append("timeZone", timeZoneId(base.getTimeZone()));
    }
    if (calendar instanceof HolidayCalendar holiday) {
      List<Date> dates = new ArrayList<>();
      for (Instant i : holiday.getExcludedDates()) {
        dates.add(Date.from(i));
      }
      doc.append("excludedDates", dates);
    } else if (calendar instanceof AnnualCalendar annual) {
      List<Date> days = new ArrayList<>();
      for (java.util.Calendar day : annual.getDaysExcluded()) {
        days.add(day.getTime());
      }
      doc.append("excludedDays", days);
    } else if (calendar instanceof WeeklyCalendar weekly) {
      doc.append("excludeDays", toIntList(weekly.getDaysExcluded()));
    } else if (calendar instanceof MonthlyCalendar monthly) {
      doc.append("excludeDays", toIntList(monthly.getDaysExcluded()));
    } else if (calendar instanceof DailyCalendar daily) {
      doc.append("rangeStartingHourOfDay", daily.getRangeStartingHourOfDay());
      doc.append("rangeStartingMinute", daily.getRangeStartingMinute());
      doc.append("rangeStartingSecond", daily.getRangeStartingSecond());
      doc.append("rangeStartingMillis", daily.getRangeStartingMillis());
      doc.append("rangeEndingHourOfDay", daily.getRangeEndingHourOfDay());
      doc.append("rangeEndingMinute", daily.getRangeEndingMinute());
      doc.append("rangeEndingSecond", daily.getRangeEndingSecond());
      doc.append("rangeEndingMillis", daily.getRangeEndingMillis());
      doc.append("invertTimeRange", daily.getInvertTimeRange());
    } else if (calendar instanceof CronCalendar cron) {
      doc.append("cronExpression", cron.getCronExpression().getCronExpression());
      if (cron.getTimeZone() != null) {
        doc.append("timeZone", timeZoneId(cron.getTimeZone()));
      }
    }
    return doc;
  }

  static Calendar toCalendar(Document doc) {
    if (doc == null) {
      return null;
    }
    String type = doc.getString("type");
    Calendar calendar =
        switch (type == null ? "base" : type) {
          case "holiday" -> {
            HolidayCalendar holiday = new HolidayCalendar();
            List<?> dates = doc.getList("excludedDates", Object.class);
            if (dates != null) {
              for (Object value : dates) {
                Instant i = toInstant(value);
                if (i != null) {
                  holiday.addExcludedDate(i);
                }
              }
            }
            yield holiday;
          }
          case "annual" -> {
            AnnualCalendar annual = new AnnualCalendar();
            ArrayList<java.util.Calendar> days = new ArrayList<>();
            List<?> stored = doc.getList("excludedDays", Object.class);
            if (stored != null) {
              TimeZone tz = timeZone(doc.getString("timeZone"));
              for (Object value : stored) {
                Instant i = toInstant(value);
                if (i != null) {
                  java.util.Calendar day =
                      tz == null
                          ? java.util.Calendar.getInstance()
                          : java.util.Calendar.getInstance(tz);
                  day.setTimeInMillis(i.toEpochMilli());
                  days.add(day);
                }
              }
            }
            annual.setDaysExcluded(days);
            yield annual;
          }
          case "weekly" -> {
            WeeklyCalendar weekly = new WeeklyCalendar();
            boolean[] days = toBooleanArray(doc.get("excludeDays"), 8);
            if (days != null) {
              weekly.setDaysExcluded(days);
            }
            yield weekly;
          }
          case "monthly" -> {
            MonthlyCalendar monthly = new MonthlyCalendar();
            boolean[] days = toBooleanArray(doc.get("excludeDays"), 31);
            if (days != null) {
              monthly.setDaysExcluded(days);
            }
            yield monthly;
          }
          case "daily" -> decodeDailyCalendar(doc);
          case "cron" -> {
            try {
              CronCalendar cron = new CronCalendar(doc.getString("cronExpression"));
              TimeZone tz = timeZone(doc.getString("timeZone"));
              if (tz != null) {
                cron.setTimeZone(tz);
              }
              yield cron;
            } catch (ParseException e) {
              throw new IllegalStateException("Invalid cron calendar expression", e);
            }
          }
          default -> new BaseCalendar();
        };
    calendar.setDescription(doc.getString("description"));
    Document base = doc.get("baseCalendar", Document.class);
    if (base != null) {
      calendar.setBaseCalendar(toCalendar(base));
    }
    TimeZone tz = timeZone(doc.getString("timeZone"));
    if (tz != null && calendar instanceof BaseCalendar baseCal) {
      baseCal.setTimeZone(tz);
    }
    return calendar;
  }

  static Instant readInstant(Document doc, String field) {
    return toInstant(doc.get(field));
  }

  static Date instant(Instant value) {
    return value == null ? null : Date.from(value);
  }

  static Instant toInstant(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant i) {
      return i;
    }
    if (value instanceof Date d) {
      return d.toInstant();
    }
    if (value instanceof Number n) {
      return Instant.ofEpochMilli(n.longValue());
    }
    throw new IllegalStateException("Cannot read Instant from " + value.getClass().getName());
  }

  @SuppressWarnings("unchecked")
  static Class<? extends Job> loadJobClass(String name, ClassLoader classLoader) {
    if (name == null || name.isBlank()) {
      throw new IllegalStateException("Job document is missing jobClass");
    }
    ClassLoader loader = classLoader;
    if (loader == null) {
      loader = Thread.currentThread().getContextClassLoader();
    }
    if (loader == null) {
      loader = BsonJobStoreCodec.class.getClassLoader();
    }
    try {
      return Class.forName(name, false, loader).asSubclass(Job.class);
    } catch (ClassNotFoundException | ClassCastException e) {
      throw new IllegalStateException("Unable to load job class '" + name + "'", e);
    }
  }

  private static String triggerType(OperableTrigger trigger) {
    if (trigger instanceof SimpleTrigger) {
      return TYPE_SIMPLE;
    }
    if (trigger instanceof CronTrigger) {
      return TYPE_CRON;
    }
    if (trigger instanceof CalendarIntervalTrigger) {
      return TYPE_CALENDAR_INTERVAL;
    }
    if (trigger instanceof DailyTimeIntervalTrigger) {
      return TYPE_DAILY_TIME_INTERVAL;
    }
    throw new IllegalStateException("Unsupported trigger type: " + trigger.getClass().getName());
  }

  private static String calendarType(Calendar calendar) {
    if (calendar instanceof HolidayCalendar) {
      return "holiday";
    }
    if (calendar instanceof AnnualCalendar) {
      return "annual";
    }
    if (calendar instanceof WeeklyCalendar) {
      return "weekly";
    }
    if (calendar instanceof MonthlyCalendar) {
      return "monthly";
    }
    if (calendar instanceof DailyCalendar) {
      return "daily";
    }
    if (calendar instanceof CronCalendar) {
      return "cron";
    }
    return "base";
  }

  private static final String QTZ_TYPE = "_qtz";
  private static final String QTZ_VALUE = "v";
  private static final String QTZ_CLASS = "c";

  private static Document jobData(JobDataMap map) {
    if (map == null || map.isEmpty()) {
      return new Document();
    }
    Document doc = new Document();
    for (Map.Entry<String, Object> e : map.entrySet()) {
      doc.put(e.getKey(), bsonValue(e.getValue()));
    }
    return doc;
  }

  private static JobDataMap toJobData(Object value) {
    JobDataMap map = new JobDataMap();
    if (value instanceof Document doc) {
      for (Map.Entry<String, Object> e : doc.entrySet()) {
        map.put(e.getKey(), fromBsonValue(e.getValue()));
      }
    } else if (value instanceof Map<?, ?> raw) {
      for (Map.Entry<?, ?> e : raw.entrySet()) {
        if (e.getKey() != null) {
          map.put(String.valueOf(e.getKey()), fromBsonValue(e.getValue()));
        }
      }
    }
    map.clearDirtyFlag();
    return map;
  }

  private static Object bsonValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant i) {
      return Date.from(i);
    }
    if (value instanceof Date d) {
      return typed("Date", d);
    }
    if (value instanceof Duration d) {
      return typed("Duration", d.toString());
    }
    if (value instanceof UUID u) {
      return typed("UUID", u.toString());
    }
    if (value instanceof Enum<?> e) {
      return new Document(QTZ_TYPE, "Enum")
          .append(QTZ_CLASS, e.getDeclaringClass().getName())
          .append(QTZ_VALUE, e.name());
    }
    if (value instanceof Character || value.getClass() == char.class) {
      return String.valueOf(value);
    }
    if (value instanceof JobDataMap map) {
      return jobData(map);
    }
    if (value instanceof Map<?, ?> map) {
      Document doc = new Document();
      for (Map.Entry<?, ?> e : map.entrySet()) {
        if (e.getKey() != null) {
          doc.put(String.valueOf(e.getKey()), bsonValue(e.getValue()));
        }
      }
      return doc;
    }
    if (value instanceof Collection<?> col) {
      List<Object> list = new ArrayList<>();
      for (Object item : col) {
        list.add(bsonValue(item));
      }
      return list;
    }
    if (value instanceof Object[] arr) {
      List<Object> list = new ArrayList<>();
      for (Object item : arr) {
        list.add(bsonValue(item));
      }
      return list;
    }
    if (value instanceof byte[]
        || value instanceof Boolean
        || value instanceof Number
        || value instanceof String
        || value instanceof Binary) {
      return value;
    }
    throw new IllegalArgumentException(
        "JobDataMap value of type "
            + value.getClass().getName()
            + " cannot be stored in MongoDB BSON. Use String, Number, Boolean, Instant, Duration, UUID, byte[], Enum, or nested Map/List.");
  }

  private static Object fromBsonValue(Object value) {
    if (value instanceof Date d) {
      return d.toInstant();
    }
    if (value instanceof Binary binary) {
      return binary.getData();
    }
    if (value instanceof Document doc) {
      String type = doc.getString(QTZ_TYPE);
      if (type != null) {
        return fromTyped(doc, type);
      }
      Map<String, Object> map = new LinkedHashMap<>();
      for (Map.Entry<String, Object> e : doc.entrySet()) {
        map.put(e.getKey(), fromBsonValue(e.getValue()));
      }
      return map;
    }
    if (value instanceof List<?> list) {
      List<Object> out = new ArrayList<>(list.size());
      for (Object item : list) {
        out.add(fromBsonValue(item));
      }
      return out;
    }
    return value;
  }

  private static Document typed(String type, Object value) {
    return new Document(QTZ_TYPE, type).append(QTZ_VALUE, value);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object fromTyped(Document doc, String type) {
    return switch (type) {
      case "Duration" -> Duration.parse(doc.getString(QTZ_VALUE));
      case "UUID" -> UUID.fromString(doc.getString(QTZ_VALUE));
      case "Date" -> {
        Object stored = doc.get(QTZ_VALUE);
        if (stored instanceof Date d) {
          yield d;
        }
        throw new IllegalStateException("JobDataMap Date value is missing");
      }
      case "Enum" -> {
        String className = doc.getString(QTZ_CLASS);
        String name = doc.getString(QTZ_VALUE);
        try {
          Class<? extends Enum> clazz = Class.forName(className).asSubclass(Enum.class);
          yield Enum.valueOf(clazz, name);
        } catch (ClassNotFoundException e) {
          throw new IllegalStateException("Unable to load JobDataMap enum '" + className + "'", e);
        }
      }
      default -> throw new IllegalStateException("Unknown JobDataMap type tag '" + type + "'");
    };
  }

  private static DailyCalendar decodeDailyCalendar(Document doc) {
    DailyCalendar daily = new DailyCalendar("00:00:00:000", "00:00:00:001");
    int startH = integer(doc, "rangeStartingHourOfDay", 0);
    int startM = integer(doc, "rangeStartingMinute", 0);
    int startS = integer(doc, "rangeStartingSecond", 0);
    int startMs = integer(doc, "rangeStartingMillis", 0);
    int endH = integer(doc, "rangeEndingHourOfDay", 0);
    int endM = integer(doc, "rangeEndingMinute", 0);
    int endS = integer(doc, "rangeEndingSecond", 0);
    int endMs = integer(doc, "rangeEndingMillis", 0);
    long start = millisOfDay(startH, startM, startS, startMs);
    long end = millisOfDay(endH, endM, endS, endMs);
    if (end <= start) {
      end = Math.min(start + 1, millisOfDay(23, 59, 59, 999));
      if (end <= start) {
        start = 0;
        end = 1;
      }
      int[] parts = partsOfDay(end);
      endH = parts[0];
      endM = parts[1];
      endS = parts[2];
      endMs = parts[3];
      parts = partsOfDay(start);
      startH = parts[0];
      startM = parts[1];
      startS = parts[2];
      startMs = parts[3];
    }
    daily.setTimeRange(startH, startM, startS, startMs, endH, endM, endS, endMs);
    daily.setInvertTimeRange(bool(doc, "invertTimeRange"));
    return daily;
  }

  private static long millisOfDay(int hour, int minute, int second, int millis) {
    return ((hour * 60L + minute) * 60 + second) * 1000 + millis;
  }

  private static int[] partsOfDay(long millisOfDay) {
    int ms = (int) (millisOfDay % 1000);
    long rest = millisOfDay / 1000;
    int s = (int) (rest % 60);
    rest /= 60;
    int m = (int) (rest % 60);
    int h = (int) (rest / 60);
    return new int[] {h, m, s, ms};
  }

  private static String duration(Duration duration) {
    return duration == null ? null : duration.toString();
  }

  private static Duration toDuration(Object value) {
    if (value == null) {
      return Duration.ZERO;
    }
    if (value instanceof Duration d) {
      return d;
    }
    if (value instanceof String s) {
      if (s.isBlank()) {
        return Duration.ZERO;
      }
      if (s.charAt(0) == 'P' || s.charAt(0) == 'p') {
        return Duration.parse(s);
      }
      return Duration.ofMillis(Long.parseLong(s));
    }
    if (value instanceof Number n) {
      return Duration.ofMillis(n.longValue());
    }
    throw new IllegalStateException("Cannot read Duration from " + value.getClass().getName());
  }

  private static Document timeOfDay(TimeOfDay time) {
    if (time == null) {
      return null;
    }
    return new Document("hour", time.getHour())
        .append("minute", time.getMinute())
        .append("second", time.getSecond());
  }

  private static TimeOfDay toTimeOfDay(Object value) {
    if (!(value instanceof Document doc)) {
      return null;
    }
    return new TimeOfDay(
        integer(doc, "hour", 0), integer(doc, "minute", 0), integer(doc, "second", 0));
  }

  private static String timeZoneId(TimeZone timeZone) {
    return timeZone == null ? null : timeZone.getID();
  }

  private static TimeZone timeZone(String id) {
    return id == null || id.isBlank() ? null : TimeZone.getTimeZone(id);
  }

  private static List<Integer> toIntList(boolean[] days) {
    if (days == null) {
      return null;
    }
    List<Integer> list = new ArrayList<>(days.length);
    for (boolean day : days) {
      list.add(day ? 1 : 0);
    }
    return list;
  }

  private static boolean[] toBooleanArray(Object value, int size) {
    if (!(value instanceof List<?> list)) {
      return null;
    }
    boolean[] days = new boolean[size];
    for (int i = 0; i < Math.min(size, list.size()); i++) {
      Object item = list.get(i);
      days[i] =
          Boolean.TRUE.equals(item)
              || (item instanceof Number n && n.intValue() != 0)
              || "1".equals(String.valueOf(item));
    }
    return days;
  }

  private static boolean bool(Document doc, String field) {
    Object value = doc.get(field);
    if (value instanceof Boolean b) {
      return b;
    }
    if (value instanceof Number n) {
      return n.intValue() != 0;
    }
    return false;
  }

  private static int integer(Document doc, String field, int fallback) {
    Number n = doc.get(field, Number.class);
    return n == null ? fallback : n.intValue();
  }
}
