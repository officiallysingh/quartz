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
 *
 */

package org.quartz;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * <code>DateBuilder</code> is used to conveniently create <code>java.time.Instant</code> instances
 * that meet particular criteria.
 *
 * <p>Quartz provides a builder-style API for constructing scheduling-related entities via a
 * Domain-Specific Language (DSL). The DSL can best be utilized through the usage of static imports
 * of the methods on the classes <code>TriggerBuilder</code>, <code>JobBuilder</code>, <code>
 * DateBuilder</code>, <code>JobKey</code>, <code>TriggerKey</code> and the various <code>
 * ScheduleBuilder</code> implementations.
 *
 * <p>Client code can then use the DSL to write code such as this:
 *
 * <pre>
 *         JobDetail job = newJob(MyJob.class)
 *             .withIdentity("myJob")
 *             .build();
 *
 *         Trigger trigger = newTrigger()
 *             .withIdentity(triggerKey("myTrigger", "myTriggerGroup"))
 *             .withSchedule(simpleSchedule()
 *                 .withIntervalInHours(1)
 *                 .repeatForever())
 *             .startAt(futureDate(10, MINUTES))
 *             .build();
 *
 *         scheduler.scheduleJob(job, trigger);
 * </pre>
 *
 * @see TriggerBuilder
 * @see JobBuilder
 */
public class DateBuilder {

  public enum IntervalUnit {
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    YEAR
  }

  public static final int SUNDAY = 1;

  public static final int MONDAY = 2;

  public static final int TUESDAY = 3;

  public static final int WEDNESDAY = 4;

  public static final int THURSDAY = 5;

  public static final int FRIDAY = 6;

  public static final int SATURDAY = 7;

  public static final int JANUARY = 1;

  public static final int FEBRUARY = 2;

  public static final int MARCH = 3;

  public static final int APRIL = 4;

  public static final int MAY = 5;

  public static final int JUNE = 6;

  public static final int JULY = 7;

  public static final int AUGUST = 8;

  public static final int SEPTEMBER = 9;

  public static final int OCTOBER = 10;

  public static final int NOVEMBER = 11;

  public static final int DECEMBER = 12;

  public static final long MILLISECONDS_IN_MINUTE = 60L * 1000L;

  public static final long MILLISECONDS_IN_HOUR = 60L * 60L * 1000L;

  public static final long SECONDS_IN_MOST_DAYS = 24L * 60L * 60L;

  public static final long MILLISECONDS_IN_DAY = SECONDS_IN_MOST_DAYS * 1000L;

  private int month = -1;
  private int day = -1;
  private int year = -1;
  private int hour = -1;
  private int minute = -1;
  private int second = -1;
  private ZoneId zoneId;
  private Clock clock = Clock.systemDefaultZone();

  /**
   * Create a DateBuilder, with initial settings for the current date and time in the system default
   * timezone.
   */
  private DateBuilder() {
    this(ZoneId.systemDefault());
  }

  /**
   * Create a DateBuilder, with initial settings for the current date and time in the given
   * timezone.
   */
  private DateBuilder(ZoneId zoneId) {
    this.zoneId = zoneId != null ? zoneId : ZoneId.systemDefault();
  }

  void setClock(Clock clock) {
    this.clock = clock;
  }

  /**
   * Create a DateBuilder, with initial settings for the current date and time in the system default
   * timezone.
   */
  public static DateBuilder newDate() {
    return new DateBuilder();
  }

  /**
   * Create a DateBuilder, with initial settings for the current date and time in the given
   * timezone.
   */
  public static DateBuilder newDateInTimezone(ZoneId zoneId) {
    return new DateBuilder(zoneId);
  }

  /** Build the Date defined by this builder instance. */
  public Instant build() {
    var useZoneId = (zoneId != null) ? zoneId : ZoneId.systemDefault();

    if (year == -1 || month == -1 || day == -1 || hour == -1 || minute == -1 || second == -1) {
      var zdt = ZonedDateTime.now(clock).withZoneSameInstant(useZoneId);

      year = zdt.getYear();
      month = zdt.getMonthValue();
      day = zdt.getDayOfMonth();
      hour = zdt.getHour();
      minute = zdt.getMinute();
      second = zdt.getSecond();
    }
    var zdt = ZonedDateTime.of(year, month, day, hour, minute, second, 0, useZoneId);

    return (zdt.toInstant());
  }

  /** Set the hour (0-23) for the Date that will be built by this builder. */
  public DateBuilder atHourOfDay(int atHour) {
    validateHour(atHour);

    this.hour = atHour;
    return this;
  }

  /** Set the minute (0-59) for the Date that will be built by this builder. */
  public DateBuilder atMinute(int atMinute) {
    validateMinute(atMinute);

    this.minute = atMinute;
    return this;
  }

  /**
   * Set the second (0-59) for the Date that will be built by this builder, and truncate the
   * milliseconds to 000.
   */
  public DateBuilder atSecond(int atSecond) {
    validateSecond(atSecond);

    this.second = atSecond;
    return this;
  }

  public DateBuilder atHourMinuteAndSecond(int atHour, int atMinute, int atSecond) {
    validateHour(atHour);
    validateMinute(atMinute);
    validateSecond(atSecond);

    this.hour = atHour;
    this.second = atSecond;
    this.minute = atMinute;
    return this;
  }

  /** Set the day of month (1-31) for the Date that will be built by this builder. */
  public DateBuilder onDay(int onDay) {
    validateDayOfMonth(onDay);

    this.day = onDay;
    return this;
  }

  /** Set the month (1-12) for the Date that will be built by this builder. */
  public DateBuilder inMonth(int inMonth) {
    validateMonth(inMonth);

    this.month = inMonth;
    return this;
  }

  public DateBuilder inMonthOnDay(int inMonth, int onDay) {
    validateMonth(inMonth);
    validateDayOfMonth(onDay);

    this.month = inMonth;
    this.day = onDay;
    return this;
  }

  /** Set the year for the Date that will be built by this builder. */
  public DateBuilder inYear(int inYear) {
    validateYear(inYear);

    this.year = inYear;
    return this;
  }

  /**
   * Set the time zone for the Instant that will be built by this builder (if {@code null}, system
   * default is used).
   */
  public DateBuilder inTimeZone(ZoneId zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  public static Instant futureDate(int interval, IntervalUnit unit) {
    return futureDate(interval, unit, Clock.systemDefaultZone());
  }

  static Instant futureDate(int interval, IntervalUnit unit, Clock clock) {
    return (ZonedDateTime.now(clock).plus(interval, translate(unit)).toInstant());
  }

  private static ChronoUnit translate(IntervalUnit unit) {
    switch (unit) {
      case DAY:
        return ChronoUnit.DAYS;
      case HOUR:
        return ChronoUnit.HOURS;
      case MINUTE:
        return ChronoUnit.MINUTES;
      case MONTH:
        return ChronoUnit.MONTHS;
      case SECOND:
        return ChronoUnit.SECONDS;
      case MILLISECOND:
        return ChronoUnit.MILLIS;
      case WEEK:
        return ChronoUnit.WEEKS;
      case YEAR:
        return ChronoUnit.YEARS;
      default:
        throw new IllegalArgumentException("Unknown IntervalUnit");
    }
  }

  /**
   * Get a <code>Date</code> object that represents the given time, on tomorrow's date.
   *
   * @param hour The value (0-23) to give the hours field of the date
   * @param minute The value (0-59) to give the minutes field of the date
   * @param second The value (0-59) to give the seconds field of the date
   * @return the new date
   */
  public static Instant tomorrowAt(int hour, int minute, int second) {
    return tomorrowAt(hour, minute, second, Clock.systemDefaultZone());
  }

  static Instant tomorrowAt(int hour, int minute, int second, Clock clock) {
    return (ZonedDateTime.now(clock)
        .truncatedTo(ChronoUnit.DAYS)
        .plusHours(24)
        .with(LocalTime.of(hour, minute, second, 0))
        .toInstant());
  }

  /**
   * Get a <code>Date</code> object that represents the given time, on today's date (equivalent to
   * {@link #dateOf(int, int, int)}).
   *
   * @param hour The value (0-23) to give the hours field of the date
   * @param minute The value (0-59) to give the minutes field of the date
   * @param second The value (0-59) to give the seconds field of the date
   * @return the new date
   */
  public static Instant todayAt(int hour, int minute, int second) {
    return todayAt(hour, minute, second, Clock.systemDefaultZone());
  }

  static Instant todayAt(int hour, int minute, int second, Clock clock) {
    return dateOf(hour, minute, second, clock);
  }

  /**
   * Get a <code>Date</code> object that represents the given time, on today's date (equivalent to
   * {@link #todayAt(int, int, int)}).
   *
   * @param hour The value (0-23) to give the hours field of the date
   * @param minute The value (0-59) to give the minutes field of the date
   * @param second The value (0-59) to give the seconds field of the date
   * @return the new date
   */
  public static Instant dateOf(int hour, int minute, int second) {
    return dateOf(hour, minute, second, Clock.systemDefaultZone());
  }

  static Instant dateOf(int hour, int minute, int second, Clock clock) {
    return (ZonedDateTime.now(clock).with(LocalTime.of(hour, minute, second, 0)).toInstant());
  }

  /**
   * Get a <code>Date</code> object that represents the given time, on the given date.
   *
   * @param hour The value (0-23) to give the hours field of the date
   * @param minute The value (0-59) to give the minutes field of the date
   * @param second The value (0-59) to give the seconds field of the date
   * @param dayOfMonth The value (1-31) to give the day of month field of the date
   * @param month The value (1-12) to give the month field of the date
   * @return the new date
   */
  public static Instant dateOf(int hour, int minute, int second, int dayOfMonth, int month) {
    return dateOf(hour, minute, second, dayOfMonth, month, Clock.systemDefaultZone());
  }

  static Instant dateOf(int hour, int minute, int second, int dayOfMonth, int month, Clock clock) {
    var zdt = ZonedDateTime.now(clock);
    return (zdt.with(LocalDateTime.of(zdt.getYear(), month, dayOfMonth, hour, minute, second, 0))
        .toInstant());
  }

  /**
   * Get a <code>Date</code> object that represents the given time, on the given date.
   *
   * @param hour The value (0-23) to give the hours field of the date
   * @param minute The value (0-59) to give the minutes field of the date
   * @param second The value (0-59) to give the seconds field of the date
   * @param dayOfMonth The value (1-31) to give the day of month field of the date
   * @param month The value (1-12) to give the month field of the date
   * @param year The value (1970-999999999) to give the year field of the date
   * @return the new date
   */
  public static Instant dateOf(
      int hour, int minute, int second, int dayOfMonth, int month, int year) {
    return dateOf(hour, minute, second, dayOfMonth, month, year, Clock.systemDefaultZone());
  }

  static Instant dateOf(
      int hour, int minute, int second, int dayOfMonth, int month, int year, Clock clock) {
    return (LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, 0)
        .atZone(clock.getZone())
        .toInstant());
  }

  /**
   * Returns a date that is rounded to the next even hour after the current time.
   *
   * <p>For example a current time of 08:13:54 would result in a date with the time of 09:00:00. If
   * the date's time is in the 23rd hour, the date's 'day' will be promoted, and the time will be
   * set to 00:00:00.
   *
   * @return the new rounded date
   */
  public static Instant evenHourDateAfterNow() {
    return evenHourDateAfterNow(Clock.systemDefaultZone());
  }

  static Instant evenHourDateAfterNow(Clock clock) {
    return evenHourDate(null, clock);
  }

  /**
   * Returns a date that is rounded to the next even hour above the given date.
   *
   * <p>For example an input date with a time of 08:13:54 would result in a date with the time of
   * 09:00:00. If the date's time is in the 23rd hour, the date's 'day' will be promoted, and the
   * time will be set to 00:00:00.
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @return the new rounded date
   */
  public static Instant evenHourDate(Instant date) {
    return evenHourDate(date, Clock.systemDefaultZone());
  }

  static Instant evenHourDate(Instant date, Clock clock) {
    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);

    zdt = zdt.plusHours(1);
    return (zdt.truncatedTo(ChronoUnit.HOURS).toInstant());
  }

  /**
   * Returns a date that is rounded to the previous even hour below the given date.
   *
   * <p>For example an input date with a time of 08:13:54 would result in a date with the time of
   * 08:00:00.
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @return the new rounded date
   */
  public static Instant evenHourDateBefore(Instant date) {
    return evenHourDateBefore(date, Clock.systemDefaultZone());
  }

  static Instant evenHourDateBefore(Instant date, Clock clock) {
    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);

    return (zdt.truncatedTo(ChronoUnit.HOURS).toInstant());
  }

  /**
   * Returns a date that is rounded to the next even minute after the current time.
   *
   * <p>For example a current time of 08:13:54 would result in a date with the time of 08:14:00. If
   * the date's time is in the 59th minute, then the hour (and possibly the day) will be promoted.
   *
   * @return the new rounded date
   */
  public static Instant evenMinuteDateAfterNow() {
    return evenMinuteDateAfterNow(Clock.systemDefaultZone());
  }

  static Instant evenMinuteDateAfterNow(Clock clock) {
    return evenMinuteDate(null, clock);
  }

  /**
   * Returns a date that is rounded to the next even minute above the given date.
   *
   * <p>For example an input date with a time of 08:13:54 would result in a date with the time of
   * 08:14:00. If the date's time is in the 59th minute, then the hour (and possibly the day) will
   * be promoted.
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @return the new rounded date
   */
  public static Instant evenMinuteDate(Instant date) {
    return evenMinuteDate(date, Clock.systemDefaultZone());
  }

  public static Instant evenMinuteDate(Instant date, Clock clock) {
    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);

    zdt = zdt.plusMinutes(1);
    return (zdt.truncatedTo(ChronoUnit.MINUTES).toInstant());
  }

  /**
   * Returns a date that is rounded to the previous even minute below the given date.
   *
   * <p>For example an input date with a time of 08:13:54 would result in a date with the time of
   * 08:13:00.
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @return the new rounded date
   */
  public static Instant evenMinuteDateBefore(Instant date) {
    return evenMinuteDateBefore(date, Clock.systemDefaultZone());
  }

  static Instant evenMinuteDateBefore(Instant date, Clock clock) {
    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);

    return (zdt.truncatedTo(ChronoUnit.MINUTES).toInstant());
  }

  /**
   * Returns a date that is rounded to the next even second after the current time.
   *
   * @return the new rounded date
   */
  public static Instant evenSecondDateAfterNow() {
    return evenSecondDateAfterNow(Clock.systemDefaultZone());
  }

  static Instant evenSecondDateAfterNow(Clock clock) {
    return evenSecondDate(null, clock);
  }

  /**
   * Returns a date that is rounded to the next even second above the given date.
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @return the new rounded date
   */
  public static Instant evenSecondDate(Instant date) {
    return evenSecondDate(date, Clock.systemDefaultZone());
  }

  static Instant evenSecondDate(Instant date, Clock clock) {
    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);

    zdt = zdt.plusSeconds(1);
    return (zdt.truncatedTo(ChronoUnit.SECONDS).toInstant());
  }

  /**
   * Returns a date that is rounded to the previous even second below the given date.
   *
   * <p>For example an input date with a time of 08:13:54.341 would result in a date with the time
   * of 08:13:54.000.
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @return the new rounded date
   */
  public static Instant evenSecondDateBefore(Instant date) {
    return evenSecondDateBefore(date, Clock.systemDefaultZone());
  }

  static Instant evenSecondDateBefore(Instant date, Clock clock) {
    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);

    return (zdt.truncatedTo(ChronoUnit.SECONDS).toInstant());
  }

  /**
   * Returns a date that is rounded to the next even multiple of the given minute.
   *
   * <p>For example an input date with a time of 08:13:54, and an input minute-base of 5 would
   * result in a date with the time of 08:15:00. The same input date with an input minute-base of 10
   * would result in a date with the time of 08:20:00. But a date with the time 08:53:31 and an
   * input minute-base of 45 would result in 09:00:00, because the even-hour is the next 'base' for
   * 45-minute intervals.
   *
   * <p>More examples:
   *
   * <table>
   * <caption>Examples of inputs and corresponding outputs.</caption>
   * <tr>
   * <th>Input Time</th>
   * <th>Minute-Base</th>
   * <th>Result Time</th>
   * </tr>
   * <tr>
   * <td>11:16:41</td>
   * <td>20</td>
   * <td>11:20:00</td>
   * </tr>
   * <tr>
   * <td>11:36:41</td>
   * <td>20</td>
   * <td>11:40:00</td>
   * </tr>
   * <tr>
   * <td>11:46:41</td>
   * <td>20</td>
   * <td>12:00:00</td>
   * </tr>
   * <tr>
   * <td>11:26:41</td>
   * <td>30</td>
   * <td>11:30:00</td>
   * </tr>
   * <tr>
   * <td>11:36:41</td>
   * <td>30</td>
   * <td>12:00:00</td>
   * </tr>
   * <tr>
   * <td>11:16:41</td>
   * <td>17</td>
   * <td>11:17:00</td>
   * </tr>
   * <tr>
   * <td>11:17:41</td>
   * <td>17</td>
   * <td>11:34:00</td>
   * </tr>
   * <tr>
   * <td>11:52:41</td>
   * <td>17</td>
   * <td>12:00:00</td>
   * </tr>
   * <tr>
   * <td>11:52:41</td>
   * <td>5</td>
   * <td>11:55:00</td>
   * </tr>
   * <tr>
   * <td>11:57:41</td>
   * <td>5</td>
   * <td>12:00:00</td>
   * </tr>
   * <tr>
   * <td>11:17:41</td>
   * <td>0</td>
   * <td>12:00:00</td>
   * </tr>
   * <tr>
   * <td>11:17:41</td>
   * <td>1</td>
   * <td>11:08:00</td>
   * </tr>
   * </table>
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @param minuteBase the base-minute to set the time on
   * @return the new rounded date
   * @see #nextGivenSecondDate(Instant, int)
   */
  public static Instant nextGivenMinuteDate(Instant date, int minuteBase) {
    return nextGivenMinuteDate(date, minuteBase, Clock.systemDefaultZone());
  }

  static Instant nextGivenMinuteDate(Instant date, int minuteBase, Clock clock) {
    if (minuteBase < 0 || minuteBase > 59) {
      throw new IllegalArgumentException("minuteBase must be >=0 and <= 59");
    }

    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);
    if (minuteBase == 0) {
      zdt = zdt.truncatedTo(ChronoUnit.HOURS).plusHours(1);
      return (zdt.toInstant());
    }

    zdt = zdt.truncatedTo(ChronoUnit.MINUTES);
    int minute = zdt.getMinute();
    int nextminute = minute + minuteBase - (minute % minuteBase);

    if (nextminute >= 60) {
      zdt = zdt.truncatedTo(ChronoUnit.HOURS).plusHours(1);
    } else {
      zdt = zdt.withMinute(nextminute);
    }

    return (zdt.toInstant());
  }

  /**
   * Returns a date that is rounded to the next even multiple of the given second.
   *
   * <p>The rules for calculating the second are the same as those for calculating the minute in the
   * method <code>getNextGivenMinuteDate(..)</code>.
   *
   * @param date the Date to round, if <code>null</code> the current time will be used
   * @param secondBase the base-second to set the time on
   * @return the new rounded date
   * @see #nextGivenMinuteDate(Instant, int)
   */
  public static Instant nextGivenSecondDate(Instant date, int secondBase) {
    return nextGivenSecondDate(date, secondBase, Clock.systemDefaultZone());
  }

  static Instant nextGivenSecondDate(Instant date, int secondBase, Clock clock) {
    if (secondBase < 0 || secondBase > 59) {
      throw new IllegalArgumentException("secondBase must be >=0 and <= 59");
    }

    var zdt =
        (date == null) ? ZonedDateTime.now(clock) : ZonedDateTime.ofInstant(date, ZoneOffset.UTC);
    if (secondBase == 0) {
      zdt = zdt.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
      return (zdt.toInstant());
    }

    zdt = zdt.truncatedTo(ChronoUnit.SECONDS);
    int second = zdt.getSecond();
    int nextSecond = second + secondBase - (second % secondBase);

    if (nextSecond >= 60) {
      zdt = zdt.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
    } else {
      zdt = zdt.withSecond(nextSecond);
    }

    return (zdt.toInstant());
  }

  /**
   * Translate a date and time from a users time zone to the another (probably server) time zone to
   * assist in creating a simple trigger with the right date and time.
   *
   * @param date the date to translate
   * @param src the original time-zone
   * @param dest the destination time-zone
   * @return the translated date
   */
  /** Shift an instant by the difference between two zone offsets at that instant. */
  public static Instant translateTime(Instant date, ZoneId src, ZoneId dest) {
    int offsetMillis =
        dest.getRules().getOffset(date).getTotalSeconds() * 1000
            - src.getRules().getOffset(date).getTotalSeconds() * 1000;
    return date.minusMillis(offsetMillis);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  public static void validateDayOfWeek(int dayOfWeek) {
    if (dayOfWeek < SUNDAY || dayOfWeek > SATURDAY) {
      throw new IllegalArgumentException("Invalid day of week.");
    }
  }

  public static void validateHour(int hour) {
    if (hour < 0 || hour > 23) {
      throw new IllegalArgumentException("Invalid hour (must be >= 0 and <= 23).");
    }
  }

  public static void validateMinute(int minute) {
    if (minute < 0 || minute > 59) {
      throw new IllegalArgumentException("Invalid minute (must be >= 0 and <= 59).");
    }
  }

  public static void validateSecond(int second) {
    if (second < 0 || second > 59) {
      throw new IllegalArgumentException("Invalid second (must be >= 0 and <= 59).");
    }
  }

  public static void validateDayOfMonth(int day) {
    if (day < 1 || day > 31) {
      throw new IllegalArgumentException("Invalid day of month.");
    }
  }

  public static void validateMonth(int month) {
    if (month < 1 || month > 12) {
      throw new IllegalArgumentException("Invalid month (must be >= 1 and <= 12.");
    }
  }

  public static void validateYear(int year) {
    if (year < 1970 || year > Year.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid year (must be >= 0 and <= " + Year.MAX_VALUE);
    }
  }
}
