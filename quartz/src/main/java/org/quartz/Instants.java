package org.quartz;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

/**
 * Bridges remaining {@link Calendar} usage to {@link Instant}.
 */
public final class Instants {

    private Instants() {
    }

    public static Instant now() {
        return Instant.now();
    }

    public static Instant ofEpochMilli(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli);
    }

    public static long toEpochMilli(Instant instant) {
        return instant == null ? 0L : instant.toEpochMilli();
    }

    public static Instant fromCalendar(Calendar calendar) {
        return calendar == null ? null : calendar.toInstant();
    }

    public static void setCalendar(Calendar calendar, Instant instant) {
        if (calendar != null && instant != null) {
            calendar.setTimeInMillis(instant.toEpochMilli());
        }
    }

    public static Instant plusMillis(Instant instant, long millis) {
        return instant.plusMillis(millis);
    }

    public static Date toDate(Instant instant) {
        return instant == null ? null : Date.from(instant);
    }

    public static Instant fromDate(Date date) {
        return date == null ? null : date.toInstant();
    }
}
