package org.quartz;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.quartz.DateBuilder.JULY;
import static org.quartz.DateBuilder.JUNE;
import static org.quartz.DateBuilder.dateOf;
import static org.quartz.DateBuilder.futureDate;
import static org.quartz.DateBuilder.newDate;
import static org.quartz.DateBuilder.newDateInTimezone;
import static org.quartz.DateBuilder.todayAt;
import static org.quartz.DateBuilder.translateTime;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.quartz.DateBuilder.IntervalUnit;

class DateBuilderTest {

    @Test
    void testJustInstantiatedBuilderShouldReturnSameInstantTruncatedToSeconds() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T10:15:30.999Z"), ZoneOffset.UTC);
        var builder = DateBuilder.newDate();
        builder.setClock(clock);

        assertEquals(Instant.parse("2007-12-03T10:15:30.000Z"), builder.build());
    }

    @Test
    void testBuilder() {
        var expected = ZonedDateTime.of(2013, 07, 1, 10, 30, 0, 0, ZoneId.systemDefault()).toInstant();

        var bd1 = newDate().inYear(2013).inMonth(JULY).onDay(1).atHourOfDay(10).atMinute(30).atSecond(0).build();
        var bd2 = newDate().inYear(2013).inMonthOnDay(JULY, 1).atHourMinuteAndSecond(10, 30, 0).build();

        assertAll(
                "dateBuilderDefaultTimeZone",
                () -> assertEquals(expected, bd1),
                () -> assertEquals(expected, bd2));
    }

    @Test
    void testBuilderWithTimeZone() {
        ZoneId zone = ZoneOffset.ofHours(-4);
        var expected = Instant.parse("2013-06-01T14:33:12.000Z");

        var bd1 = newDate().inYear(2013)
                .inMonth(JUNE)
                .onDay(1)
                .atHourOfDay(10)
                .atMinute(33)
                .atSecond(12)
                .inTimeZone(zone)
                .build();

        var bd2 = newDateInTimezone(zone).inYear(2013)
                .inMonth(JUNE)
                .onDay(1)
                .atHourOfDay(10)
                .atMinute(33)
                .atSecond(12)
                .build();

        assertAll(
                "dateBuilderWithCustomTimeZone",
                () -> assertEquals(expected, bd1),
                () -> assertEquals(expected, bd2));
    }

    /*
     * futureDate tests
     */

    private static Stream<Arguments> futureDateTestData() {
        return Stream.of(
                Arguments.of(1, IntervalUnit.MILLISECOND, "2007-12-03T10:15:30.001Z"),
                Arguments.of(1, IntervalUnit.SECOND, "2007-12-03T10:15:31.000Z"),
                Arguments.of(1, IntervalUnit.MINUTE, "2007-12-03T10:16:30.000Z"),
                Arguments.of(1, IntervalUnit.HOUR, "2007-12-03T11:15:30.000Z"),
                Arguments.of(1, IntervalUnit.DAY, "2007-12-04T10:15:30.000Z"),
                Arguments.of(1, IntervalUnit.WEEK, "2007-12-10T10:15:30.000Z"),
                Arguments.of(1, IntervalUnit.MONTH, "2008-01-03T10:15:30.000Z"),
                Arguments.of(1, IntervalUnit.YEAR, "2008-12-03T10:15:30.000Z"));
    }

    @ParameterizedTest
    @MethodSource("futureDateTestData")
    void testFutureDate(int interval, IntervalUnit unit, String expected) {
        var clock = Clock.fixed(Instant.parse("2007-12-03T10:15:30.000Z"), ZoneId.systemDefault());

        assertEquals(Instant.parse(expected), futureDate(interval, unit, clock));
    }

    @ParameterizedTest
    @MethodSource("futureDateTestData")
    void testFutureDateWithNonDefaultZone(int interval, IntervalUnit unit, String expected) {
        var clock = Clock.fixed(Instant.parse("2007-12-03T10:15:30.000Z"), ZoneOffset.ofHoursMinutes(3, 15));

        assertEquals(Instant.parse(expected), futureDate(interval, unit, clock));
    }

    /*
     * tomorrowAt tests
     */

    private static Stream<Arguments> tomorrowAtTestData() {
        return Stream.of(
                Arguments.of("2024-10-25T23:59:30.999Z", ZoneOffset.UTC, "2024-10-26T02:30:20.000Z"),
                Arguments.of(
                        "2024-03-30T23:59:59.999+01:00",
                        ZoneId.of("Europe/Vienna"),
                        "2024-03-31T03:30:20.000+02:00"), // DST change skipped one hour
                Arguments.of(
                        "2024-10-26T23:59:59.999+01:00",
                        ZoneId.of("Europe/Vienna"),
                        "2024-10-27T02:30:20.000+01:00")); // DST change added one hour
    }

    @ParameterizedTest
    @MethodSource("tomorrowAtTestData")
    void testTomorrowAt(String input, ZoneId zoneId, String expected) {
        var instant = ZonedDateTime.parse(input).toInstant();
        var clock = Clock.fixed(instant, zoneId);
        var tomorrow = DateBuilder.tomorrowAt(2, 30, 20, clock);

        assertEquals(ZonedDateTime.parse(expected).toInstant(), tomorrow);
    }

    /*
     * todayAt tests
     */

    @ParameterizedTest
    @MethodSource("testDateOf3PWithInvalidParametersTestData")
    void testTodayAtInvalidValues(int hour, int minute, int second) {
        assertThrows(DateTimeException.class, () -> todayAt(hour, minute, second));
    }

    @Test
    void testTodayAt() {
        var clock1 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        var date1 = DateBuilder.todayAt(1, 1, 1, clock1);
        var clock2 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneId.of("Europe/Vienna"));
        var date2 = DateBuilder.todayAt(1, 1, 1, clock2);
        var clock3 = Clock.fixed(Instant.parse("2024-03-31T08:13:54.341Z"), ZoneId.of("Europe/Vienna"));
        var dstSkippedHour = DateBuilder.todayAt(2, 30, 17, clock3); // DST change => skipped hour

        assertAll(
                "dateOf3P",
                () -> assertNotEquals(date1, date2),
                () -> assertEquals(Instant.parse("2007-12-03T01:01:01.000Z"), date1),
                () -> assertEquals(Instant.parse("2007-12-03T00:01:01.000Z"), date2),
                () -> assertEquals(
                        ZonedDateTime.parse("2024-03-31T03:30:17.000+02:00")
                                .toInstant(),
                        dstSkippedHour));
    }

    /*
     * dateOf tests
     */

    private static Stream<Arguments> testDateOf3PWithInvalidParametersTestData() {
        return Stream.of(
                Arguments.of(-1, 0, 0),
                Arguments.of(24, 0, 0),
                Arguments.of(0, -1, 0),
                Arguments.of(0, 60, 0),
                Arguments.of(0, 0, -1),
                Arguments.of(0, 0, 60));
    }

    @ParameterizedTest
    @MethodSource("testDateOf3PWithInvalidParametersTestData")
    void testDateOf3PWithInvalidParameters(int hour, int minute, int second) {
        assertThrows(DateTimeException.class, () -> dateOf(hour, minute, second));
    }

    @Test
    void testDateOf3P() {
        var clock1 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        var clock2 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneId.of("Europe/Vienna"));
        var date1 = DateBuilder.dateOf(1, 1, 1, clock1);
        var date2 = DateBuilder.dateOf(1, 1, 1, clock2);
        assertAll(
                "dateOf3P",
                () -> assertNotEquals(date1, date2),
                () -> assertEquals(Instant.parse("2007-12-03T01:01:01.000Z"), date1),
                () -> assertEquals(Instant.parse("2007-12-03T00:01:01.000Z"), date2));
    }

    private static Stream<Arguments> testDateOf5PWithInvalidParametersTestData() {
        return Stream.of(
                Arguments.of(-1, 0, 0, 0, 0),
                Arguments.of(24, 0, 0, 0, 0),
                Arguments.of(0, -1, 0, 0, 0),
                Arguments.of(0, 60, 0, 0, 0),
                Arguments.of(0, 0, -1, 0, 0),
                Arguments.of(0, 0, 60, 0, 0),
                Arguments.of(0, 0, 0, 0, 0),
                Arguments.of(0, 0, 0, 32, 0),
                Arguments.of(0, 0, 0, 0, 0),
                Arguments.of(0, 0, 0, 0, 13),
                Arguments.of(0, 0, 0, 0, 0),
                Arguments.of(0, 0, 0, 0, 0));
    }

    @ParameterizedTest
    @MethodSource("testDateOf5PWithInvalidParametersTestData")
    void testDateOf5PWithInvalidParameters(int hour, int minute, int second,
            int dayOfMonth, int month) {
        assertThrows(DateTimeException.class, () -> dateOf(hour, minute, second, dayOfMonth, month));
    }

    @Test
    void testDateOf5P() {
        var clock1 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        var clock2 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneId.of("Europe/Vienna"));
        var date1 = DateBuilder.dateOf(1, 1, 1, 1, 1, clock1);
        var date2 = DateBuilder.dateOf(1, 1, 1, 1, 1, clock2);
        assertAll(
                "dateOf6P",
                () -> assertNotEquals(date1, date2),
                () -> assertEquals(Instant.parse("2007-01-01T01:01:01.000Z"), date1),
                () -> assertEquals(Instant.parse("2007-01-01T00:01:01.000Z"), date2));
    }

    private static Stream<Arguments> testDateOf6PWithInvalidParametersTestData() {
        return Stream.of(
                Arguments.of(-1, 0, 0, 0, 0, 0),
                Arguments.of(24, 0, 0, 0, 0, 0),
                Arguments.of(0, -1, 0, 0, 0, 0),
                Arguments.of(0, 60, 0, 0, 0, 0),
                Arguments.of(0, 0, -1, 0, 0, 0),
                Arguments.of(0, 0, 60, 0, 0, 0),
                Arguments.of(0, 0, 0, 0, 0, 0),
                Arguments.of(0, 0, 0, 32, 0, 0),
                Arguments.of(0, 0, 0, 0, 0, 0),
                Arguments.of(0, 0, 0, 0, 13, 0),
                Arguments.of(0, 0, 0, 0, 0, -1_000_000_000),
                Arguments.of(0, 0, 0, 0, 0, 1_000_000_000));
    }

    @ParameterizedTest
    @MethodSource("testDateOf6PWithInvalidParametersTestData")
    void testDateOf6PWithInvalidParameters(int hour, int minute, int second,
            int dayOfMonth, int month, int year) {
        assertThrows(DateTimeException.class, () -> dateOf(hour, minute, second, dayOfMonth, month, year));
    }

    @Test
    void testDateOf6P() {
        var clock1 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        var clock2 = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneId.of("Europe/Vienna"));
        var date1 = DateBuilder.dateOf(1, 1, 1, 1, 1, 2024, clock1);
        var date2 = DateBuilder.dateOf(1, 1, 1, 1, 1, 2024, clock2);
        assertAll(
                "dateOf6P",
                () -> assertNotEquals(date1, date2),
                () -> assertEquals(Instant.parse("2024-01-01T01:01:01.000Z"), date1),
                () -> assertEquals(Instant.parse("2024-01-01T00:01:01.000Z"), date2));
    }

    /*
     * evenHourDateAfterNow tests
     */

    @Test
    void testEvenHourDateAfterNow() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T09:00:00.000Z"),
                DateBuilder.evenHourDateAfterNow(clock));
    }

    /*
     * evenHourDate tests
     */

    @Test
    void testEvenHourDate() {
        assertEquals(
                Instant.parse("2007-12-03T09:00:00.000Z"),
                DateBuilder.evenHourDate(Instant.parse("2007-12-03T08:13:54.341Z")));
    }

    @Test
    void testEvenHourDateWithoutProvidedDateCurrentDateRoundedToSeconds() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T09:00:00.000Z"),
                DateBuilder.evenHourDate(null, clock));
    }

    /*
     * evenHourDateBefore tests
     */

    @Test
    void testEvenHourDateBefore() {
        assertEquals(
                Instant.parse("2007-12-03T08:00:00.000Z"),
                DateBuilder.evenHourDateBefore(Instant.parse("2007-12-03T08:13:54.341Z")));
    }

    @Test
    void testEvenHourDateBeforeWithoutProvidedDateCurrentDateRoundedToSeconds() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T08:00:00.000Z"),
                DateBuilder.evenHourDateBefore(null, clock));
    }

    /*
     * evenMinuteDateAfterNow tests
     */

    @Test
    void testEvenMinuteDateAfterNow() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T08:14:00.000Z"),
                DateBuilder.evenMinuteDateAfterNow(clock));
    }

    /*
     * evenMinuteDate tests
     */

    @Test
    void testEvenMinuteDate() {
        assertEquals(
                Instant.parse("2007-12-03T08:14:00.000Z"),
                DateBuilder.evenMinuteDate(Instant.parse("2007-12-03T08:13:54.341Z")));
    }

    @Test
    void testEvenMinuteDateWithoutProvidedDateCurrentDateRoundedToSeconds() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T08:14:00.000Z"),
                DateBuilder.evenMinuteDate(null, clock));
    }

    /*
     * evenMinuteDateBefore tests
     */

    @Test
    void testEvenMinuteDateBefore() {
        assertEquals(
                Instant.parse("2007-12-03T08:13:00.000Z"),
                DateBuilder.evenMinuteDateBefore(Instant.parse("2007-12-03T08:13:54.341Z")));
    }

    @Test
    void testEvenMinuteDateBeforeWithoutProvidedDateCurrentDateRoundedToSeconds() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T08:13:00.000Z"),
                DateBuilder.evenMinuteDateBefore(null, clock));
    }

    /*
     * evenSecondDateAfterNow tests
     */

    @Test
    void testEvenSecondDateAfterNow() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T08:13:55.000Z"),
                DateBuilder.evenSecondDateAfterNow(clock));
    }

    @Test
    void testEvenSecondDateAfterNowDSTForward() {
        var clock = Clock.fixed(
                ZonedDateTime.parse("2024-03-31T01:59:59.999+01:00")
                        .toInstant(),
                ZoneId.of("Europe/Vienna"));
        var before = clock.instant();
        var result = DateBuilder.evenSecondDateAfterNow(clock);

        assertAll("DSTChange+1H",
                () -> assertEquals(ZonedDateTime.parse("2024-03-31T01:59:59.999+01:00").toInstant(), before),
                () -> assertEquals(ZonedDateTime.parse("2024-03-31T01:00:00.000Z").toInstant(), result),
                () -> assertEquals(ZonedDateTime.parse("2024-03-31T03:00:00.000+02:00").toInstant(), result));
    }

    @Test
    void testEvenSecondDateAfterNowDSTBackward() {
        var clock = Clock.fixed(
                ZonedDateTime.parse("2024-10-27T02:59:59.999+02:00")
                        .toInstant(),
                ZoneId.of("Europe/Vienna"));
        var before = clock.instant();
        var result = DateBuilder.evenSecondDateAfterNow(clock);

        assertAll("DSTChange-1H",
                () -> assertEquals(ZonedDateTime.parse("2024-10-27T02:59:59.999+02:00").toInstant(), before),
                () -> assertEquals(ZonedDateTime.parse("2024-10-27T01:00:00.000Z").toInstant(), result),
                () -> assertEquals(ZonedDateTime.parse("2024-10-27T02:00:00.000+01:00").toInstant(), result));
    }

    /*
     * evenSecondDate tests
     */

    @Test
    void testEvenSecondDate() {
        assertEquals(
                Instant.parse("2007-12-03T08:13:55.000Z"),
                DateBuilder.evenSecondDate(Instant.parse("2007-12-03T08:13:54.341Z")));
    }

    @Test
    void testEvenSecondDateWithoutProvidedDateCurrentDateRoundedToSeconds() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T08:13:55.000Z"),
                DateBuilder.evenSecondDate(null, clock));
    }

    /*
     * evenSecondDateBefore tests
     */

    @Test
    void testEvenSecondDateBefore() {
        assertEquals(
                Instant.parse("2007-12-03T08:13:54.000Z"),
                DateBuilder.evenSecondDateBefore(Instant.parse("2007-12-03T08:13:54.341Z")));
    }

    @Test
    void testEvenSecondDateBeforeWithoutProvidedDateCurrentDateRoundedToSeconds() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T08:13:54.341Z"), ZoneOffset.UTC);
        assertEquals(
                Instant.parse("2007-12-03T08:13:54.000Z"),
                DateBuilder.evenSecondDateBefore(null, clock));
    }

    /*
     * nextGivenMinuteDate tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 60, Integer.MAX_VALUE })
    void testNextGivenMinuteDateWithInvalidSecondBaseShouldThrow(int minuteBase) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.nextGivenMinuteDate(null, minuteBase));
    }

    private static Stream<Arguments> nextGivenMinuteDateTestData() {
        return Stream.of(
                Arguments.of("2007-12-03T11:16:41.123Z", 20, "2007-12-03T11:20:00.000Z"),
                Arguments.of("2007-12-03T11:36:41.123Z", 20, "2007-12-03T11:40:00.000Z"),
                Arguments.of("2007-12-03T11:46:41.123Z", 20, "2007-12-03T12:00:00.000Z"),

                Arguments.of("2007-12-03T11:26:41.123Z", 30, "2007-12-03T11:30:00.000Z"),
                Arguments.of("2007-12-03T11:36:41.123Z", 30, "2007-12-03T12:00:00.000Z"),

                Arguments.of("2007-12-03T11:16:41.123Z", 17, "2007-12-03T11:17:00.000Z"),
                Arguments.of("2007-12-03T11:17:41.123Z", 17, "2007-12-03T11:34:00.000Z"),
                Arguments.of("2007-12-03T11:52:41.123Z", 17, "2007-12-03T12:00:00.000Z"),

                Arguments.of("2007-12-03T11:52:41.123Z", 5, "2007-12-03T11:55:00.000Z"),
                Arguments.of("2007-12-03T11:57:41.123Z", 5, "2007-12-03T12:00:00.000Z"),

                Arguments.of("2007-12-03T11:17:41.123Z", 0, "2007-12-03T12:00:00.000Z"),
                Arguments.of("2007-12-03T11:17:41.123Z", 1, "2007-12-03T11:18:00.000Z"));
    }

    @ParameterizedTest
    @MethodSource("nextGivenMinuteDateTestData")
    void testNextGivenMinuteDate(String input, int minuteBase, String expected) {
        var instant = Instant.parse(input);
        var result = DateBuilder.nextGivenMinuteDate(instant != null ? instant : null, minuteBase);
        assertEquals(Instant.parse(expected), result);
    }

    @Test
    void testNextGivenMinuteDateWithNullDateShouldReturnCurrentDateRoundedToBaseMinute() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T10:15:30.123Z"), ZoneOffset.UTC);
        var result = DateBuilder.nextGivenMinuteDate(null, 0, clock);
        assertEquals(Instant.parse("2007-12-03T11:00:00.000Z"), result);
    }

    /*
     * nextGivenSecondDate tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 60, Integer.MAX_VALUE })
    void testNextGivenSecondDateWithInvalidSecondBaseShouldThrow(int secondBase) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.nextGivenSecondDate(null, secondBase));
    }

    private static Stream<Arguments> nextGivenSecondDateTestData() {
        return Stream.of(
                Arguments.of("2007-12-03T10:15:30.123Z", 0, "2007-12-03T10:16:00.000Z"),
                Arguments.of("2007-12-03T10:15:30.123Z", 1, "2007-12-03T10:15:31.000Z"),
                Arguments.of("2007-12-03T10:15:54.256Z", 13, "2007-12-03T10:16:00.000Z"),
                Arguments.of("2007-12-03T10:15:30.256Z", 13, "2007-12-03T10:15:39.000Z"),
                Arguments.of("2007-12-03T10:15:30.000Z", 59, "2007-12-03T10:15:59.000Z"));
    }

    @ParameterizedTest
    @MethodSource("nextGivenSecondDateTestData")
    void testNextGivenSecondDate(String input, int secondBase, String expected) {
        var instant = Instant.parse(input);
        var result = DateBuilder.nextGivenSecondDate(instant != null ? instant : null, secondBase);
        assertEquals(Instant.parse(expected), result);
    }

    @Test
    void testNextGivenSecondDateWithNullDateShouldReturnCurrentDateRoundedToBaseSecond() {
        var clock = Clock.fixed(Instant.parse("2007-12-03T10:15:30.123Z"), ZoneOffset.UTC);
        var result = DateBuilder.nextGivenSecondDate(null, 0, clock);
        assertEquals(Instant.parse("2007-12-03T10:16:00.000Z"), result);
    }

    /*
     * translateTime tests
     */

    @Test
    void testTranslate() {
        ZoneId tz1 = ZoneOffset.ofHours(-2);
        ZoneId tz2 = ZoneOffset.ofHours(-4);

        Instant wallInTz1 = ZonedDateTime.of(2013, 6, 1, 10, 33, 12, 0, tz1).toInstant();
        Instant shifted = translateTime(wallInTz1, tz1, tz2);
        assertEquals(12, shifted.atZone(tz1).getHour());

        Instant wallInTz2 = ZonedDateTime.of(2013, 6, 1, 10, 33, 12, 0, tz2).toInstant();
        Instant shiftedBack = translateTime(wallInTz2, tz2, tz1);
        assertEquals(8, shiftedBack.atZone(tz2).getHour());
    }

    private static Stream<Arguments> translateTimeTestData() {
        return Stream.of(
                Arguments.of(
                        "2013-06-01T10:33:12-02:00",
                        ZoneOffset.ofHours(-2),
                        ZoneOffset.ofHours(-4),
                        "2013-06-01T12:33:12-02:00"),
                Arguments.of(
                        "2013-06-01T10:33:12-04:00",
                        ZoneOffset.ofHours(-4),
                        ZoneOffset.ofHours(-2),
                        "2013-06-01T08:33:12-04:00"));
    }

    @ParameterizedTest
    @MethodSource("translateTimeTestData")
    void testTranslateTime(String input, ZoneId tzFrom, ZoneId tzTo, String expected) {
        var zdt = ZonedDateTime.parse(input).withZoneSameInstant(tzFrom);
        var result1 = translateTime(zdt.toInstant(), tzFrom, tzTo);
        assertEquals(ZonedDateTime.parse(expected).toInstant(), result1);
    }

    /*
     * validateDayOfWeek tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 0, 8, Integer.MAX_VALUE })
    void testValidateDayOfWeekWithInvalidValueShouldThrow(int dayOfWeek) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.validateDayOfWeek(dayOfWeek));
    }

    private static Stream<Arguments> validateDayOfWeekTestData() {
        return Stream.iterate(1, i -> i + 1)
                .limit(7)
                .map(i -> Arguments.of(i));
    }

    @ParameterizedTest
    @MethodSource("validateDayOfWeekTestData")
    void testValidateDayOfWeek(int second) {
        assertDoesNotThrow(() -> DateBuilder.validateDayOfWeek(second));
    }

    /*
     * validateHour tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 24, Integer.MAX_VALUE })
    void testValidateHourWithInvalidValueShouldThrow(int minute) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.validateHour(minute));
    }

    private static Stream<Arguments> validateHourTestData() {
        return Stream.iterate(0, i -> i + 1)
                .limit(24)
                .map(i -> Arguments.of(i));
    }

    @ParameterizedTest
    @MethodSource("validateHourTestData")
    void testValidateHour(int hour) {
        assertDoesNotThrow(() -> DateBuilder.validateHour(hour));
    }

    /*
     * validateMinutes tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 60, Integer.MAX_VALUE })
    void testValidateMinuteWithInvalidValueShouldThrow(int minute) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.validateMinute(minute));
    }

    private static Stream<Arguments> validateMinuteTestData() {
        return Stream.iterate(0, i -> i + 1)
                .limit(60)
                .map(i -> Arguments.of(i));
    }

    @ParameterizedTest
    @MethodSource("validateMinuteTestData")
    void testValidateMinute(int minute) {
        assertDoesNotThrow(() -> DateBuilder.validateMinute(minute));
    }

    /*
     * validateSeconds tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 60, Integer.MAX_VALUE })
    void testValidateSecondWithInvalidValueShouldThrow(int second) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.validateSecond(second));
    }

    private static Stream<Arguments> validateSecondsTestData() {
        return Stream.iterate(0, i -> i + 1)
                .limit(60)
                .map(i -> Arguments.of(i));
    }

    @ParameterizedTest
    @MethodSource("validateSecondsTestData")
    void testValidateSeconds(int second) {
        assertDoesNotThrow(() -> DateBuilder.validateSecond(second));
    }

    /*
     * validateDayOfMonth tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 0, 32, Integer.MAX_VALUE })
    void testValidateDayOfMonthWithInvalidValueShouldThrow(int dayOfMonth) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.validateDayOfMonth(dayOfMonth));
    }

    private static Stream<Arguments> validateDayOfMonthTestData() {
        return Stream.iterate(1, i -> i + 1)
                .limit(31)
                .map(i -> Arguments.of(i));
    }

    @ParameterizedTest
    @MethodSource("validateDayOfMonthTestData")
    void testValidateDayOfMonth(int dayOfMonth) {
        assertDoesNotThrow(() -> DateBuilder.validateDayOfMonth(dayOfMonth));
    }

    /*
     * validateMonth tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 0, 13, Integer.MAX_VALUE })
    void testValidateMonthWithInvalidValueShouldThrow(int month) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.validateMonth(month));
    }

    private static Stream<Arguments> validateMonthTestData() {
        return Stream.iterate(1, i -> i + 1)
                .limit(12)
                .map(i -> Arguments.of(i));
    }

    @ParameterizedTest
    @MethodSource("validateMonthTestData")
    void testValidateMonth(int month) {
        assertDoesNotThrow(() -> DateBuilder.validateMonth(month));
    }

    /*
     * validateYear tests
     */

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, 1969, 1_000_000_000, Integer.MAX_VALUE })
    void testValidateYearWithInvalidValueShouldThrow(int year) {
        assertThrows(IllegalArgumentException.class, () -> DateBuilder.validateYear(year));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1970, 2024, 999_999_999 })
    void testValidateYear(int year) {
        assertDoesNotThrow(() -> DateBuilder.validateYear(year));
    }

}
