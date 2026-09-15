package org.quartz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.quartz.CalendarIntervalScheduleBuilder.calendarIntervalSchedule;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.time.Duration;
import java.time.Period;
import org.junit.jupiter.api.Test;
import org.quartz.DateBuilder.IntervalUnit;

class JavaTimeScheduleBuilderTest {

  @Test
  void simpleScheduleWithIntervalDuration() {
    SimpleTrigger trigger =
        newTrigger().withSchedule(simpleSchedule().withInterval(Duration.ofMinutes(5))).build();

    assertEquals(5 * 60 * 1000L, trigger.getRepeatInterval());
  }

  @Test
  void calendarScheduleWithPeriod() {
    CalendarIntervalTrigger months =
        newTrigger()
            .withSchedule(calendarIntervalSchedule().withInterval(Period.ofMonths(2)))
            .build();
    assertEquals(2, months.getRepeatInterval());
    assertEquals(IntervalUnit.MONTH, months.getRepeatIntervalUnit());

    CalendarIntervalTrigger weeks =
        newTrigger()
            .withSchedule(calendarIntervalSchedule().withInterval(Period.ofWeeks(3)))
            .build();
    assertEquals(3, weeks.getRepeatInterval());
    assertEquals(IntervalUnit.WEEK, weeks.getRepeatIntervalUnit());

    CalendarIntervalTrigger days =
        newTrigger()
            .withSchedule(calendarIntervalSchedule().withInterval(Period.ofDays(5)))
            .build();
    assertEquals(5, days.getRepeatInterval());
    assertEquals(IntervalUnit.DAY, days.getRepeatIntervalUnit());
  }

  @Test
  void calendarScheduleWithDuration() {
    CalendarIntervalTrigger hours =
        newTrigger()
            .withSchedule(calendarIntervalSchedule().withInterval(Duration.ofHours(2)))
            .build();
    assertEquals(2, hours.getRepeatInterval());
    assertEquals(IntervalUnit.HOUR, hours.getRepeatIntervalUnit());

    CalendarIntervalTrigger seconds =
        newTrigger()
            .withSchedule(calendarIntervalSchedule().withInterval(Duration.ofSeconds(45)))
            .build();
    assertEquals(45, seconds.getRepeatInterval());
    assertEquals(IntervalUnit.SECOND, seconds.getRepeatIntervalUnit());
  }

  @Test
  void calendarScheduleRejectsMixedPeriod() {
    assertThrows(
        IllegalArgumentException.class,
        () -> calendarIntervalSchedule().withInterval(Period.of(1, 2, 0)));
  }
}
