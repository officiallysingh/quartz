package org.quartz.impl.calendar;



import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BaseCalendarTest  {

    @Test
    void testClone() {
        BaseCalendar base = new BaseCalendar();
        BaseCalendar clone = (BaseCalendar) base.clone();

        assertEquals(base.getDescription(), clone.getDescription());
        assertEquals(base.getBaseCalendar(), clone.getBaseCalendar());
        assertEquals(base.getTimeZone(), clone.getTimeZone());
    }


}
