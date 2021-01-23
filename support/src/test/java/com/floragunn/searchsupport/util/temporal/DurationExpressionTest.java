package com.floragunn.searchsupport.util.temporal;

import static org.junit.Assert.assertEquals;

import java.time.Duration;

import org.junit.Test;

import com.floragunn.searchsupport.util.temporal.DurationExpression;

public class DurationExpressionTest {

    @Test
    public void constantTest() throws Exception {
        DurationExpression durationExpression = DurationExpression.parse("3h 2m");
        Duration duration = Duration.parse("PT3H2M");

        assertEquals(duration, durationExpression.getActualDuration(0));
        assertEquals(duration, durationExpression.getActualDuration(1));
        assertEquals(duration, durationExpression.getActualDuration(2));
        assertEquals(duration, durationExpression.getActualDuration(3));
    }

    @Test
    public void exponentialTest() throws Exception {
        DurationExpression durationExpression = DurationExpression.parse("1h**2");

        assertEquals(Duration.parse("PT1H"), durationExpression.getActualDuration(0));
        assertEquals(Duration.parse("PT2H"), durationExpression.getActualDuration(1));
        assertEquals(Duration.parse("PT4H"), durationExpression.getActualDuration(2));
        assertEquals(Duration.parse("PT8H"), durationExpression.getActualDuration(3));
        assertEquals(Duration.parse("PT16H"), durationExpression.getActualDuration(4));
        assertEquals(Duration.parse("PT24H"), durationExpression.getActualDuration(5));
        assertEquals(Duration.parse("PT24H"), durationExpression.getActualDuration(6));
    }


    @Test
    public void exponentialCappedTest() throws Exception {
        DurationExpression durationExpression = DurationExpression.parse("1m**2|1h");

        assertEquals(Duration.parse("PT1M"), durationExpression.getActualDuration(0));
        assertEquals(Duration.parse("PT2M"), durationExpression.getActualDuration(1));
        assertEquals(Duration.parse("PT4M"), durationExpression.getActualDuration(2));
        assertEquals(Duration.parse("PT8M"), durationExpression.getActualDuration(3));
        assertEquals(Duration.parse("PT16M"), durationExpression.getActualDuration(4));
        assertEquals(Duration.parse("PT32M"), durationExpression.getActualDuration(5));
        assertEquals(Duration.parse("PT60M"), durationExpression.getActualDuration(6));
        assertEquals(Duration.parse("PT60M"), durationExpression.getActualDuration(7));
    }
}
