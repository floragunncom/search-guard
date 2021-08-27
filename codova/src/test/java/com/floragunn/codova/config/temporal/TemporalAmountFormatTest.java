/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.codova.config.temporal;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import org.junit.Test;

public class TemporalAmountFormatTest {
    @Test
    public void parseTestDuration1() throws Exception {
        TemporalAmount duration1 = TemporalAmountFormat.INSTANCE.parse("3d 2h 1m");
        Duration duration2 = Duration.parse("P3DT2H1M");

        assertEquals(duration2, duration1);
    }

    @Test
    public void parseTestDuration2() throws Exception {
        TemporalAmount duration1 = TemporalAmountFormat.INSTANCE.parse("20m");
        Duration duration2 = Duration.parse("PT20M");

        assertEquals(duration2, duration1);
    }

    @Test
    public void parseTestDuration3() throws Exception {
        TemporalAmount duration1 = TemporalAmountFormat.INSTANCE.parse("3d2h1m");
        Duration duration2 = Duration.parse("P3DT2H1M");

        assertEquals(duration2, duration1);
    }

    @Test
    public void parseTestPeriod1() throws Exception {
        TemporalAmount period1 = TemporalAmountFormat.INSTANCE.parse("1y");
        Period period2 = Period.parse("P1Y");

        assertEquals(period2, period1);
    }

    @Test
    public void parseTestPeriod2() throws Exception {
        TemporalAmount period1 = TemporalAmountFormat.INSTANCE.parse("1y 10M");
        Period period2 = Period.parse("P1Y10M");

        assertEquals(period2, period1);
    }

    @Test
    public void parseTestPeriod3() throws Exception {
        TemporalAmount period1 = TemporalAmountFormat.INSTANCE.parse("7w");
        Period period2 = Period.parse("P7W");

        assertEquals(period2, period1);
    }

    @Test
    public void parseZeroTest() throws Exception {
        TemporalAmount temporalAmount = TemporalAmountFormat.INSTANCE.parse("0");
        Instant now = Instant.now();
        Instant then = now.plus(temporalAmount);

        assertEquals(now, then);
    }

    @Test
    public void formatTestDuration1() throws Exception {
        String s = "5w4d3h2m1s";
        TemporalAmount duration = TemporalAmountFormat.INSTANCE.parse(s);
        assertEquals(s, TemporalAmountFormat.INSTANCE.format(duration));
    }

    @Test
    public void formatTestDuration2() throws Exception {
        String s = "2m59s";
        TemporalAmount duration = TemporalAmountFormat.INSTANCE.parse(s);
        assertEquals(s, TemporalAmountFormat.INSTANCE.format(duration));
    }

    @Test
    public void formatZeroTest() throws Exception {
        String s = "0";
        TemporalAmount duration = TemporalAmountFormat.INSTANCE.parse(s);
        assertEquals(s, TemporalAmountFormat.INSTANCE.format(duration));
    }

    @Test
    public void formatTestPeriod1() throws Exception {
        String s = "2y";
        TemporalAmount period = TemporalAmountFormat.INSTANCE.parse(s);
        assertEquals(s, TemporalAmountFormat.INSTANCE.format(period));
    }

    @Test
    public void formatTestPeriod2() throws Exception {
        String s = "2y2M";
        TemporalAmount period = TemporalAmountFormat.INSTANCE.parse(s);
        assertEquals(s, TemporalAmountFormat.INSTANCE.format(period));
    }

    @Test
    public void formatTestPeriod3() throws Exception {
        String s = "2M";
        TemporalAmount period = TemporalAmountFormat.INSTANCE.parse(s);
        assertEquals(s, TemporalAmountFormat.INSTANCE.format(period));
    }
}
