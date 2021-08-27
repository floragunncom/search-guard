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

import org.junit.Test;

public class DurationFormatTest {

    @Test
    public void parseTest1() throws Exception {
        Duration duration1 = DurationFormat.INSTANCE.parse("3d 2h 1m");
        Duration duration2 = Duration.parse("P3DT2H1M");

        assertEquals(duration2, duration1);
    }

    @Test
    public void parseTest2() throws Exception {
        Duration duration1 = DurationFormat.INSTANCE.parse("20m");
        Duration duration2 = Duration.parse("PT20M");

        assertEquals(duration2, duration1);
    }

    @Test
    public void parseTest3() throws Exception {
        Duration duration1 = DurationFormat.INSTANCE.parse("3d2h1m");
        Duration duration2 = Duration.parse("P3DT2H1M");

        assertEquals(duration2, duration1);
    }

    @Test
    public void parseZeroTest() throws Exception {
        Duration duration1 = DurationFormat.INSTANCE.parse("0");

        assertEquals(0, duration1.getSeconds());
        assertEquals(0, duration1.getNano());
    }

    @Test
    public void formatTest1() throws Exception {
        String s = "5w4d3h2m1s";
        Duration duration = DurationFormat.INSTANCE.parse(s);
        assertEquals(s, DurationFormat.INSTANCE.format(duration));
    }
    
    @Test
    public void formatTest2() throws Exception {
        String s = "2m59s";
        Duration duration = DurationFormat.INSTANCE.parse(s);
        assertEquals(s, DurationFormat.INSTANCE.format(duration));
    }
    
    @Test
    public void formatZeroTest() throws Exception {
        String s = "0";
        Duration duration = DurationFormat.INSTANCE.parse(s);
        assertEquals(s, DurationFormat.INSTANCE.format(duration));
    }
}
