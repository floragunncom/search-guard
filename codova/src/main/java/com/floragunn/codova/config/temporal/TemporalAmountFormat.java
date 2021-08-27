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

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

public class TemporalAmountFormat {
    public static final TemporalAmountFormat INSTANCE = new TemporalAmountFormat();

    private final Pattern pattern = Pattern.compile("((?<period>" //
            + PeriodFormat.PATTERN_STRING //
            + ")" //
            + "|" //
            + "(?<duration>" //
            + DurationFormat.PATTERN_STRING //
            + "))");

    public TemporalAmount parse(String temporalAmountString) throws ConfigValidationException {
        if (temporalAmountString == null) {
            return null;
        }

        if (temporalAmountString.equals("0")) {
            return Duration.ZERO;
        }

        Matcher matcher = pattern.matcher(temporalAmountString);

        if (!matcher.matches()) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, temporalAmountString,
                    "<Years>y? <Months>M? <Weeks>w? <Days>d?  |  <Days>d? <Hours>h? <Minutes>m? <Seconds>s? <Milliseconds>ms?"));
        }

        if (matcher.group("period") != null) {
            return PeriodFormat.INSTANCE.parse(matcher);
        } else {
            return DurationFormat.INSTANCE.parse(matcher);
        }

    }

    public String format(TemporalAmount temporalAmount) {
        if (temporalAmount == null) {
            return null;
        }

        if (temporalAmount instanceof Duration) {
            return DurationFormat.INSTANCE.format((Duration) temporalAmount);
        } else if (temporalAmount instanceof Period) {
            return PeriodFormat.INSTANCE.format((Period) temporalAmount);
        } else {
            throw new IllegalArgumentException("Unknown temporalAmount value: " + temporalAmount);
        }
    }

    static Long getNumericMatch(Matcher matcher, String name) {
        String group = matcher.group(name);

        if (group != null) {
            return Long.parseLong(group);
        } else {
            return null;
        }
    }
}
