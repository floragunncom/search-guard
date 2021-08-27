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

import static com.floragunn.codova.config.temporal.TemporalAmountFormat.getNumericMatch;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

public class DurationFormat {

    public static final DurationFormat INSTANCE = new DurationFormat();

    static final String PATTERN_STRING = "((?<w>[0-9]+)w)??\\s*" //
            + "((?<d>[0-9]+)d)??\\s*" //
            + "((?<h>[0-9]+)h)?\\s*" //
            + "((?<m>[0-9]+)m)?\\s*" //
            + "((?<s>[0-9]+)s)?\\s*" //
            + "((?<ms>[0-9]+)ms)?";

    private final Pattern pattern = Pattern.compile(PATTERN_STRING);

    public Duration parse(String durationString) throws ConfigValidationException {
        if (durationString == null) {
            return null;
        }

        if (durationString.equals("0")) {
            return Duration.ZERO;
        }

        Matcher matcher = pattern.matcher(durationString);

        if (!matcher.matches()) {
            throw new ConfigValidationException(
                    new InvalidAttributeValue(null, durationString, "<Weeks>w? <Days>d? <Hours>h? <Minutes>m? <Seconds>s? <Milliseconds>ms?"));
        }

        return parse(matcher);
    }

    Duration parse(Matcher matcher) {

        Duration result = Duration.ZERO;

        Long w = getNumericMatch(matcher, "w");

        if (w != null) {
            result = result.plusDays(7 * w);
        }

        Long d = getNumericMatch(matcher, "d");

        if (d != null) {
            result = result.plusDays(d);
        }

        Long h = getNumericMatch(matcher, "h");

        if (h != null) {
            result = result.plusHours(h);
        }

        Long m = getNumericMatch(matcher, "m");

        if (m != null) {
            result = result.plusMinutes(m);
        }

        Long s = getNumericMatch(matcher, "s");

        if (s != null) {
            result = result.plusSeconds(s);
        }

        Long ms = getNumericMatch(matcher, "ms");

        if (ms != null) {
            result = result.plusMillis(ms);
        }

        return result;
    }

    public String format(Duration duration) {
        if (duration == null) {
            return null;
        }

        if (duration.isZero()) {
            return "0";
        }

        if (duration.isNegative()) {
            throw new IllegalArgumentException("Negative durations are not supported");
        }

        StringBuilder result = new StringBuilder();

        long seconds = duration.getSeconds();
        int nanos = duration.getNano();

        long minutes = seconds / 60;
        seconds -= minutes * 60;

        long hours = minutes / 60;
        minutes -= hours * 60;

        long days = hours / 24;
        hours -= days * 24;

        long weeks = days / 7;
        days -= weeks * 7;

        int millis = nanos / 1000000;

        if (weeks != 0) {
            result.append(weeks).append("w");
        }

        if (days != 0) {
            result.append(days).append("d");
        }

        if (hours != 0) {
            result.append(hours).append("h");
        }

        if (minutes != 0) {
            result.append(minutes).append("m");
        }

        if (seconds != 0) {
            result.append(seconds).append("s");
        }

        if (millis != 0) {
            result.append(millis).append("ms");
        }

        return result.toString();
    }

}
