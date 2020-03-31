package com.floragunn.searchsupport.util.duration;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.InvalidAttributeValue;

public class DurationFormat {

    public static final DurationFormat INSTANCE = new DurationFormat();

    private final Pattern pattern = Pattern
            .compile("((?<w>[0-9]+)w)?\\s*((?<d>[0-9]+)d)?\\s*((?<h>[0-9]+)h)?\\s*((?<m>[0-9]+)m)?\\s*((?<s>[0-9]+)s)?\\s*((?<ms>[0-9]+)ms)?");

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

    private Long getNumericMatch(Matcher matcher, String name) {
        String group = matcher.group(name);

        if (group != null) {
            return Long.parseLong(group);
        } else {
            return null;
        }
    }
}
