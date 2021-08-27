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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

public class ExpontentialDurationExpression implements DurationExpression {
    private final static Duration DEFAULT_MAX_DURATION = Duration.ofDays(1);
    private final static Pattern PARSE_PATTERN = Pattern.compile("([0-9wdhms\\s\\.]+)\\*\\*([0-9]+\\.?[0-9]*)(\\|([0-9wdhms\\s\\.]+))?");

    private final Duration initialDuration;
    private final double basis;
    private final int iterationCeiling;
    private final Duration maxDuration;

    public ExpontentialDurationExpression(Duration initialDuration, double basis, Duration maxDuration) {
        this.initialDuration = initialDuration;
        this.basis = basis;
        this.maxDuration = maxDuration != null ? maxDuration : DEFAULT_MAX_DURATION;

        // ad = d * b ^ i
        // ad / d = b ^ i
        // log_b(ad / d) = i
        this.iterationCeiling = (int) Math
                .ceil(Math.log((double) this.maxDuration.toMillis() / (double) initialDuration.toMillis()) / Math.log(basis));
    }

    public ExpontentialDurationExpression(Duration initialDuration, double basis, int maxIterations) {
        this.initialDuration = initialDuration;
        this.basis = basis;
        this.iterationCeiling = maxIterations;
        this.maxDuration = null;
    }

    @Override
    public Duration getActualDuration(int iteration) {
        if (iteration > iterationCeiling) {
            iteration = iterationCeiling;
        }

        Duration result = Duration.ofMillis((long) (((double) this.initialDuration.toMillis()) * Math.pow(this.basis, iteration)));

        if (maxDuration != null && result.compareTo(maxDuration) > 0) {
            result = maxDuration;
        }

        return result;
    }

    public String toString() {
        DecimalFormatSymbols locale = new DecimalFormatSymbols(Locale.US);

        if (this.maxDuration == null || this.maxDuration.equals(DEFAULT_MAX_DURATION)) {
            return DurationFormat.INSTANCE.format(initialDuration) + "**" + new DecimalFormat("0.##", locale).format(basis);
        } else {
            return DurationFormat.INSTANCE.format(initialDuration) + "**" + new DecimalFormat("0.##", locale).format(basis) + "|"
                    + DurationFormat.INSTANCE.format(maxDuration);
        }
    }

    public static DurationExpression tryParse(String string) throws ConfigValidationException {
        Matcher m = PARSE_PATTERN.matcher(string);

        if (!m.matches()) {
            return null;
        }

        try {
            Duration initialDuration = DurationFormat.INSTANCE.parse(m.group(1));
            double basis = Double.parseDouble(m.group(2));
            Duration maxDuration = DurationFormat.INSTANCE.parse(m.group(4));

            return new ExpontentialDurationExpression(initialDuration, basis, maxDuration);
        } catch (ConfigValidationException | NumberFormatException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, string,
                    "<Duration>**<Exp Basis>|<Max Duration>?; Duration is: <Weeks>w? <Days>d? <Hours>h? <Minutes>m? <Seconds>s? <Milliseconds>ms?")
                            .cause(e));
        }
    }
}
