/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.signals.watch.common.throttle;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.signals.settings.SignalsSettings;

import java.time.Duration;

public class ValidatingThrottlePeriodParser extends ThrottlePeriodParser {

    public ValidatingThrottlePeriodParser(SignalsSettings signalsSettings) {
        super(signalsSettings);
    }

    @Override
    public DurationExpression parseThrottle(String throttle) throws ConfigValidationException {
        DurationExpression lowerBoundExpression = signalsSettings.getThrottlePeriodLowerBound();
        if (throttle == null) {
            return lowerBoundExpression;
        }
        DurationExpression throttleDurationExpression = DurationExpression.parse(throttle);
        if (lowerBoundExpression != null) {
            Duration lowerBoundDuration = lowerBoundExpression.getActualDuration(0);
            Duration throttleDuration = throttleDurationExpression.getActualDuration(0);
            if (throttleDuration.compareTo(lowerBoundDuration) < 0) {
                throw new ConfigValidationException(new InvalidAttributeValue(
                        null, throttle,
                        String.format("Throttle period: %s longer than configured lower bound: %s", throttleDurationExpression, lowerBoundExpression)
                ));
            }
        }
        return throttleDurationExpression;
    }
}
