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
