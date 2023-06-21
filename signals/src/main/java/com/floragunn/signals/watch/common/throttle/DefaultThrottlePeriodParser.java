package com.floragunn.signals.watch.common.throttle;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.settings.SignalsSettings;

import java.time.Duration;

public class DefaultThrottlePeriodParser extends ThrottlePeriodParser {

    public DefaultThrottlePeriodParser(SignalsSettings signalsSettings) {
        super(signalsSettings);
    }

    @Override
    public DurationExpression parseThrottle(String throttle) throws ConfigValidationException {
        DurationExpression lowerBound = signalsSettings.getThrottlePeriodLowerBound();
        if (throttle == null) {
            return lowerBound;
        }
        DurationExpression throttleDurationExpression = DurationExpression.parse(throttle);
        if (lowerBound != null) {
            Duration throttleInitialDuration = throttleDurationExpression.getActualDuration(0);
            Duration lowerBoundInitialDuration = lowerBound.getActualDuration(0);
            return throttleInitialDuration.compareTo(lowerBoundInitialDuration) < 0? lowerBound : throttleDurationExpression;
        }
        return throttleDurationExpression;
    }
}
