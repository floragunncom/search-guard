package com.floragunn.signals.watch.common.throttle;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.settings.SignalsSettings;

public abstract class ThrottlePeriodParser {

    protected final SignalsSettings signalsSettings;

    protected ThrottlePeriodParser(SignalsSettings signalsSettings) {
        this.signalsSettings = signalsSettings;
    }

    public abstract DurationExpression parseThrottle(String throttle) throws ConfigValidationException;

    public DurationExpression getDefaultThrottle() {
        return signalsSettings.getThrottlePeriodLowerBound();
    }

}
