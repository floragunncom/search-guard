package com.floragunn.searchsupport.util.temporal;

import java.time.Duration;

import com.floragunn.searchsupport.config.validation.ConfigValidationException;

public interface DurationExpression {
    Duration getActualDuration(int iteration);

    public static DurationExpression parse(String string) throws ConfigValidationException {
        if (string == null) {
            return null;
        }

        DurationExpression result = ExpontentialDurationExpression.tryParse(string);

        if (result != null) {
            return result;
        } else {
            return new ConstantDurationExpression(DurationFormat.INSTANCE.parse(string));
        }
    }
}