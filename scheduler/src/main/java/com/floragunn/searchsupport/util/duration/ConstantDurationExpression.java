package com.floragunn.searchsupport.util.duration;

import java.time.Duration;

public class ConstantDurationExpression implements DurationExpression {

    private final Duration duration;

    public ConstantDurationExpression(Duration duration) {
        this.duration = duration;
    }

    @Override
    public Duration getActualDuration(int iteration) {
        return duration;
    }

    public String toString() {
        return DurationFormat.INSTANCE.format(duration);
    }
}
