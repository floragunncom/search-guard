package com.floragunn.signals.actions.summary;

import com.floragunn.signals.actions.summary.WatchFilter.Range;

public class RangesFilters {

    private final Range levelNumericRange;
    private final Range actionsCheckedRange;
    private final Range actionsTriggeredRange;
    private final Range actionsExecutionRange;

    public RangesFilters(Range levelNumericRange, Range actionsCheckedRange, Range actionsTriggeredRange, Range actionsExecutionRange) {
        this.levelNumericRange = levelNumericRange;
        this.actionsCheckedRange = actionsCheckedRange;
        this.actionsTriggeredRange = actionsTriggeredRange;
        this.actionsExecutionRange = actionsExecutionRange;
    }

    public Range getLevelNumericRange() {
        return levelNumericRange;
    }

    public Range getActionsCheckedRange() {
        return actionsCheckedRange;
    }

    public Range getActionsTriggeredRange() {
        return actionsTriggeredRange;
    }

    public Range getActionsExecutionRange() {
        return actionsExecutionRange;
    }

    @Override public String toString() {
        return "RangesFilters{" + "levelNumericRange=" + levelNumericRange + ", actionsCheckedRange=" + actionsCheckedRange + ", actionsTriggeredRange=" + actionsTriggeredRange + ", actionsExecutionRange=" + actionsExecutionRange + '}';
    }
}
