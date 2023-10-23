package com.floragunn.signals.actions.summary;

import com.google.common.base.Strings;
import java.util.List;
import java.util.Objects;

class WatchFilter {

    static class Range<T> {
        private final T equalTo;
        private final T greaterThan;
        private final T lessThan;
        private final String fieldName;

        public Range(T equalTo, T greaterThan, T lessThan, String fieldName) {
            this.equalTo = equalTo;
            this.greaterThan = greaterThan;
            this.lessThan = lessThan;
            this.fieldName = fieldName;
        }

        public T getEqualTo() {
            return equalTo;
        }

        public T getGreaterThan() {
            return greaterThan;
        }

        public T getLessThan() {
            return lessThan;
        }

        public boolean containsEqualTo() {
            return equalTo != null;
        }

        public boolean containsLessThan() {
            return lessThan != null;
        }

        public boolean containsGreaterThan() {
            return greaterThan != null;
        }

        public String getFieldName() {
            return fieldName;
        }

        @Override
        public String toString() {
            return "Range{" + "equalTo=" + equalTo + ", greaterThan=" + greaterThan + ", lessThan=" + lessThan + ", fieldName='" + fieldName + '\'' + '}';
        }
    }

    private final String watchId;
    private final List<String> watchStatusCodes;
    private final List<String> severities;
    private final List<String> actionNames;
    private final ActionProperties actionProperties;
    private final RangesFilters ranges;

    WatchFilter(String watchId, List<String> watchStatusCodes, List<String> severities, List<String> actionNames,
        RangesFilters ranges, ActionProperties actionProperties) {
        this.watchId = watchId;
        this.watchStatusCodes = Objects.requireNonNull(watchStatusCodes, "Watch status code list is null");
        this.severities = Objects.requireNonNull(severities);
        this.actionNames = actionNames;
        this.ranges = ranges;
        this.actionProperties = actionProperties;
    }

    public List<String> getWatchStatusCodes() {
        return watchStatusCodes;
    }

    public boolean containsWatchId() {
        return ! Strings.isNullOrEmpty(watchId);
    }

    public String getWatchId() {
        return watchId;
    }

    public boolean containsWatchStatusFilter() {
        return ! watchStatusCodes.isEmpty();
    }

    public boolean containsSeverities() {
        return ! severities.isEmpty();
    }

    public List<String> getSeverities() {
        return severities;
    }

    public List<String> getActionNames() {
        return actionNames;
    }

    public boolean containsActions() {
        return !actionNames.isEmpty();
    }

    public ActionProperties getActionProperties() {
        return actionProperties;
    }

    public RangesFilters getRangesFilters() {
        return ranges;
    }
}
