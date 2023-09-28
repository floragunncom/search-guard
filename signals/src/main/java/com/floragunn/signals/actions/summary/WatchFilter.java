package com.floragunn.signals.actions.summary;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Objects;

class WatchFilter {

    static class Range<T> {
        private final T equalTo;
        private final T greaterThan;
        private final T lessThan;

        public Range(T equalTo, T greaterThan, T lessThan) {
            this.equalTo = equalTo;
            this.greaterThan = greaterThan;
            this.lessThan = lessThan;
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
    }

    private final String watchId;
    private final List<String> watchStatusCodes;
    private final List<String> severities;
    private final Range<Integer> levelNumeric;

    WatchFilter(String watchId, List<String> watchStatusCodes, List<String> severities, Range<Integer> levelNumeric) {
        this.watchId = watchId;
        this.watchStatusCodes = Objects.requireNonNull(watchStatusCodes, "Watch status code list is null");
        this.severities = Objects.requireNonNull(severities);
        this.levelNumeric = levelNumeric;
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

    public boolean containsLevelNumeric() {
        return levelNumeric != null;
    }

    public Range<Integer> getLevelNumeric() {
        return levelNumeric;
    }
}
