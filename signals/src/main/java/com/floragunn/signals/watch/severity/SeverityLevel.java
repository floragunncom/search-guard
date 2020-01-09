package com.floragunn.signals.watch.severity;

import java.io.IOException;
import java.util.EnumSet;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;

public enum SeverityLevel implements Comparable<SeverityLevel> {
    NONE(0, "None"), INFO(1, "Info"), WARNING(2, "Warning"), ERROR(3, "Error"), CRITICAL(4, "Critical");

    private final int level;
    private final String name;
    private final String id;

    SeverityLevel(int level, String name) {
        this.level = level;
        this.name = name;
        this.id = name().toLowerCase();
    }

    public int getLevel() {
        return level;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public String toString() {
        return id;
    }

    public boolean isHigherThan(SeverityLevel other) {
        if (other == null) {
            return true;
        }

        return this.level > other.level;
    }

    public boolean isLowerThan(SeverityLevel other) {
        if (other == null) {
            return false;
        }

        return this.level < other.level;
    }

    public boolean isGained(SeverityLevel oldLevel, SeverityLevel newLevel) {
        if (oldLevel == null && newLevel == null) {
            return false;
        }

        if (oldLevel == null && newLevel != null) {
            return this.level <= newLevel.level;
        }

        if (oldLevel != null && newLevel == null) {
            return false;
        }

        return oldLevel.level < this.level && this.level <= newLevel.level;
    }

    public boolean isLost(SeverityLevel oldLevel, SeverityLevel newLevel) {
        if (oldLevel == null && newLevel == null) {
            return false;
        }

        if (oldLevel == null && newLevel != null) {
            return false;
        }

        if (oldLevel != null && newLevel == null) {
            return this.level <= oldLevel.level;
        }

        return oldLevel.level >= this.level && this.level > newLevel.level;
    }

    public static int compare(SeverityLevel l1, SeverityLevel l2) {
        if (l1 == null) {
            l1 = SeverityLevel.NONE;
        }

        if (l2 == null) {
            l2 = SeverityLevel.NONE;
        }

        return l1.level - l2.level;
    }

    public static SeverityLevel getById(String id) {
        if (id == null) {
            return null;
        } else {
            return valueOf(id.toUpperCase());
        }
    }

    public static class Set implements ToXContent {
        private final EnumSet<SeverityLevel> levels;

        public Set(SeverityLevel first, SeverityLevel... rest) {
            this(EnumSet.of(first, rest));
        }

        private Set(EnumSet<SeverityLevel> levels) {
            this.levels = levels;
        }

        public boolean contains(SeverityLevel level) {
            return this.levels.contains(level);
        }

        public boolean isGained(SeverityLevel oldLevel, SeverityLevel newLevel) {
            if (oldLevel == null && newLevel == null) {
                return false;
            }

            for (SeverityLevel level : levels) {
                if (level.isGained(oldLevel, newLevel)) {
                    return true;
                }
            }

            return false;
        }

        public boolean isSubsetOf(java.util.Set<SeverityLevel> other) {
            return other.containsAll(this.levels);
        }

        public java.util.Set<SeverityLevel> missingFromOther(java.util.Set<SeverityLevel> other) {
            java.util.Set<SeverityLevel> result = EnumSet.copyOf(this.levels);
            result.removeAll(other);
            return result;
        }

        public SeverityLevel getLowest() {
            SeverityLevel result = null;

            for (SeverityLevel level : this.levels) {
                if (result == null || level.level < result.level) {
                    result = level;
                }
            }

            return result;
        }

        public boolean isLost(SeverityLevel oldLevel, SeverityLevel newLevel) {
            if (oldLevel == null && newLevel == null) {
                return false;
            }

            for (SeverityLevel level : levels) {
                if (level.isLost(oldLevel, newLevel)) {
                    return true;
                }
            }

            return false;
        }

        public static Set create(JsonNode jsonNode) throws ConfigValidationException {
            if (jsonNode == null) {
                return null;
            }

            if (!jsonNode.isArray()) {
                throw new ConfigValidationException(new InvalidAttributeValue(null, jsonNode, "Array of severities", jsonNode));
            }

            ValidationErrors validationErrors = new ValidationErrors();

            EnumSet<SeverityLevel> result = EnumSet.noneOf(SeverityLevel.class);

            int i = 0;

            for (JsonNode severityLevelNode : jsonNode) {
                try {
                    SeverityLevel severityLevel = SeverityLevel.valueOf(severityLevelNode.asText().toUpperCase());

                    result.add(severityLevel);
                } catch (IllegalArgumentException e) {
                    validationErrors
                            .add(new InvalidAttributeValue(i + "", severityLevelNode.asText(), "info|warning|error|critical", severityLevelNode));
                }

                i++;
            }

            validationErrors.throwExceptionForPresentErrors();

            return new Set(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();

            for (SeverityLevel level : levels) {
                builder.value(level.id);
            }

            builder.endArray();
            return builder;
        }

        public boolean equals(Object o) {
            return levels.equals(o);
        }

        public int size() {
            return levels.size();
        }

        public String toString() {
            return levels.toString();
        }

    }
}
