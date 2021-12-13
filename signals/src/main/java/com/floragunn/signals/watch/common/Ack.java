package com.floragunn.signals.watch.common;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;

public class Ack implements ToXContentObject {
    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    private final Instant on;
    private final String by;

    public Ack(Instant on, String by) {
        this.on = on;
        this.by = by;
    }

    public Instant getOn() {
        return on;
    }

    public String getBy() {
        return by;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("on", DATE_FORMATTER.format(on));

        if (by != null) {
            builder.field("by", by);
        }

        builder.endObject();
        return builder;
    }

    public static Ack create(JsonNode jsonNode) {
        Instant on = null;
        String by = null;

        if (jsonNode.hasNonNull("on")) {
            on = Instant.from(DATE_FORMATTER.parse(jsonNode.get("on").asText()));
        }

        if (jsonNode.hasNonNull("by")) {
            by = jsonNode.get("by").asText();
        }

        return new Ack(on, by);
    }

    @Override
    public String toString() {
        return "Ack [on=" + on + ", by=" + by + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((by == null) ? 0 : by.hashCode());
        result = prime * result + ((on == null) ? 0 : on.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Ack other = (Ack) obj;
        if (by == null) {
            if (other.by != null)
                return false;
        } else if (!by.equals(other.by))
            return false;
        if (on == null) {
            if (other.on != null)
                return false;
        } else if (!on.equals(other.on))
            return false;
        return true;
    }
}
