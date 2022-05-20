/*
 * Copyright 2020-2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.signals.watch.common;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;

import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;

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

    public static Ack create(DocNode jsonNode) {
        Instant on = null;
        String by = null;

        if (jsonNode.hasNonNull("on")) {
            on = Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("on")));
        }

        if (jsonNode.hasNonNull("by")) {
            by = jsonNode.getAsString("by");
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
