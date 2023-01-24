/*
 * Copyright 2021-2022 floragunn GmbH
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
package com.floragunn.searchsupport.cstate.metrics;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CountAggregation extends Measurement<CountAggregation> {
    public static CountAggregation basic(MetricsLevel level) {
        if (level.basicEnabled()) {
            return new CountAggregation();
        } else {
            return NOOP;
        }
    }

    public static CountAggregation noop() {
        return NOOP;
    }

    final static String TYPE = "agg";

    private final static OrderedImmutableMap<String, Object> ZERO_COUNT = OrderedImmutableMap.of("count", 0);
    private final static CountAggregation NOOP = new CountAggregation(0) {

        @Override
        public void increment() {
        }

        @Override
        public CountAggregation getSubCount(String name) {
            return this;
        }

        @Override
        public void addToThis(CountAggregation other) {
        }

        @Override
        public void addToThis(Measurement<?> other) {
        }

    };

    private final AtomicLong count = new AtomicLong();
    private final Map<String, CountAggregation> subCounts;

    public CountAggregation() {
        this.subCounts = new ConcurrentHashMap<>();
    }

    CountAggregation(long count) {
        this.count.set(count);
        this.subCounts = new HashMap<>();
    }

    CountAggregation(DocNode docNode) {
        if (docNode.get("count") instanceof Number) {
            this.count.set(((Number) docNode.get("count")).longValue());
        }

        this.subCounts = new HashMap<>();

        if (docNode.hasNonNull("parts")) {
            DocNode partsNode = docNode.getAsNode("parts");

            for (String key : partsNode.keySet()) {
                this.subCounts.put(key, new CountAggregation(partsNode.getAsNode(key)));
            }
        }
    }

    public void increment() {
        this.count.incrementAndGet();
    }

    public void add(long number) {
        this.count.addAndGet(number);
    }

    public CountAggregation getSubCount(String name) {
        return this.subCounts.computeIfAbsent(name, (k) -> new CountAggregation());
    }

    @Override
    public Object toBasicObject() {
        long count = this.count.get();

        if (count == 0) {
            return ZERO_COUNT;
        } else if (subCounts.size() != 0) {
            return OrderedImmutableMap.of("count", count, "parts", subCounts);
        } else {
            return OrderedImmutableMap.of("count", count);
        }
    }

    @Override
    public Measurement<CountAggregation> clone() {
        return new CountAggregation(this.count.get());
    }

    @Override
    public void addToThis(CountAggregation other) {
        this.count.addAndGet(other.count.get());

        if (other.subCounts.size() != 0) {
            for (Map.Entry<String, CountAggregation> entry : other.subCounts.entrySet()) {
                CountAggregation part = getSubCount(entry.getKey());
                part.addToThis(entry.getValue());
            }
        }
    }

    @Override
    public void addToThis(Measurement<?> other) {
        if (other instanceof CountAggregation) {
            addToThis((CountAggregation) other);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void reset() {
        this.count.set(0);
        this.subCounts.forEach((k, v) -> v.reset());
    }

}
