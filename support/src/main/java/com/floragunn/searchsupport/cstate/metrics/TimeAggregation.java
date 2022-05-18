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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.OrderedImmutableMap;

public abstract class TimeAggregation extends Measurement<TimeAggregation> {
    public abstract void recordMs(long ms);

    public abstract long getCount();

    public abstract long getAggMs();

    public abstract int getMinMs();

    public abstract int getMaxMs();

    public abstract TimeAggregation getSubAggregation(String name);

    public abstract CountAggregation getCountAggregation(String name);

    public abstract Map<String, ? extends Measurement<?>> getSubMeasurements();

    public static class Milliseconds extends TimeAggregation {
        final static String TYPE = "agg";

        private final static OrderedImmutableMap<String, Object> ZERO_COUNT = OrderedImmutableMap.of("count", 0);

        private AtomicLong aggMs = new AtomicLong();
        private AtomicInteger minMs = new AtomicInteger();
        private AtomicInteger maxMs = new AtomicInteger();
        private AtomicLong count = new AtomicLong();

        private Map<String, Measurement<?>> subMeasurements;

        public Milliseconds() {
            this.subMeasurements = new ConcurrentHashMap<>();
        }

        Milliseconds(long aggMs, long count) {
            this.aggMs.set(aggMs);
            this.count.set(count);
        }

        Milliseconds(DocNode docNode) throws ConfigValidationException {
            this.aggMs.set(docNode.hasNonNull("agg_ms") ? docNode.getNumber("agg_ms").longValue() : 0);
            this.count.set(docNode.hasNonNull("count") ? docNode.getNumber("count").longValue() : 0);
            this.maxMs.set(docNode.hasNonNull("max_ms") ? docNode.getNumber("max_ms").intValue() : 0);
            this.minMs.set(docNode.hasNonNull("min_ms") ? docNode.getNumber("min_ms").intValue() : 0);

            if (docNode.hasNonNull("parts")) {
                DocNode sub = docNode.getAsNode("parts");
                this.subMeasurements = new HashMap<String, Measurement<?>>(sub.size());
                for (String key : sub.keySet()) {
                    this.subMeasurements.put(key, new Milliseconds(sub.getAsNode(key)));
                }
            } else {
                this.subMeasurements = new HashMap<>();
            }
        }

        public long getCount() {
            return count.get();
        }

        public void setCount(long count) {
            this.count.set(count);
        }

        public long getAggMs() {
            return aggMs.get();
        }

        public void setAggMs(long aggMs) {
            this.aggMs.set(aggMs);
        }

        public int getMinMs() {
            return minMs.get();
        }

        public void setMinMs(int minMs) {
            this.minMs.set(minMs);
        }

        public int getMaxMs() {
            return maxMs.get();
        }

        public void setMaxMs(int maxMs) {
            this.maxMs.set(maxMs);
        }

        @Override
        public Milliseconds clone() {
            Milliseconds result = new Milliseconds();
            result.aggMs = this.aggMs;
            result.minMs = this.minMs;
            result.maxMs = this.maxMs;
            result.count = this.count;
            return result;
        }

        @Override
        public void addToThis(TimeAggregation other) {
            aggMs.addAndGet(other.getAggMs());
            count.addAndGet(other.getCount());

            minMs.set(Math.min(minMs.get(), other.getMinMs()));
            maxMs.set(Math.max(maxMs.get(), other.getMaxMs()));

            @SuppressWarnings("unchecked")
            Map<String, TimeAggregation> otherMap = (Map<String, TimeAggregation>) other.getSubMeasurements();

            if (otherMap != null && otherMap.size() != 0) {
                for (Map.Entry<String, TimeAggregation> otherEntry : otherMap.entrySet()) {
                    String key = otherEntry.getKey();
                    Measurement<?> here = this.subMeasurements.get(key);

                    if (here != null) {
                        here.addToThis(otherEntry.getValue());
                    } else {
                        this.subMeasurements.put(key, otherEntry.getValue());
                    }
                }
            }
        }

        @Override
        public void addToThis(Measurement<?> other) {
            if (other instanceof TimeAggregation) {
                addToThis((Milliseconds) other);
            }
        }

        @Override
        public void recordMs(long ms) {
            count.incrementAndGet();
            aggMs.addAndGet(ms);

            int min = minMs.get();

            if (ms < min) {
                minMs.set((int) ms);
            }

            int max = maxMs.get();

            if (ms > max) {
                maxMs.set((int) ms);
            }
        }

        @Override
        public TimeAggregation getSubAggregation(String name) {
            return (TimeAggregation) this.subMeasurements.computeIfAbsent(name, (k) -> new Milliseconds());
        }

        @Override
        public CountAggregation getCountAggregation(String name) {
            return (CountAggregation) this.subMeasurements.computeIfAbsent(name, (k) -> new CountAggregation());
        }

        @Override
        public void reset() {
            this.count.set(0);
            this.subMeasurements.forEach((k, v) -> v.reset());
        }

        @Override
        public Object toBasicObject() {
            long count = this.count.get();
            long aggMs = this.aggMs.get();

            if (count == 0) {
                return ZERO_COUNT;
            } else {
                OrderedImmutableMap<String, Object> result = OrderedImmutableMap.of("count", count, "avg_ms", ((double) aggMs) / ((double) count),
                        "agg_ms", aggMs, "min_ms", minMs.get(), "max_ms", maxMs.get());

                if (this.subMeasurements != null && this.subMeasurements.size() != 0) {
                    result = result.with("parts", this.subMeasurements);
                }

                return result;
            }
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public Map<String, ? extends Measurement<?>> getSubMeasurements() {
            return subMeasurements;
        }

    }
}
