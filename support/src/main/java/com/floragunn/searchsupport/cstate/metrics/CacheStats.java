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
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.google.common.cache.Cache;

public abstract class CacheStats extends Measurement<CacheStats> {
    final static String TYPE = "cache";

    public abstract int getCurrentSize();

    public abstract long getHitCount();

    public abstract long getMissCount();

    public abstract long getEvictionCount();

    @Override
    public CacheStats clone() {
        CacheStats.Static result = new CacheStats.Static();
        result.currentSize = this.getCurrentSize();
        result.hitCount = this.getHitCount();
        result.missCount = this.getMissCount();
        result.evictionCount = this.getEvictionCount();

        return result;
    }

    public static CacheStats from(Cache<?, ?> cache) {
        return new Live(cache);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    static class Live extends CacheStats {
        private final Cache<?, ?> cache;

        Live(Cache<?, ?> cache) {
            this.cache = cache;
        }

        @Override
        public Object toBasicObject() {
            com.google.common.cache.CacheStats cacheStats = cache.stats();

            return OrderedImmutableMap.of("current_size", cache.size(), "hit_count", cacheStats.hitCount(), "miss_count", cacheStats.missCount(),
                    "eviction_count", cacheStats.evictionCount());
        }

        @Override
        public int getCurrentSize() {
            return (int) cache.size();
        }

        @Override
        public long getHitCount() {
            return cache.stats().hitCount();
        }

        @Override
        public long getMissCount() {
            return cache.stats().missCount();
        }

        @Override
        public long getEvictionCount() {
            return cache.stats().evictionCount();
        }

        @Override
        public void addToThis(CacheStats other) {

        }

        @Override
        public void addToThis(Measurement<?> other) {

        }

        @Override
        public CacheStats clone() {
            Static result = new Static();
            result.currentSize = (int) cache.size();
            com.google.common.cache.CacheStats cacheStats = cache.stats();
            result.hitCount = cacheStats.hitCount();
            result.missCount = cacheStats.loadCount();
            result.evictionCount = cacheStats.evictionCount();

            return result;
        }

        @Override
        public void reset() {

        }
    }

    static class Static extends CacheStats {
        private int currentSize;
        private long hitCount;
        private long missCount;
        private long evictionCount;

        public Static() {

        }

        Static(DocNode docNode) throws ConfigValidationException {
            this.currentSize = docNode.getNumber("current_size") != null ? docNode.getNumber("current_size").intValue() : 0;
            this.hitCount = docNode.getNumber("hit_count") != null ? docNode.getNumber("hit_count").longValue() : 0;
            this.missCount = docNode.getNumber("miss_count") != null ? docNode.getNumber("miss_count").longValue() : 0;
            this.evictionCount = docNode.getNumber("eviction_count") != null ? docNode.getNumber("eviction_count").longValue() : 0;
        }

        public int getCurrentSize() {
            return currentSize;
        }

        public void setCurrentSize(int currentSize) {
            this.currentSize = currentSize;
        }

        public long getHitCount() {
            return hitCount;
        }

        public void setHitCount(long hitCount) {
            this.hitCount = hitCount;
        }

        public long getMissCount() {
            return missCount;
        }

        public void setMissCount(long missCount) {
            this.missCount = missCount;
        }

        public long getEvictionCount() {
            return evictionCount;
        }

        public void setEvictionCount(long evictionCount) {
            this.evictionCount = evictionCount;
        }

        @Override
        public CacheStats clone() {
            Static result = new Static();
            result.currentSize = this.currentSize;
            result.hitCount = this.hitCount;
            result.missCount = this.missCount;
            result.evictionCount = this.evictionCount;

            return result;
        }

        public static CacheStats copyFrom(Cache<?, ?> cache) {
            Static result = new Static();
            result.currentSize = (int) cache.size();
            com.google.common.cache.CacheStats cacheStats = cache.stats();
            result.hitCount = cacheStats.hitCount();
            result.missCount = cacheStats.loadCount();
            result.evictionCount = cacheStats.evictionCount();

            return result;
        }

        @Override
        public void addToThis(CacheStats other) {
            this.currentSize += other.getCurrentSize();
            this.hitCount += other.getHitCount();
            this.missCount += other.getMissCount();
            this.evictionCount += other.getEvictionCount();
        }

        @Override
        public void addToThis(Measurement<?> other) {
            if (other instanceof CacheStats) {
                this.addToThis((CacheStats) other);
            }
        }

        @Override
        public Object toBasicObject() {
            return OrderedImmutableMap.of("current_size", currentSize, "hit_count", hitCount, "miss_count", missCount, "eviction_count",
                    evictionCount);
        }

        @Override
        public void reset() {

        }

    }

}
