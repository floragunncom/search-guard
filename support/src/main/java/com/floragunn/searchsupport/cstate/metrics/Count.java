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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.floragunn.codova.documents.DocNode;

public class Count extends Measurement<Count> {
    final static String TYPE = "count";

    private final AtomicLong count = new AtomicLong();

    public Count() {

    }

    public Count(long count) {
        this.count.set(count);
    }

    Count(DocNode docNode) {
        if (docNode.toBasicObject() instanceof Number) {
            this.count.set(((Number) docNode.toBasicObject()).longValue());
        }
    }

    public void increment() {
        this.count.incrementAndGet();
    }

    @Override
    public Object toBasicObject() {
        return count.get();
    }

    @Override
    public Measurement<Count> clone() {
        return new Count(this.count.get());
    }

    @Override
    public void addToThis(Count other) {
        this.count.addAndGet(other.count.get());
    }

    @Override
    public void addToThis(Measurement<?> other) {
        if (other instanceof Count) {
            addToThis((Count) other);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void reset() {
        this.count.set(0);
    }
    
    public void set(long count) {
        this.count.set(count);
    }
    
    public static class Live extends Measurement<Count> {
        
        private final Supplier<Long> countSupplier;
        
        public Live(Supplier<Long> countSupplier) {
            this.countSupplier = countSupplier;
        }

        @Override
        public Object toBasicObject() {
            return this.countSupplier.get();
        }

        @Override
        public Measurement<Count> clone() {
            return new Count(this.countSupplier.get());
        }

        @Override
        public void addToThis(Count other) {
            // not supported
        }

        @Override
        public void addToThis(Measurement<?> other) {
            // not supported
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public void reset() {
            // not supported
        }
        
    }
}
