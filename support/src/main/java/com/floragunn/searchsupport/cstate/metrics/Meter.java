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

import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public interface Meter extends AutoCloseable {
    static Meter basic(MetricsLevel level, TimeAggregation sink) {
        if (level.basicEnabled()) {
            return new SystemCurrentTimeMillisMeter(level, sink);
        } else {
            return NO_OP;
        }
    }

    static Meter basic(MetricsLevel level, Measurement<?> sink) {
        if (level.basicEnabled()) {
            if (sink instanceof TimeAggregation) {
                return new SystemCurrentTimeMillisMeter(level, (TimeAggregation) sink);
            } else if (sink instanceof CountAggregation) {
                return new CountingMeter(level, (CountAggregation) sink);
            } else {
                return NO_OP;
            }
        } else {
            return NO_OP;
        }
    }
    
    static Meter detail(MetricsLevel level, Measurement<?> sink) {
        if (level.detailedEnabled()) {
            if (sink instanceof TimeAggregation) {
                return new SystemCurrentTimeMillisMeter(level, (TimeAggregation) sink);
            } else if (sink instanceof CountAggregation) {
                return new CountingMeter(level, (CountAggregation) sink);
            } else {
                return NO_OP;
            }
        } else {
            return NO_OP;
        }
    }

    void close();

    Meter basic(String name);

    Meter detail(String name);

    void count(String name);

    void count(String name, long count);

    default <O> Consumer<O> accept(Class<O> o) {
        return new Consumer<O>() {

            @Override
            public void accept(O t) {
                close();
            }
        };
    }

    default <O> Consumer<O> consumer(Consumer<O> delegate) {
        return new Consumer<O>() {

            @Override
            public void accept(O t) {
                close();
                delegate.accept(t);
            }
        };
    }

    default Consumer<Exception> acceptException() {
        return new Consumer<Exception>() {

            @Override
            public void accept(Exception t) {
                close();
            }
        };
    }

    static final Meter NO_OP = new Meter() {

        @Override
        public void close() {

        }

        @Override
        public Meter basic(String name) {
            return NO_OP;
        }

        @Override
        public Meter detail(String name) {
            return NO_OP;
        }

        @Override
        public void count(String name) {

        }

        @Override
        public void count(String name, long count) {

        }
    };

    public static class SystemCurrentTimeMillisMeter implements Meter {
        private final static Logger log = LogManager.getLogger(SystemCurrentTimeMillisMeter.class);

        private final MetricsLevel level;
        private final TimeAggregation sink;
        private final long start;
        private boolean active = true;

        public SystemCurrentTimeMillisMeter(MetricsLevel level, TimeAggregation sink) {
            this.level = level;
            this.sink = sink;
            this.start = System.currentTimeMillis();
        }

        @Override
        public void close() {
            long end = System.currentTimeMillis();
            active = false;
            sink.recordMs(end - start);
        }

        @Override
        public Meter basic(String name) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            return new SystemCurrentTimeMillisMeter(level, sink.getSubAggregation(name));
        }

        @Override
        public Meter detail(String name) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            if (level.detailedEnabled()) {
                return new SystemCurrentTimeMillisMeter(level, sink.getSubAggregation(name));
            } else {
                return NO_OP;
            }
        }

        @Override
        public void count(String name) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            sink.getCountAggregation(name).increment();
        }

        @Override
        public void count(String name, long count) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            sink.getCountAggregation(name).add(count);
        }
    }

    public static class CountingMeter implements Meter {
        private final static Logger log = LogManager.getLogger(CountingMeter.class);

        private final MetricsLevel level;
        private final CountAggregation sink;
        private boolean active = true;

        public CountingMeter(MetricsLevel level, CountAggregation sink) {
            this.level = level;
            this.sink = sink;
        }

        @Override
        public void close() {
            active = false;
            sink.increment();
        }

        @Override
        public Meter basic(String name) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            return new CountingMeter(level, sink.getSubCount(name));
        }

        @Override
        public Meter detail(String name) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            if (level.detailedEnabled()) {
                return new CountingMeter(level, sink.getSubCount(name));
            } else {
                return NO_OP;
            }
        }

        @Override
        public void count(String name) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            sink.getSubCount(name).increment();
        }

        @Override
        public void count(String name, long count) {
            if (!active) {
                log.error("Trying to start sub-meter for inactive meter", new Throwable());
            }

            sink.getSubCount(name).add(count);
        }
    }
}
