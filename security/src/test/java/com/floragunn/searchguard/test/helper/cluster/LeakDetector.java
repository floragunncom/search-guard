/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.awaitility.Awaitility;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.LeakTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

public class LeakDetector {

    private final LeakToListAppender leakToListAppender = new LeakToListAppender();

    public void start() {
        Logger leakTrackerLoggerInstance = getLeakTrackerLogger();

        if (! leakTrackerLoggerInstance.isErrorEnabled()) {
            throw new RuntimeException("LeakTracker's logger - error level is disabled");
        }
        leakToListAppender.start();

        Loggers.addAppender(leakTrackerLoggerInstance, leakToListAppender);
    }

    public void stop() {
        // LeakTracker uses java.lang.ref.Cleaner to track leaks. It seems that the garbage collector is not called before the test is finished (at least not always).
        // For this reason CleanerImpl#queue may be empty when CleanerImpl#run() method is being called. When CleanerImpl#queue is empty LeakTracker.Leak#run() method won't be called, so leaks won't be logged.
        // System#gc() method is called to increase the probability that the references will be added to CleanerImpl#queue.
        try {
            System.gc();
            Awaitility.await("List of logged leaks should be empty")
                    .failFast(
                            "List of logged leaks is not empty",
                            () -> assertThat(leakToListAppender.getLoggedLeaks(), empty())
                    )
                    .during(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(50))
                    .until(() -> true);
        } finally {
            Logger leakTrackerLoggerInstance = getLeakTrackerLogger();
            Loggers.removeAppender(leakTrackerLoggerInstance, leakToListAppender);
            leakToListAppender.stop();
        }
    }

    private Logger getLeakTrackerLogger() {
        String leakTrackerLoggerName = LeakTracker.class.getName();
        return LogManager.getLogger(leakTrackerLoggerName);
    }

    private static class LeakToListAppender extends AbstractAppender {

        private final List<String> leaksFromLogs = Collections.synchronizedList(new ArrayList<>());

        private LeakToListAppender() {
            super(
                    "leak to list appender", null, PatternLayout.newBuilder().withPattern("%m").build(),
                    true, Property.EMPTY_ARRAY
            );
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (Level.ERROR.equals(event.getLevel()) && message.contains("LEAK:")) {
                leaksFromLogs.add(message);
            }
        }

        private List<String> getLoggedLeaks() {
            return leaksFromLogs;
        }
    }
}
