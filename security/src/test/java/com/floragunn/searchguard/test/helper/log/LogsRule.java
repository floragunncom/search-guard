/*
 * Based on https://github.com/opensearch-project/security/blob/3.2.0.0/src/integrationTest/java/org/opensearch/test/framework/log/LogsRule.java
 * from Apache 2 licensed OpenSearch 3.2.0.0.
 *
 * Original license header:
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 *
 *
 *
 * Modifications:
 *
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

package com.floragunn.searchguard.test.helper.log;

import org.junit.rules.ExternalResource;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

/**
* The class is a JUnit 4 rule and enables developers to write assertion related to log messages generated in the course of test. To use
* {@link LogsRule} appender  {@link LogCapturingAppender} must be properly configured. The rule also manages {@link LogCapturingAppender}
* so that memory occupied by gathered log messages is released after each test.
*/
public class LogsRule extends ExternalResource {

    private final String[] loggerNames;

    /**
    * Constructor used to start gathering log messages from certain loggers
    * @param loggerNames Loggers names. Log messages are collected only if the log message is associated with the logger with a name which
    *                    is present in <code>loggerNames</code> parameter.
    */
    public LogsRule(String... loggerNames) {
        this.loggerNames = Objects.requireNonNull(loggerNames, "Logger names are required");
    }

    @Override
    protected void before() {
        LogCapturingAppender.enable(loggerNames);
    }

    @Override
    protected void after() {
        LogCapturingAppender.disable();
    }

    /**
    * Check if during the tests certain log message was logged
    * @param expectedLogMessage expected log message
    */
    public void assertThatContainExactly(String expectedLogMessage) {
        List<String> messages = LogCapturingAppender.getLogMessagesAsString();
        String reason = reasonMessage(expectedLogMessage, messages);
        assertThat(reason, messages, hasItem(expectedLogMessage));
    }

    /**
    * Check if during the tests certain log message was logged
    * @param messageFragment expected log message fragment
    */
    public void assertThatContain(String messageFragment) {
        List<String> messages = LogCapturingAppender.getLogMessagesAsString();

        String reason = reasonMessage(messageFragment, messages);
        assertThat(reason, messages, hasItem(containsString(messageFragment)));
    }

    /**
    * Check if during the tests a stack trace was logged which contain given fragment
    * @param stackTraceFragment stack trace fragment
    */
    public void assertThatStackTraceContain(String stackTraceFragment) {
        long count = LogCapturingAppender.getLogMessages()
            .stream()
            .filter(logMessage -> logMessage.stackTraceContains(stackTraceFragment))
            .count();
        String reason = "Stack trace does not contain element " + stackTraceFragment;
        assertThat(reason, count, greaterThan(0L));
    }

    private static String reasonMessage(String expectedLogMessage, List<String> messages) {
        String concatenatedLogMessages = messages.stream().map(message -> String.format("'%s'", message)).collect(Collectors.joining(", "));
        return String.format(
            "Expected message '%s' has not been found in logs. All captured log messages: %s",
            expectedLogMessage,
            concatenatedLogMessages
        );
    }
}
