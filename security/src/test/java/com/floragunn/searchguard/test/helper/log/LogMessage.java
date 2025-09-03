/*
 * Based on https://github.com/opensearch-project/security/blob/3.2.0.0/src/integrationTest/java/org/opensearch/test/framework/log/LogMessage.java
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

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Objects;
import java.util.Optional;

class LogMessage {

    private final String message;
    private final String stackTrace;

    public LogMessage(String message, Throwable throwable) {
        this.message = message;
        this.stackTrace = Optional.ofNullable(throwable).map(ExceptionUtils::getStackTrace).orElse("");
    }

    public boolean containMessage(String expectedMessage) {
        Objects.requireNonNull(expectedMessage, "Expected message must not be null.");
        return expectedMessage.equals(message);
    }

    public boolean stackTraceContains(String stackTraceFragment) {
        Objects.requireNonNull(stackTraceFragment, "Stack trace fragment is required.");
        return stackTrace.contains(stackTraceFragment);
    }

    public String getMessage() {
        return message;
    }
}
