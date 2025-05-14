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

package com.floragunn.signals.watch.state;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.watch.common.Ack;
import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.severity.SeverityLevel;

public class ActionState implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(ActionState.class);

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    private Instant lastTriggered;
    private Instant lastCheck;
    private boolean lastCheckResult;
    private Ack acked;
    private Instant lastExecution;
    private SeverityLevel lastSeverityLevel;
    private int executionCount = 0;
    private volatile Status lastStatus;
    private volatile Instant lastError;

    public synchronized BasicState beforeExecution(DurationExpression throttleDuration) {

        Instant now = Instant.now();
        this.lastTriggered = now;

        if (this.lastExecution == null) {
            return BasicState.EXECUTABLE;
        }

        if (throttleDuration == null) {
            return BasicState.EXECUTABLE;
        }

        Duration actualThrottleDuration = throttleDuration.getActualDuration(executionCount);

        if (log.isDebugEnabled()) {
            log.debug("Actual throttle duration after " + executionCount + " executions: " + actualThrottleDuration);
        }

        if (lastExecution.plus(actualThrottleDuration).isAfter(now)) {
            return BasicState.THROTTLED;
        } else {
            return BasicState.EXECUTABLE;
        }
    }

    public synchronized void afterSuccessfulExecution() {
        this.lastExecution = this.lastTriggered;
        this.executionCount++;
    }

    public synchronized Ack afterPositiveTriage() {
        this.lastCheck = this.lastTriggered;

        if (this.lastCheckResult == true && this.acked != null) {
            return this.acked;
        } else {
            this.lastCheckResult = true;
            return null;
        }
    }

    public synchronized void afterNegativeTriage() {
        this.lastCheck = this.lastTriggered;
        this.lastCheckResult = false;
        this.acked = null;
        this.executionCount = 0;
    }

    public synchronized void ack(String user) {
        if (this.lastCheckResult == false) {
            throw new IllegalStateException("Cannot ack this action because it was not positively triaged recently. Last triage was at " + lastCheck);
        }

        this.acked = new Ack(Instant.now(), user);
    }

    public synchronized boolean ackIfPossible(String user) {
        if (this.lastCheckResult == false) {
            return false;
        }
        if (this.acked != null) {
            return true;
        }

        this.acked = new Ack(Instant.now(), user);

        return true;

    }

    public synchronized boolean unackIfPossible(String user) {
        if (this.acked == null) {
            return false;
        }

        this.acked = null;
        return true;
    }

    public synchronized Ack getAcked() {
        return acked;
    }

    public enum BasicState {
        EXECUTABLE, THROTTLED
    }

    @Override
    public String toString() {
        return "ActionState [lastTriggered=" + lastTriggered + ", lastCheck=" + lastCheck + ", lastCheckResult=" + lastCheckResult + ", acked="
                + acked + ", lastExecution=" + lastExecution + "]";
    }

    @Override
    public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("last_triggered", lastTriggered != null ? DATE_FORMATTER.format(lastTriggered) : null);
        builder.field("last_check", lastCheck != null ? DATE_FORMATTER.format(lastCheck) : null);
        builder.field("last_check_result", lastCheckResult);
        builder.field("last_execution", lastExecution != null ? DATE_FORMATTER.format(lastExecution) : null);
        builder.field("last_error", lastError != null ? DATE_FORMATTER.format(lastError) : null);
        builder.field("last_status", lastStatus);

        if (lastSeverityLevel != null) {
            builder.field("last_execution_severity_level", lastSeverityLevel.getId());
        }

        builder.field("execution_count", executionCount);

        if (acked != null) {
            builder.field("acked", acked);
        }

        builder.endObject();
        return builder;
    }

    public static ActionState createFrom(DocNode jsonNode) {
        ActionState result = new ActionState();

        if (jsonNode.hasNonNull("last_triggered")) {
            result.lastTriggered = Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("last_triggered")));
        }

        if (jsonNode.hasNonNull("last_check")) {
            result.lastCheck = Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("last_check")));
        } else if (jsonNode.hasNonNull("last_triage")) {
            result.lastCheck = Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("last_triage")));
        }

        if (jsonNode.hasNonNull("last_execution")) {
            result.lastExecution = Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("last_execution")));
        }

        if (jsonNode.hasNonNull("last_error")) {
            result.lastError = Instant.from(DATE_FORMATTER.parse(jsonNode.getAsString("last_error")));
        }

        if (jsonNode.hasNonNull("last_check_result")) {
            try {
                result.lastCheckResult = jsonNode.getBoolean("last_check_result");
            } catch (ConfigValidationException e) {
                log.error("Error parsing " + jsonNode, e);
            }
        } else if (jsonNode.hasNonNull("last_triage_result")) {
            try {
                result.lastCheckResult = jsonNode.getBoolean("last_triage_result");
            } catch (ConfigValidationException e) {
                log.error("Error parsing " + jsonNode, e);
            }
        }

        if (jsonNode.hasNonNull("last_status")) {
            result.lastStatus = Status.parse(jsonNode.getAsNode("last_status"));
        }

        if (jsonNode.hasNonNull("execution_count")) {
            try {
                result.executionCount = jsonNode.getNumber("execution_count").intValue();
            } catch (ConfigValidationException e) {
                log.error("Error parsing " + jsonNode, e);
            }
        }

        if (jsonNode.hasNonNull("acked")) {
            result.acked = Ack.create(jsonNode.getAsNode("acked"));
        }

        return result;
    }

    public Status getLastStatus() {
        return lastStatus;
    }

    public void setLastStatus(Status lastStatus) {
        this.lastStatus = lastStatus;
    }

    public Instant getLastError() {
        return lastError;
    }

    public void setLastError(Instant lastError) {
        this.lastError = lastError;
    }
}
