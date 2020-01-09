package com.floragunn.signals.watch.state;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.util.duration.DurationExpression;
import com.floragunn.signals.watch.common.Ack;
import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.severity.SeverityLevel;

public class ActionState implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(ActionState.class);

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    private Instant lastTriggered;
    private Instant lastTriage;
    private boolean lastTriageResult;
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
        this.lastTriage = this.lastTriggered;

        if (this.lastTriageResult == true && this.acked != null) {
            return this.acked;
        } else {
            this.lastTriageResult = true;
            return null;
        }
    }

    public synchronized void afterNegativeTriage() {
        this.lastTriage = this.lastTriggered;
        this.lastTriageResult = false;
        this.acked = null;
        this.executionCount = 0;
    }

    public synchronized void ack(String user) {
        if (this.lastTriageResult == false) {
            throw new IllegalStateException(
                    "Cannot ack this action because it was not positively triaged recently. Last triage was at " + lastTriage);
        }

        this.acked = new Ack(Instant.now(), user);
    }

    public synchronized boolean ackIfPossible(String user) {
        if (this.lastTriageResult == false) {
            return false;
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
        return "ActionState [lastTriggered=" + lastTriggered + ", lastTriage=" + lastTriage + ", lastTriageResult=" + lastTriageResult + ", acked="
                + acked + ", lastExecution=" + lastExecution + "]";
    }

    @Override
    public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("last_triggered", lastTriggered != null ? DATE_FORMATTER.format(lastTriggered) : null);
        builder.field("last_triage", lastTriage != null ? DATE_FORMATTER.format(lastTriage) : null);
        builder.field("last_triage_result", lastTriageResult);
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

    public static ActionState createFrom(JsonNode jsonNode) {
        ActionState result = new ActionState();

        if (jsonNode.hasNonNull("last_triggered")) {
            result.lastTriggered = Instant.from(DATE_FORMATTER.parse(jsonNode.get("last_triggered").asText()));
        }

        if (jsonNode.hasNonNull("last_triage")) {
            result.lastTriage = Instant.from(DATE_FORMATTER.parse(jsonNode.get("last_triage").asText()));
        }

        if (jsonNode.hasNonNull("last_execution")) {
            result.lastExecution = Instant.from(DATE_FORMATTER.parse(jsonNode.get("last_execution").asText()));
        }

        if (jsonNode.hasNonNull("last_error")) {
            result.lastError = Instant.from(DATE_FORMATTER.parse(jsonNode.get("last_error").asText()));
        }

        if (jsonNode.hasNonNull("last_triage_result")) {
            result.lastTriageResult = jsonNode.get("last_triage_result").asBoolean();
        }

        if (jsonNode.hasNonNull("last_status")) {
            result.lastStatus = Status.parse(jsonNode.get("last_status"));
        }

        if (jsonNode.hasNonNull("execution_count")) {
            result.executionCount = jsonNode.get("execution_count").asInt();
        }

        if (jsonNode.hasNonNull("acked")) {
            result.acked = Ack.create(jsonNode.get("acked"));
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
