package com.floragunn.signals.execution;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.severity.SeverityMapping;

public class WatchExecutionContextData implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(WatchExecutionContextData.class);

    private NestedValueMap data;
    private NestedValueMap item;
    private SeverityMapping.EvaluationResult severity;
    private TriggerInfo triggerInfo;
    private JodaCompatibleZonedDateTime executionTime;

    public WatchExecutionContextData() {
        this.data = new NestedValueMap();
    }

    public WatchExecutionContextData(NestedValueMap data) {
        this.data = data;
    }

    public WatchExecutionContextData(NestedValueMap data, TriggerInfo triggerInfo, JodaCompatibleZonedDateTime executionTime) {
        this.data = data;
        this.triggerInfo = triggerInfo;
        this.executionTime = executionTime;
    }

    public WatchExecutionContextData(NestedValueMap data, TriggerInfo triggerInfo, JodaCompatibleZonedDateTime executionTime,
            SeverityMapping.EvaluationResult severity) {
        this.data = data;
        this.triggerInfo = triggerInfo;
        this.executionTime = executionTime;
        this.severity = severity;
    }

    public WatchExecutionContextData(NestedValueMap data, TriggerInfo triggerInfo, JodaCompatibleZonedDateTime executionTime,
            SeverityMapping.EvaluationResult severity, NestedValueMap item) {
        this.data = data;
        this.triggerInfo = triggerInfo;
        this.executionTime = executionTime;
        this.severity = severity;
        this.item = item;
    }

    public Map<String, Object> getTemplateScriptParamsAsMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("data", getData());
        result.put("item", getItem());
        result.put("severity", severity);
        result.put("trigger", triggerInfo);
        result.put("execution_time", executionTime);

        return result;
    }

    public NestedValueMap getData() {
        return data;
    }

    public void setData(NestedValueMap data) {
        this.data = data;
    }

    public SeverityMapping.EvaluationResult getSeverity() {
        return severity;
    }

    public void setSeverity(SeverityMapping.EvaluationResult severity) {
        this.severity = severity;
    }

    public TriggerInfo getTriggerInfo() {
        return triggerInfo;
    }

    public void setTriggerInfo(TriggerInfo triggerInfo) {
        this.triggerInfo = triggerInfo;
    }

    public JodaCompatibleZonedDateTime getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(JodaCompatibleZonedDateTime executionTime) {
        this.executionTime = executionTime;
    }

    public WatchExecutionContextData clone() {
        return new WatchExecutionContextData(data.clone(), triggerInfo, executionTime, severity, item);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("data", data);
        builder.field("severity", severity);
        builder.field("trigger", triggerInfo);
        builder.field("execution_time", executionTime);
        builder.endObject();
        return builder;
    }

    public static WatchExecutionContextData create(JsonNode jsonNode) {
        WatchExecutionContextData result = new WatchExecutionContextData();

        if (jsonNode.hasNonNull("data")) {
            try {
                result.data = NestedValueMap.createUnmodifieableMap(DefaultObjectMapper.readTree(jsonNode.get("data"), Map.class));
            } catch (Exception e) {
                log.error("Error while parsing " + jsonNode.get("data"), e);
            }
        }

        if (jsonNode.hasNonNull("severity")) {
            try {
                result.severity = SeverityMapping.EvaluationResult.create(jsonNode.get("severity"));
            } catch (Exception e) {
                log.error("Error while parsing " + jsonNode.get("severity"), e);
            }
        }

        if (jsonNode.hasNonNull("trigger")) {
            try {
                result.triggerInfo = TriggerInfo.create(jsonNode.get("trigger"));
            } catch (Exception e) {
                log.error("Error while parsing " + jsonNode.get("trigger"), e);
            }
        }

        if (jsonNode.hasNonNull("execution_time")) {
            try {
                result.executionTime = parseJodaCompatibleZonedDateTime(jsonNode.get("execution_time").textValue());
            } catch (Exception e) {
                log.error("Error while parsing " + jsonNode.get("execution_time"), e);
            }
        }

        return result;
    }

    public static class TriggerInfo implements ToXContentObject {
        public final JodaCompatibleZonedDateTime triggeredTime;
        public final JodaCompatibleZonedDateTime scheduledTime;
        public final JodaCompatibleZonedDateTime previousScheduledTime;
        public final JodaCompatibleZonedDateTime nextScheduledTime;

        public TriggerInfo(JodaCompatibleZonedDateTime triggeredTime, JodaCompatibleZonedDateTime scheduledTime,
                JodaCompatibleZonedDateTime previousScheduledTime, JodaCompatibleZonedDateTime nextScheduledTime) {
            this.triggeredTime = triggeredTime;
            this.scheduledTime = scheduledTime;
            this.previousScheduledTime = previousScheduledTime;
            this.nextScheduledTime = nextScheduledTime;
        }

        public TriggerInfo(Date triggeredTime, Date scheduledTime, Date previousScheduledTime, Date nextScheduledTime) {
            this.triggeredTime = new JodaCompatibleZonedDateTime(triggeredTime.toInstant(), ZoneOffset.UTC);
            this.scheduledTime = new JodaCompatibleZonedDateTime(scheduledTime.toInstant(), ZoneOffset.UTC);
            this.previousScheduledTime = new JodaCompatibleZonedDateTime(previousScheduledTime.toInstant(), ZoneOffset.UTC);
            this.nextScheduledTime = new JodaCompatibleZonedDateTime(nextScheduledTime.toInstant(), ZoneOffset.UTC);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("triggered_time",
                    triggeredTime != null ? DateTimeFormatter.ISO_ZONED_DATE_TIME.format(triggeredTime.getZonedDateTime()) : null);
            builder.field("scheduled_time",
                    scheduledTime != null ? DateTimeFormatter.ISO_ZONED_DATE_TIME.format(scheduledTime.getZonedDateTime()) : null);
            builder.field("previous_scheduled_time",
                    previousScheduledTime != null ? DateTimeFormatter.ISO_ZONED_DATE_TIME.format(previousScheduledTime.getZonedDateTime()) : null);
            builder.field("next_scheduled_time",
                    nextScheduledTime != null ? DateTimeFormatter.ISO_ZONED_DATE_TIME.format(nextScheduledTime.getZonedDateTime()) : null);
            builder.endObject();
            return builder;
        }

        public static TriggerInfo create(JsonNode jsonNode) {
            JodaCompatibleZonedDateTime triggeredTime = null;
            JodaCompatibleZonedDateTime scheduledTime = null;
            JodaCompatibleZonedDateTime previousScheduledTime = null;
            JodaCompatibleZonedDateTime nextScheduledTime = null;

            if (jsonNode.hasNonNull("triggered_time")) {
                triggeredTime = parseJodaCompatibleZonedDateTime(jsonNode.get("triggered_time").textValue());
            }
            if (jsonNode.hasNonNull("scheduled_time")) {
                scheduledTime = parseJodaCompatibleZonedDateTime(jsonNode.get("scheduled_time").textValue());
            }
            if (jsonNode.hasNonNull("previous_scheduled_time")) {
                previousScheduledTime = parseJodaCompatibleZonedDateTime(jsonNode.get("previous_scheduled_time").textValue());
            }
            if (jsonNode.hasNonNull("next_scheduled_time")) {
                nextScheduledTime = parseJodaCompatibleZonedDateTime(jsonNode.get("next_scheduled_time").textValue());
            }
            return new TriggerInfo(triggeredTime, scheduledTime, previousScheduledTime, nextScheduledTime);
        }

        public JodaCompatibleZonedDateTime getTriggeredTime() {
            return triggeredTime;
        }

        public JodaCompatibleZonedDateTime getScheduledTime() {
            return scheduledTime;
        }

        public JodaCompatibleZonedDateTime getPreviousScheduledTime() {
            return previousScheduledTime;
        }

        public JodaCompatibleZonedDateTime getNextScheduledTime() {
            return nextScheduledTime;
        }
        
        public JodaCompatibleZonedDateTime getTriggered_time() {
            return triggeredTime;
        }

        public JodaCompatibleZonedDateTime getScheduled_time() {
            return scheduledTime;
        }

        public JodaCompatibleZonedDateTime getPrevious_scheduled_time() {
            return previousScheduledTime;
        }

        public JodaCompatibleZonedDateTime getNext_scheduled_time() {
            return nextScheduledTime;
        }
    }

    static JodaCompatibleZonedDateTime parseJodaCompatibleZonedDateTime(String value) {
        ZonedDateTime dt = ZonedDateTime.parse(value);

        return new JodaCompatibleZonedDateTime(dt.toInstant(), dt.getZone());

    }

    public NestedValueMap getItem() {
        return item;
    }

    public void setItem(NestedValueMap item) {
        this.item = item;
    }
}
