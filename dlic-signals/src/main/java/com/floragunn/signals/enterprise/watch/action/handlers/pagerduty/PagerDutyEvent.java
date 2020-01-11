package com.floragunn.signals.enterprise.watch.action.handlers.pagerduty;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.signals.watch.severity.SeverityLevel;

public class PagerDutyEvent implements ToXContentObject {
    private String routingKey;
    private EventAction eventAction;
    private String dedupKey;
    private Payload payload;

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public EventAction getEventAction() {
        return eventAction;
    }

    public void setEventAction(EventAction eventAction) {
        this.eventAction = eventAction;
    }

    public String getDedupKey() {
        return dedupKey;
    }

    public void setDedupKey(String dedupKey) {
        this.dedupKey = dedupKey;
    }

    public Payload getPayload() {
        return payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    public String toJson() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("routing_key", routingKey);
        builder.field("event_action", eventAction);
        builder.field("dedup_key", dedupKey);
        builder.field("payload", payload);
        builder.endObject();

        return builder;
    }

    static class Payload implements ToXContentObject {
        private String summary;
        private String source;

        private Severity severity;
        private String component;
        private String group;
        private String eventClass;
        private Map<String, Object> customDetails;

        public String getSummary() {
            return summary;
        }

        public void setSummary(String summary) {
            this.summary = summary;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public Severity getSeverity() {
            return severity;
        }

        public void setSeverity(Severity severity) {
            this.severity = severity;
        }

        public String getComponent() {
            return component;
        }

        public void setComponent(String component) {
            this.component = component;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getEventClass() {
            return eventClass;
        }

        public void setEventClass(String eventClass) {
            this.eventClass = eventClass;
        }

        public Map<String, Object> getCustomDetails() {
            return customDetails;
        }

        public void setCustomDetails(Map<String, Object> customDetails) {
            this.customDetails = customDetails;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("summary", summary);
            builder.field("source", source);
            builder.field("severity", severity);
            builder.field("component", component);
            builder.field("group", group);
            builder.field("class", eventClass);
            builder.field("custom_details", customDetails);
            builder.endObject();
            return builder;
        }

        static enum Severity {
            INFO, WARNING, ERROR, CRITICAL;

            public String toString() {
                return name().toLowerCase();
            }

            public static Severity from(SeverityLevel severityLevel) {
                switch (severityLevel) {
                case CRITICAL:
                    return CRITICAL;
                case ERROR:
                    return ERROR;
                case INFO:
                    return INFO;
                case WARNING:
                    return WARNING;
                default:
                    return null;

                }
            }
        }

    }

    static enum EventAction {
        TRIGGER, ACKNOWLEDGE, RESOLVE;

        public String toString() {
            return name().toLowerCase();
        }
    }

}
