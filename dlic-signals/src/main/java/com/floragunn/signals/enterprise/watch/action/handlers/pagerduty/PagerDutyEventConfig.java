package com.floragunn.signals.enterprise.watch.action.handlers.pagerduty;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.script.ScriptException;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchsupport.config.elements.InlineMustacheTemplate;
import com.floragunn.searchsupport.config.validation.ScriptExecutionError;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.support.InlinePainlessScript;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class PagerDutyEventConfig implements ToXContent {

    private InlineMustacheTemplate<PagerDutyEvent.EventAction> eventAction;
    private InlineMustacheTemplate<String> dedupKey;
    private Payload payload;

    PagerDutyEvent render(WatchExecutionContext ctx, PagerDutyAccount account) throws ActionExecutionException {
        ValidationErrors validationErrors = new ValidationErrors();

        PagerDutyEvent event = new PagerDutyEvent();

        event.setRoutingKey(account.getIntegrationKey());

        if (eventAction != null) {
            event.setEventAction(eventAction.get(ctx.getTemplateScriptParamsAsMap(), "event_action", validationErrors));
        }

        if (dedupKey != null) {
            event.setDedupKey(dedupKey.get(ctx.getTemplateScriptParamsAsMap(), "dedup_key", validationErrors));
        }

        if (payload != null) {
            try {
                event.setPayload(payload.render(ctx));
            } catch (ConfigValidationException e) {
                validationErrors.add("payload", e);
            }
        }

        if (validationErrors.hasErrors()) {
            throw new ActionExecutionException(null, "Error while rendering PagerDuty event", validationErrors);
        }

        return event;

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("event_action", eventAction);
        builder.field("dedup_key", dedupKey);
        builder.field("payload", payload);
        builder.endObject();

        return builder;
    }

    static PagerDutyEventConfig create(WatchInitializationService watchInitializationService, JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors).with(watchInitializationService.getScriptService());

        PagerDutyEventConfig result = new PagerDutyEventConfig();

        result.eventAction = vJsonNode.template("event_action", PagerDutyEvent.EventAction.class);
        result.dedupKey = vJsonNode.template("dedup_key");

        if (vJsonNode.hasNonNull("payload")) {
            try {
                result.payload = Payload.create(watchInitializationService, vJsonNode.get("payload"));
            } catch (ConfigValidationException e) {
                validationErrors.add("payload", e);
            }
        } else {
            validationErrors.add(new MissingAttribute("payload", jsonNode));
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static class Payload implements ToXContent {

        private InlineMustacheTemplate<String> summary;
        private InlineMustacheTemplate<String> source;
        private InlineMustacheTemplate<PagerDutyEvent.Payload.Severity> severity;
        private InlineMustacheTemplate<String> component;
        private InlineMustacheTemplate<String> group;
        private InlineMustacheTemplate<String> eventClass;
        private InlinePainlessScript<SignalsObjectFunctionScript.Factory> customDetails;

        PagerDutyEvent.Payload render(WatchExecutionContext ctx) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();

            PagerDutyEvent.Payload payload = new PagerDutyEvent.Payload();

            if (summary != null) {
                payload.setSummary(summary.get(ctx.getTemplateScriptParamsAsMap(), "summary", validationErrors));
            }

            if (source != null) {
                payload.setSource(source.get(ctx.getTemplateScriptParamsAsMap(), "source", validationErrors));
            }

            if (severity != null) {
                payload.setSeverity(severity.get(ctx.getTemplateScriptParamsAsMap(), "severity", validationErrors));
            }

            if (component != null) {
                payload.setComponent(component.get(ctx.getTemplateScriptParamsAsMap(), "component", validationErrors));
            }

            if (group != null) {
                payload.setGroup(group.get(ctx.getTemplateScriptParamsAsMap(), "group", validationErrors));
            }

            if (eventClass != null) {
                payload.setEventClass(eventClass.get(ctx.getTemplateScriptParamsAsMap(), "class", validationErrors));
            }

            if (customDetails != null && customDetails.getScriptFactory() != null) {
                try {
                    Object details = customDetails.getScriptFactory().newInstance(Collections.emptyMap(), ctx).execute();

                    if (details instanceof Map) {
                        payload.setCustomDetails(NestedValueMap.copy((Map<?, ?>) details));
                    } else {
                        HashMap<String, Object> map = new HashMap<>();
                        map.put("_value", details);
                        payload.setCustomDetails(map);
                    }
                } catch (ScriptException e) {
                    validationErrors.add(new ScriptExecutionError("custom_details", e));
                }
            }

            validationErrors.throwExceptionForPresentErrors();

            return payload;
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

        static Payload create(WatchInitializationService watchInitializationService, JsonNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors).with(watchInitializationService.getScriptService());

            Payload result = new Payload();

            result.summary = vJsonNode.requiredTemplate("summary");
            result.source = vJsonNode.requiredTemplate("source");
            result.severity = vJsonNode.template("severity", PagerDutyEvent.Payload.Severity.class);
            result.component = vJsonNode.template("component");
            result.group = vJsonNode.template("group");
            result.eventClass = vJsonNode.template("class");
            result.customDetails = vJsonNode.value("custom_details", new InlinePainlessScript.Parser<SignalsObjectFunctionScript.Factory>(
                    SignalsObjectFunctionScript.CONTEXT, watchInitializationService), null);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        public InlinePainlessScript<SignalsObjectFunctionScript.Factory> getCustomDetails() {
            return customDetails;
        }

        public void setCustomDetails(InlinePainlessScript<SignalsObjectFunctionScript.Factory> customDetails) {
            this.customDetails = customDetails;
        }

        public InlineMustacheTemplate<String> getSummary() {
            return summary;
        }

        public void setSummary(InlineMustacheTemplate<String> summary) {
            this.summary = summary;
        }

        public InlineMustacheTemplate<String> getSource() {
            return source;
        }

        public void setSource(InlineMustacheTemplate<String> source) {
            this.source = source;
        }

        public InlineMustacheTemplate<PagerDutyEvent.Payload.Severity> getSeverity() {
            return severity;
        }

        public void setSeverity(InlineMustacheTemplate<PagerDutyEvent.Payload.Severity> severity) {
            this.severity = severity;
        }

        public InlineMustacheTemplate<String> getComponent() {
            return component;
        }

        public void setComponent(InlineMustacheTemplate<String> component) {
            this.component = component;
        }

        public InlineMustacheTemplate<String> getGroup() {
            return group;
        }

        public void setGroup(InlineMustacheTemplate<String> group) {
            this.group = group;
        }

        public InlineMustacheTemplate<String> getEventClass() {
            return eventClass;
        }

        public void setEventClass(InlineMustacheTemplate<String> eventClass) {
            this.eventClass = eventClass;
        }

    }

    public Payload getPayload() {
        return payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    public InlineMustacheTemplate<PagerDutyEvent.EventAction> getEventAction() {
        return eventAction;
    }

    public void setEventAction(InlineMustacheTemplate<PagerDutyEvent.EventAction> eventAction) {
        this.eventAction = eventAction;
    }

    public InlineMustacheTemplate<String> getDedupKey() {
        return dedupKey;
    }

    public void setDedupKey(InlineMustacheTemplate<String> dedupKey) {
        this.dedupKey = dedupKey;
    }

}
