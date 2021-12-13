package com.floragunn.signals.watch.action.invokers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.elasticsearch.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.temporal.DurationExpression;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.support.InlinePainlessScript;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.severity.SeverityLevel;
import com.floragunn.signals.watch.severity.SeverityMapping;

public class AlertAction extends ActionInvoker {
    protected final DurationExpression throttlePeriod;
    protected final SeverityLevel.Set severityLevels;

    public AlertAction(String name, ActionHandler handler, DurationExpression throttlePeriod, SeverityLevel.Set severityLevels, List<Check> checks,
            InlinePainlessScript<SignalsObjectFunctionScript.Factory> foreach, Integer foreachLimit) {
        super(name, handler, checks, foreach, foreachLimit);
        this.throttlePeriod = throttlePeriod;
        this.severityLevels = severityLevels;
    }

    public DurationExpression getThrottlePeriod() {
        return throttlePeriod;
    }

    public SeverityLevel.Set getSeverityLevels() {
        return severityLevels;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", handler.getType());

        if (name != null) {
            builder.field("name", name);
        }

        if (severityLevels != null) {
            builder.field("severity", severityLevels);
        }

        if (throttlePeriod != null) {
            builder.field("throttle_period", throttlePeriod.toString());
        }

        if (foreach != null) {
            builder.field("foreach", foreach);
        }

        if (foreachLimit != 100) {
            builder.field("foreach_limit", foreachLimit);
        }

        if (checks != null && checks.size() > 0) {
            builder.field("checks").startArray();

            for (Check check : checks) {
                check.toXContent(builder, params);
            }

            builder.endArray();
        }

        handler.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    public static AlertAction create(WatchInitializationService watchInitService, ObjectNode jsonObject, SeverityMapping severityMapping)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonObject, validationErrors);

        String name = vJsonNode.requiredString("name");
        List<Check> checks = createNestedChecks(watchInitService, vJsonNode, validationErrors);
        DurationExpression throttlePeriod = vJsonNode.durationExpression("throttle_period");
        SeverityLevel.Set severityLevels = null;
        ActionHandler handler = null;
        Integer foreachLimit = null;

        try {
            severityLevels = SeverityLevel.Set.createWithNoneDisallowed(vJsonNode.get("severity"));

            if (severityLevels != null) {
                validateSeverityLevelsAgainstSeverityMapping(severityLevels, severityMapping);
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("severity", e);
        }

        try {
            handler = ActionHandler.create(watchInitService, vJsonNode);
        } catch (ConfigValidationException e) {
            validationErrors.add(null, e);
        }

        InlinePainlessScript<SignalsObjectFunctionScript.Factory> foreach = vJsonNode.value("foreach",
                new InlinePainlessScript.Parser<SignalsObjectFunctionScript.Factory>(SignalsObjectFunctionScript.CONTEXT, watchInitService), null);

        foreachLimit = vJsonNode.intNumber("foreach_limit", null);

        vJsonNode.validateUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        return new AlertAction(name, handler, throttlePeriod, severityLevels, checks, foreach, foreachLimit);

    }

    public static List<AlertAction> createFromArray(WatchInitializationService ctx, ArrayNode arrayNode, SeverityMapping severityMapping)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        ArrayList<AlertAction> result = new ArrayList<>(arrayNode.size());

        for (JsonNode member : arrayNode) {
            if (member instanceof ObjectNode) {
                try {
                    result.add(create(ctx, (ObjectNode) member, severityMapping));
                } catch (ConfigValidationException e) {
                    validationErrors.add(member.hasNonNull("name") ? "[" + member.get("name").asText() + "]" : "[]", e);
                }
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    private static void validateSeverityLevelsAgainstSeverityMapping(SeverityLevel.Set severityLevels, SeverityMapping severityMapping)
            throws ConfigValidationException {
        if (severityMapping == null) {
            throw new ConfigValidationException(new ValidationError(null, "Severity can only be used in actions with a defined severity mapping"));
        }

        Set<SeverityLevel> definedLevels = severityMapping.getDefinedLevels();

        if (!severityLevels.isSubsetOf(definedLevels)) {
            throw new ConfigValidationException(new ValidationError(null,
                    "Uses a severity which is not defined by severity mapping: " + severityLevels.missingFromOther(definedLevels)));

        }

    }

}
