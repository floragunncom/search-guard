/*
 * Copyright 2019-2022 floragunn GmbH
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

package com.floragunn.signals.watch.action.invokers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
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
    protected final boolean ackEnabled;

    public AlertAction(String name, ActionHandler handler, DurationExpression throttlePeriod, SeverityLevel.Set severityLevels, List<Check> checks,
            InlinePainlessScript<SignalsObjectFunctionScript.Factory> foreach, Integer foreachLimit, boolean ackEnabled) {
        super(name, handler, checks, foreach, foreachLimit);
        this.throttlePeriod = throttlePeriod;
        this.severityLevels = severityLevels;
        this.ackEnabled = ackEnabled;
    }

    public DurationExpression getThrottlePeriod() {
        return throttlePeriod;
    }

    public SeverityLevel.Set getSeverityLevels() {
        return severityLevels;
    }

    public boolean isAckEnabled() {
        return ackEnabled;
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

        if (!ackEnabled) {
            builder.field("ack_enabled", false);            
        }
        
        handler.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    public static AlertAction create(WatchInitializationService watchInitService, DocNode jsonObject, SeverityMapping severityMapping)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);
        ThrottlePeriodParser throttlePeriodParser = watchInitService.getThrottlePeriodParser();

        String name = vJsonNode.get("name").required().asString();
        List<Check> checks = createNestedChecks(watchInitService, vJsonNode, validationErrors);
        DurationExpression throttlePeriod = vJsonNode.get("throttle_period")
                .withDefault(throttlePeriodParser.getDefaultThrottle())
                .byString(throttlePeriodParser::parseThrottle);
        SeverityLevel.Set severityLevels = null;
        ActionHandler handler = null;
        Integer foreachLimit = null;
        boolean ackEnabled = true;

        try {
            severityLevels = SeverityLevel.Set.createWithNoneDisallowed(vJsonNode.get("severity").asAnything());

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

        InlinePainlessScript<SignalsObjectFunctionScript.Factory> foreach = vJsonNode.get("foreach")
                .byString((s) -> InlinePainlessScript.parse(s, SignalsObjectFunctionScript.CONTEXT, watchInitService));

        foreachLimit = vJsonNode.get("foreach_limit").asInteger();        
        ackEnabled = vJsonNode.get("ack_enabled").withDefault(true).asBoolean();

        vJsonNode.checkForUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        return new AlertAction(name, handler, throttlePeriod, severityLevels, checks, foreach, foreachLimit, ackEnabled);

    }

    public static List<AlertAction> createFromArray(WatchInitializationService ctx, List<DocNode> arrayNode, SeverityMapping severityMapping)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        ArrayList<AlertAction> result = new ArrayList<>(arrayNode.size());

        for (DocNode member : arrayNode) {
            if (member.isMap()) {
                try {
                    result.add(create(ctx, member, severityMapping));
                } catch (ConfigValidationException e) {
                    validationErrors.add(member.hasNonNull("name") ? "[" + member.get("name") + "]" : "[]", e);
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
