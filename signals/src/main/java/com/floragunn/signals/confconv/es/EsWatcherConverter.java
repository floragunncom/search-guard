/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.confconv.es;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchsupport.jobs.config.schedule.Schedule;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.action.invokers.AlertAction;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig.Method;
import com.floragunn.signals.watch.common.auth.Auth;
import com.floragunn.signals.watch.common.auth.BasicAuth;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EsWatcherConverter {

    private final DocNode watcherJson;

    public EsWatcherConverter(DocNode watcherJson) {
        this.watcherJson = watcherJson;
    }

    public ConversionResult<Watch> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();
        Schedule schedule = null;
        List<Check> checks = new ArrayList<>();
        List<AlertAction> actions = Collections.emptyList();

        if (watcherJson.hasNonNull("metadata")) {
            ConversionResult<List<Check>> conversionResult = new MetaConverter(watcherJson.getAsNode("metadata")).convertToSignals();
            checks.addAll(conversionResult.getElement());
            validationErrors.add("metadata", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("trigger") && watcherJson.getAsNode("trigger").hasNonNull("schedule")) {
            ConversionResult<Schedule> conversionResult = new ScheduleConverter(watcherJson.getAsNode("trigger").getAsNode("schedule"))
                    .convertToSignals();
            schedule = conversionResult.getElement();
            validationErrors.add("schedule", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("input")) {
            ConversionResult<List<Check>> conversionResult = new InputConverter(watcherJson.getAsNode("input")).convertToSignals();
            checks.addAll(conversionResult.getElement());
            validationErrors.add("input", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("condition")) {
            ConversionResult<List<Check>> conversionResult = new ConditionConverter(watcherJson.getAsNode("condition")).convertToSignals();
            checks.addAll(conversionResult.getElement());
            validationErrors.add("condition", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("transform")) {
            ConversionResult<List<Check>> conversionResult = new TransformConverter(watcherJson.getAsNode("transform")).convertToSignals();
            checks.addAll(conversionResult.getElement());
            validationErrors.add("transform", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("actions")) {
            ConversionResult<List<AlertAction>> conversionResult = new ActionConverter(watcherJson.getAsNode("actions")).convertToSignals();
            actions = conversionResult.getElement();
            validationErrors.add("actions", conversionResult.getSourceValidationErrors());
        }

        return new ConversionResult<Watch>(new Watch(null, schedule, checks, null, actions, null), validationErrors);
    }

    static ConversionResult<HttpRequestConfig> createHttpRequestConfig(DocNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);
        URI url = null;

        if (vJsonNode.hasNonNull("url")) {
            url = vJsonNode.get("url").required().asURI();
        } else {
            String scheme = vJsonNode.get("scheme").withDefault("http").asString();
            String host = vJsonNode.get("host").required().asString();
            int port = vJsonNode.get("port").withDefault(-1).asInt();

            try {
                url = new URI(scheme, null, host, port, null, null, null);
            } catch (URISyntaxException e) {
                // Should not happen
                validationErrors.add(new ValidationError("url", e.getMessage()).cause(e));
            }
        }

        String path = vJsonNode.get("path").asString();

        ConversionResult<String> convertedPath = new MustacheTemplateConverter(path).convertToSignals();
        validationErrors.add("path", convertedPath.getSourceValidationErrors());
        path = convertedPath.getElement();

        String query = vJsonNode.get("params").asString();

        ConversionResult<String> convertedQuery = new MustacheTemplateConverter(query).convertToSignals();
        validationErrors.add("params", convertedQuery.getSourceValidationErrors());
        query = convertedQuery.getElement();

        String body = vJsonNode.get("body").asString();

        ConversionResult<String> convertedBody = new MustacheTemplateConverter(body).convertToSignals();
        validationErrors.add("body", convertedBody.getSourceValidationErrors());
        body = convertedBody.getElement();

        Method method = vJsonNode.get("method").withDefault(Method.GET).asEnum(Method.class);

        Map<String, Object> headers = jsonNode.hasNonNull("headers") ? jsonNode.getAsNode("headers").toMap() : null;

        ConversionResult<Auth> auth = null;

        if (vJsonNode.hasNonNull("auth")) {
            auth = createAuth(vJsonNode.getAsDocNode("auth"));
            validationErrors.add("auth", auth.getSourceValidationErrors());
        }

        HttpRequestConfig result = new HttpRequestConfig(method, url, path, query, body, headers, auth.getElement(), null);

        return new ConversionResult<HttpRequestConfig>(result, validationErrors);
    }

    static ConversionResult<Auth> createAuth(DocNode jsonNode) {
        if (jsonNode.hasNonNull("basic")) {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode.getAsNode("basic"), validationErrors);

            return new ConversionResult<Auth>(new BasicAuth(vJsonNode.get("username").required().asString(), vJsonNode.get("password").asString()),
                    validationErrors);
        } else {
            return new ConversionResult<Auth>(null, new ValidationErrors().add(new ValidationError(null, "Unknown auth type")));
        }
    }
}
