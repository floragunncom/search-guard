package com.floragunn.signals.confconv.es;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            ConversionResult<Schedule> conversionResult = new ScheduleConverter(watcherJson.getAsNode("trigger").getAsNode("schedule")).convertToSignals();
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

        Map<String, String> headers = !vJsonNode.hasNonNull("headers") ?
                null :
                vJsonNode.getAsDocNode("headers").keySet().stream()
                        .collect(Collectors.toMap(key -> key, key -> {
                            String headerValue = vJsonNode.get("headers").asValidatingDocNode().get(key).asString();
                            if (headerValue == null) {
                                validationErrors.add(new ValidationError("headers." + key, "Value cannot be null"));
                                return "null";
                            }
                            return headerValue;
                        }));

        ConversionResult<Auth> convertedAuth = null;

        if (vJsonNode.hasNonNull("auth")) {
            convertedAuth = createAuth(vJsonNode.getAsDocNode("auth"));
            validationErrors.add("auth", convertedAuth.getSourceValidationErrors());
        }

        Auth auth = convertedAuth != null? convertedAuth.getElement() : null;

        HttpRequestConfig result = new HttpRequestConfig(method, url, path, query, body, null, headers, auth, null);

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
