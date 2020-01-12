package com.floragunn.signals.confconv.es;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.schedule.Schedule;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationError;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.JacksonTools;
import com.floragunn.signals.confconv.ConversionResult;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.action.invokers.AlertAction;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig.Method;
import com.floragunn.signals.watch.common.auth.Auth;
import com.floragunn.signals.watch.common.auth.BasicAuth;

public class EsWatcherConverter {

    
    // TODO metadata
    
    private final JsonNode watcherJson;

    public EsWatcherConverter(JsonNode watcherJson) {
        this.watcherJson = watcherJson;
    }

    public ConversionResult<Watch> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();
        Schedule schedule = null;
        List<Check> checks = new ArrayList<>();
        List<AlertAction> actions = Collections.emptyList();

        if (watcherJson.hasNonNull("trigger") && watcherJson.get("trigger").hasNonNull("schedule")) {
            ConversionResult<Schedule> conversionResult = new ScheduleConverter(watcherJson.get("trigger").get("schedule")).convertToSignals();
            schedule = conversionResult.getElement();
            validationErrors.add("schedule", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("input")) {
            ConversionResult<List<Check>> conversionResult = new InputConverter(watcherJson.get("input")).convertToSignals();
            checks.addAll(conversionResult.getElement());
            validationErrors.add("input", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("condition")) {
            ConversionResult<List<Check>> conversionResult = new ConditionConverter(watcherJson.get("condition")).convertToSignals();
            checks.addAll(conversionResult.getElement());
            validationErrors.add("condition", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("transform")) {
            ConversionResult<List<Check>> conversionResult = new TransformConverter(watcherJson.get("transform")).convertToSignals();
            checks.addAll(conversionResult.getElement());
            validationErrors.add("transform", conversionResult.getSourceValidationErrors());
        }

        if (watcherJson.hasNonNull("actions")) {
            ConversionResult<List<AlertAction>> conversionResult = new ActionConverter(watcherJson.get("actions")).convertToSignals();
            actions = conversionResult.getElement();
            validationErrors.add("actions", conversionResult.getSourceValidationErrors());
        }

        return new ConversionResult<Watch>(new Watch(null, schedule, checks, null, actions, null), validationErrors);
    }

    static ConversionResult<HttpRequestConfig> createHttpRequestConfig(JsonNode jsonNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);
        URI url = null;

        if (vJsonNode.hasNonNull("url")) {
            url = vJsonNode.requiredURI("url");
        } else {
            String scheme = vJsonNode.string("scheme", "http");
            String host = vJsonNode.requiredString("host");
            int port = vJsonNode.intNumber("port", -1);

            try {
                url = new URI(scheme, null, host, port, null, null, null);
            } catch (URISyntaxException e) {
                // Should not happen
                validationErrors.add(new ValidationError("url", e.getMessage()).cause(e));
            }
        }

        String path = vJsonNode.string("path");
        String query = vJsonNode.string("params");
        String body = vJsonNode.string("body");
        Method method = vJsonNode.caseInsensitiveEnum("method", Method.class, Method.GET);

        Map<String, Object> headers = vJsonNode.hasNonNull("headers") ? JacksonTools.toMap(vJsonNode.get("headers")) : null;

        ConversionResult<Auth> auth = null;

        if (vJsonNode.hasNonNull("auth")) {
            auth = createAuth(vJsonNode.get("auth"));
            validationErrors.add("auth", auth.getSourceValidationErrors());
        }

        HttpRequestConfig result = new HttpRequestConfig(method, url, path, query, body, headers, auth.getElement(), null);

        return new ConversionResult<HttpRequestConfig>(result, validationErrors);
    }

    static ConversionResult<Auth> createAuth(JsonNode jsonNode) {
        if (jsonNode.hasNonNull("basic")) {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode.get("basic"), validationErrors);

            return new ConversionResult<Auth>(new BasicAuth(vJsonNode.requiredString("username"), vJsonNode.string("password")), validationErrors);
        } else {
            return new ConversionResult<Auth>(null, new ValidationErrors().add(new ValidationError(null, "Unknown auth type")));
        }
    }
}
