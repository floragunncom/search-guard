package com.floragunn.signals.watch.action.handlers;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionException;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.HttpUtils;
import com.floragunn.signals.watch.common.WatchElement;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.google.common.collect.Iterables;

public class WebhookAction extends ActionHandler {
    private static final Logger log = LogManager.getLogger(WebhookAction.class);

    public static final String TYPE = "webhook";

    private final HttpRequestConfig requestConfig;
    private final HttpClientConfig httpClientConfig;

    public WebhookAction(HttpRequestConfig request, HttpClientConfig httpClientConfig) {
        this.requestConfig = request;
        this.httpClientConfig = httpClientConfig;
    }

    @Override
    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {

        try (CloseableHttpClient httpClient = httpClientConfig.createHttpClient(ctx.getHttpProxyConfig())) {
            HttpUriRequest request = requestConfig.createHttpRequest(ctx);

            if (log.isDebugEnabled()) {
                log.debug("Going to execute: " + request);
            }

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {

                CloseableHttpResponse response = AccessController
                        .doPrivileged((PrivilegedExceptionAction<CloseableHttpResponse>) () -> httpClient.execute(request));

                if (response.getStatusLine().getStatusCode() >= 400) {
                    throw new WatchExecutionException(
                            "Web hook returned error: " + response.getStatusLine() + "\n\n" + HttpUtils.getEntityAsDebugString(response), null);
                }
            }

            return new ActionExecutionResult(HttpUtils.getRequestAsDebugString(request));
        } catch (PrivilegedActionException e) {
            throw new ActionExecutionException(this, e.getCause());
        } catch (Exception e) {
            throw new ActionExecutionException(this, e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Iterable<? extends WatchElement> getChildren() {
        return Iterables.concat(super.getChildren(), Collections.singletonList(this.requestConfig));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("request");
        requestConfig.toXContent(builder, params);

        httpClientConfig.toXContent(builder, params);

        return builder;
    }

    public static class Factory extends ActionHandler.Factory<WebhookAction> {
        public Factory() {
            super(WebhookAction.TYPE);
        }

        @Override
        protected WebhookAction create(WatchInitializationService watchInitService, ValidatingDocNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {
            HttpClientConfig httpClientConfig = null;
            HttpRequestConfig request = null;

            if (vJsonNode.hasNonNull("request")) {
                try {
                    request = HttpRequestConfig.create(watchInitService, vJsonNode.getDocumentNode().getAsNode("request"));
                } catch (ConfigValidationException e) {
                    validationErrors.add("request", e);
                }
            } else {
                validationErrors.add(new MissingAttribute("request", vJsonNode));
            }

            try {
                httpClientConfig = HttpClientConfig.create(vJsonNode);
            } catch (ConfigValidationException e) {
                validationErrors.add(null, e);
            }
            //  vJsonNode.validateUnusedAttributes();

            validationErrors.throwExceptionForPresentErrors();

            return new WebhookAction(request, httpClientConfig);
        }
    }

}
