package com.floragunn.signals.watch.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.validation.errors.ValidationError;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionException;
import com.floragunn.signals.watch.common.HttpEndpointWhitelist.NotWhitelistedException;
import com.floragunn.signals.watch.common.auth.Auth;
import com.floragunn.signals.watch.common.auth.BasicAuth;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class HttpRequestConfig extends WatchElement implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(HttpRequestConfig.class);
    private static final Configuration jsonPathConfig = BasicJsonPathDefaultConfiguration.defaultConfiguration();

    private Method method;
    private String accept;
    private URI uri;
    private String path;
    private String queryParams;
    private String body;
    private JsonPath jsonBodyFrom;
    private Map<String, String> headers;
    private Auth auth;
    private TemplateScript.Factory pathTemplateScriptFactory;
    private TemplateScript.Factory queryParamsTemplateScriptFactory;
    private TemplateScript.Factory bodyTemplateScriptFactory;
    private Map<String, TemplateScript.Factory> headerTemplateScriptFactories;

    public HttpRequestConfig(Method method, URI uri, String path, String queryParams, String body, JsonPath jsonBodyFrom, Map<String, String> headers, Auth auth,
                             String accept) {
        super();
        if (body != null && jsonBodyFrom != null) {
            throw new IllegalStateException("body and jsonBodyFrom fields are mutually exclusive");
        }
        this.method = method;
        this.uri = uri;
        this.path = path;
        this.queryParams = queryParams;
        this.body = body;
        this.jsonBodyFrom = jsonBodyFrom;
        this.headers = headers == null ? new HashMap<>() : headers;
        this.auth = auth;
        this.accept = accept;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        if (this.jsonBodyFrom != null) {
            throw new IllegalStateException("body and jsonBodyFrom cannot be populated at the same time");
        }
        this.body = body;
    }

    public JsonPath getJsonBodyFrom() {
        return jsonBodyFrom;
    }

    public void setJsonBodyFrom(JsonPath jsonBodyFrom) {
        if (this.body != null) {
            throw new IllegalStateException("body and jsonBodyFrom cannot be populated at the same time");
        }
        this.jsonBodyFrom = jsonBodyFrom;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers == null ? new HashMap<>() : headers;
    }

    public void compileScripts(WatchInitializationService watchInitializationService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.bodyTemplateScriptFactory = watchInitializationService.compileTemplate("body", this.body, validationErrors);
        this.pathTemplateScriptFactory = watchInitializationService.compileTemplate("path", this.path, validationErrors);
        this.queryParamsTemplateScriptFactory = watchInitializationService.compileTemplate("query_params", this.queryParams, validationErrors);
        this.headerTemplateScriptFactories = getHeaders()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> watchInitializationService.compileTemplate("headers." + entry.getKey(), entry.getValue(), validationErrors)));

        validationErrors.throwExceptionForPresentErrors();
    }

    public HttpUriRequest createHttpRequest(WatchExecutionContext ctx)
            throws UnsupportedEncodingException, WatchExecutionException, NotWhitelistedException {

        URI uri = getRenderedUri(ctx);

        checkWhitelist(ctx, uri);

        RequestBuilder httpRequestBuilder = createHttpRequestBuilder(uri, method);
        Map<String, String> renderedHeaders = getRenderedHeaders(ctx);

        for (Map.Entry<String, String> header : renderedHeaders.entrySet()) {
            httpRequestBuilder.setHeader(header.getKey(), header.getValue());
        }

        if (auth instanceof BasicAuth) {
            BasicAuth basicAuth = (BasicAuth) auth;
            String encodedAuth = Base64.getEncoder().encodeToString((basicAuth.getUsername() + ":" + basicAuth.getPassword()).getBytes());

            httpRequestBuilder.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
        }

        String body = prepareBody(ctx);
        httpRequestBuilder.setEntity(new StringEntity(body));
        HttpUriRequest request = httpRequestBuilder.build();

        if (log.isDebugEnabled()) {
            log.debug("Rendered HTTP request:\n" + request + "\n" + body);
        }

        return request;
    }

    // TODO maybe move this to a dedicated http client
    public void checkHttpResponse(HttpRequest request, HttpResponse response) throws WatchExecutionException {
        if (response.getEntity() == null || response.getEntity().getContentType() == null
                || response.getEntity().getContentType().getValue() == null) {
            return;
        }

        String receivedContentType = response.getEntity().getContentType().getValue();
        String accept = this.accept;

        if (accept == null && this.headers.containsKey(HttpHeaders.ACCEPT)) {
            accept = this.headers.get(HttpHeaders.ACCEPT);
        }

        if (accept != null) {
            String[] acceptedContentTypes = accept.split(",\\s*");
            boolean acceptedContentTypeFound = false;

            for (String acceptedContentType : acceptedContentTypes) {
                if (acceptedContentType.contains(";")) {
                    acceptedContentType = acceptedContentType.substring(0, acceptedContentType.indexOf(';'));
                }

                if (receivedContentType.equalsIgnoreCase(acceptedContentType)) {
                    acceptedContentTypeFound = true;
                }
            }

            if (!acceptedContentTypeFound) {
                throw new WatchExecutionException("Web service at " + request.getRequestLine().getUri() + " returned the Content-Type "
                        + receivedContentType + "; The content types configured to be accepted are: " + accept, null);
            }
        }
    }

    private void checkWhitelist(WatchExecutionContext ctx, URI uri) throws NotWhitelistedException {
        if (ctx.getHttpEndpointWhitelist() != null) {
            ctx.getHttpEndpointWhitelist().check(uri);
        }
    }

    private RequestBuilder createHttpRequestBuilder(URI url, Method method) throws WatchExecutionException {
        switch (method) {
            case POST:
                return RequestBuilder.post(url);
            case PUT:
                return RequestBuilder.put(url);
            case DELETE:
                return RequestBuilder.delete(url);
            case GET:
                return RequestBuilder.get(url);
            default:
                throw new WatchExecutionException("Unsupported request method " + method, null);
        }
    }

    private URI getRenderedUri(WatchExecutionContext ctx) throws WatchExecutionException {
        try {
            if (this.path == null && this.queryParams == null) {
                return this.uri;
            }

            String renderedPath = this.uri.getPath();
            String renderedQueryParams = this.uri.getQuery();

            Map<String, Object> runtimeData = ctx.getTemplateScriptParamsAsMap();

            if (this.pathTemplateScriptFactory != null) {
                renderedPath = this.pathTemplateScriptFactory.newInstance(runtimeData).execute();
            }

            if (this.queryParamsTemplateScriptFactory != null) {
                renderedQueryParams = this.queryParamsTemplateScriptFactory.newInstance(runtimeData).execute();
            }

            return new URI(this.uri.getScheme(), this.uri.getAuthority(), renderedPath, renderedQueryParams, this.uri.getFragment());
        } catch (URISyntaxException e) {
            throw new WatchExecutionException(e.getMessage(), null);
        }
    }

    private String prepareBody(WatchExecutionContext ctx) throws WatchExecutionException {
        if (this.body != null) {
            return getRenderedBody(ctx);
        } else if (this.jsonBodyFrom != null) {
            return getBodyByJsonPath(ctx);
        } else {
            return "";
        }
    }

    private String getRenderedBody(WatchExecutionContext ctx) {
        Map<String, Object> runtimeData = ctx.getTemplateScriptParamsAsMap();

        return this.bodyTemplateScriptFactory.newInstance(runtimeData).execute();
    }

    private String getBodyByJsonPath(WatchExecutionContext ctx) throws WatchExecutionException {
        Map<String, Object> runtimeData = ctx.getTemplateScriptParamsAsMap();

        try {
            Object runtimeDataObject = jsonBodyFrom.read(runtimeData, jsonPathConfig);
            return jsonPathConfig.jsonProvider().toJson(runtimeDataObject);
        } catch (Exception e) {
            throw new WatchExecutionException("Failed to read body from runtime data using JSON Path: " + jsonBodyFrom.getPath(), e, null);
        }
    }

    private Map<String, String> getRenderedHeaders(WatchExecutionContext ctx) {
        Map<String, Object> runtimeData = ctx.getTemplateScriptParamsAsMap();

        return this.headerTemplateScriptFactories.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().newInstance(runtimeData).execute()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("method", method);

        if (uri != null) {
            builder.field("url", String.valueOf(uri));
        }

        builder.field("body", body);

        if (jsonBodyFrom != null) {
            builder.field("json_body_from", jsonBodyFrom.getPath());
        }

        if (!headers.isEmpty()) {
            builder.field("headers", headers);
        }

        if (auth != null) {
            builder.field("auth");
            auth.toXContent(builder, params);
        }

        if (path != null) {
            builder.field("path", path);
        }

        if (queryParams != null) {
            builder.field("query_params", queryParams);
        }

        if (accept != null) {
            builder.field("accept", accept);
        }

        builder.endObject();

        return builder;
    }

    public static HttpRequestConfig create(WatchInitializationService watchInitService, DocNode objectNode)
            throws ConfigValidationException {
        HttpRequestConfig result = createWithoutCompilation(objectNode);

        result.compileScripts(watchInitService);

        return result;

    }

    public static HttpRequestConfig createWithoutCompilation(DocNode objectNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(objectNode, validationErrors);

        Method method = vJsonNode.get("method").withDefault(Method.POST).asEnum(Method.class);
        URI uri = vJsonNode.get("url").required().asURI();
        String body = vJsonNode.get("body").asString();
        JsonPath jsonBodyFrom = vJsonNode.get("json_body_from").asJsonPath();
        String path = vJsonNode.get("path").asString();
        String queryParams = vJsonNode.get("query_params").asString();
        String accept = vJsonNode.get("accept").asString();
        Map<String, String> headers = extractHeadersFromDocNode(vJsonNode, validationErrors);
        Auth auth = vJsonNode.get("auth").by(Auth::create);

        if (vJsonNode.hasNonNull("json_body_from")) {
            if (vJsonNode.hasNonNull("body")) {
                Stream.of("body", "json_body_from").forEach(attribute -> validationErrors.add(new ValidationError(
                        attribute, "Both body and json_body_from are set. These are mutually exclusive."
                )));
            }

            String actualContentType = headers.get(HttpHeaders.CONTENT_TYPE);
            if (actualContentType != null && !ContentType.APPLICATION_JSON.getMimeType().equalsIgnoreCase(actualContentType)) {
                validationErrors.add(new ValidationError(
                        "headers." + HttpHeaders.CONTENT_TYPE,
                        String.format("Content type header should be set to %s when json_body_from is used.", ContentType.APPLICATION_JSON.getMimeType())
                ));
            } else {
                headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
            }
        }

        vJsonNode.checkForUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        return new HttpRequestConfig(method, uri, path, queryParams, body, jsonBodyFrom, headers, auth, accept);
    }

    private static Map<String, String> extractHeadersFromDocNode(ValidatingDocNode vJsonNode, ValidationErrors validationErrors) {
        if (!vJsonNode.hasNonNull("headers")) {
            return new HashMap<>();
        }
        return vJsonNode.getAsDocNode("headers").keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> {
                    String headerValue = vJsonNode.get("headers").asValidatingDocNode().get(key).asString();
                    if (headerValue == null) {
                        validationErrors.add(new ValidationError("headers." + key, "Value cannot be null"));
                        return "null";
                    }
                    return headerValue;
                }));
    }

    public Auth getAuth() {
        return auth;
    }

    public void setAuth(Auth auth) {
        this.auth = auth;
    }

    public enum Method {
        POST, PUT, DELETE, GET
    }
}
