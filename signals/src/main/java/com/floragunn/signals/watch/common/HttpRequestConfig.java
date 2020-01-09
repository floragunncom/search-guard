package com.floragunn.signals.watch.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.TemplateScript;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.JacksonTools;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionException;
import com.floragunn.signals.watch.common.HttpEndpointWhitelist.NotWhitelistedException;
import com.floragunn.signals.watch.common.auth.Auth;
import com.floragunn.signals.watch.common.auth.BasicAuth;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class HttpRequestConfig extends WatchElement implements ToXContentObject {
    private static final Logger log = LogManager.getLogger(HttpRequestConfig.class);

    private Method method;
    private String accept;
    private URI uri;
    private String path;
    private String queryParams;
    private String body;
    private Map<String, Object> headers;
    private Auth auth;
    private TemplateScript.Factory pathTemplateScriptFactory;
    private TemplateScript.Factory queryParamsTemplateScriptFactory;
    private TemplateScript.Factory bodyTemplateScriptFactory;

    public HttpRequestConfig(Method method, URI uri, String path, String queryParams, String body, Map<String, Object> headers, Auth auth,
            String accept) {
        super();
        this.method = method;
        this.uri = uri;
        this.path = path;
        this.queryParams = queryParams;
        this.body = body;
        this.headers = headers;
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
        this.body = body;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public void compileScripts(WatchInitializationService watchInitializationService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.bodyTemplateScriptFactory = watchInitializationService.compileTemplate("body", this.body, validationErrors);
        this.pathTemplateScriptFactory = watchInitializationService.compileTemplate("path", this.path, validationErrors);
        this.queryParamsTemplateScriptFactory = watchInitializationService.compileTemplate("query_params", this.queryParams, validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    public HttpUriRequest createHttpRequest(WatchExecutionContext ctx)
            throws UnsupportedEncodingException, WatchExecutionException, NotWhitelistedException {

        URI uri = getRenderedUri(ctx);

        checkWhitelist(ctx, uri);

        HttpUriRequest result = createHttpRequest(uri, method);

        if (getHeaders() != null) {
            for (Map.Entry<String, Object> header : getHeaders().entrySet()) {
                result.setHeader(header.getKey(), String.valueOf(header.getValue()));
            }
        }

        if (auth instanceof BasicAuth) {
            BasicAuth basicAuth = (BasicAuth) auth;
            String encodedAuth = Base64.getEncoder().encodeToString((basicAuth.getUsername() + ":" + basicAuth.getPassword()).getBytes());

            result.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
        }

        String body = null;

        if (result instanceof HttpEntityEnclosingRequestBase) {
            body = getRenderedBody(ctx);
            ((HttpEntityEnclosingRequestBase) result).setEntity(new StringEntity(body));
        }

        if (log.isDebugEnabled()) {
            log.debug("Rendered HTTP request:\n" + result + "\n" + body);
        }

        return result;
    }

    // TODO maybe move this to a dedicated http client
    public void checkHttpResponse(HttpRequest request, HttpResponse response) throws WatchExecutionException {
        if (response.getEntity() == null || response.getEntity().getContentType() == null
                || response.getEntity().getContentType().getValue() == null) {
            return;
        }

        String receivedContentType = response.getEntity().getContentType().getValue();
        String accept = this.accept;

        if (accept == null && this.headers != null && this.headers.containsKey("Accept")) {
            accept = String.valueOf(this.headers.get("Accept"));
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

    private HttpUriRequest createHttpRequest(URI url, Method method) throws UnsupportedEncodingException, WatchExecutionException {

        switch (method) {
        case POST:
            return new HttpPost(url);
        case PUT:
            return new HttpPut(url);
        case DELETE:
            return new HttpDelete(url);
        case GET:
            return new HttpGet(url);
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

    private String getRenderedBody(WatchExecutionContext ctx) {
        if (this.body == null) {
            return "";
        }

        Map<String, Object> runtimeData = ctx.getTemplateScriptParamsAsMap();

        return this.bodyTemplateScriptFactory.newInstance(runtimeData).execute();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("method", method);

        if (uri != null) {
            builder.field("url", String.valueOf(uri));
        }

        builder.field("body", body);

        if (headers != null) {
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

    public static HttpRequestConfig create(WatchInitializationService watchInitService, JsonNode objectNode)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(objectNode, validationErrors);

        Method method = vJsonNode.caseInsensitiveEnum("method", Method.class, Method.POST);
        URI uri = vJsonNode.requiredURI("url");
        String body = vJsonNode.string("body");
        String path = vJsonNode.string("path");
        String queryParams = vJsonNode.string("query_params");
        String accept = vJsonNode.string("accept");
        Map<String, Object> headers = null;
        Auth auth = null;

        if (vJsonNode.hasNonNull("headers")) {
            headers = JacksonTools.toMap(vJsonNode.get("headers"));
        }

        if (vJsonNode.hasNonNull("auth")) {
            try {
                auth = Auth.create(vJsonNode.get("auth"));
            } catch (ConfigValidationException e) {
                validationErrors.add("auth", e);
            }
        }

        vJsonNode.validateUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        HttpRequestConfig result = new HttpRequestConfig(method, uri, path, queryParams, body, headers, auth, accept);

        result.compileScripts(watchInitService);

        return result;

    }

    public Auth getAuth() {
        return auth;
    }

    public void setAuth(Auth auth) {
        this.auth = auth;
    }

    public static enum Method {
        POST, PUT, DELETE, GET;
    }
}
