package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.json.JacksonTools;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.HttpUtils;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class HttpInput extends AbstractInput {
    private static final Logger log = LogManager.getLogger(HttpInput.class);

    private HttpClientConfig httpClientConfig;
    private HttpRequestConfig request;

    public HttpInput(String name, String target, HttpRequestConfig request, HttpClientConfig httpClientConfig) {
        super(name, target);
        this.request = request;
        this.httpClientConfig = httpClientConfig;
    }

    @Override
    public boolean execute(WatchExecutionContext ctx) throws CheckExecutionException {

        try (CloseableHttpClient httpClient = httpClientConfig.createHttpClient(ctx.getHttpProxyConfig())) {
            HttpUriRequest httpRequest = request.createHttpRequest(ctx);
            CloseableHttpResponse response = AccessController
                    .doPrivileged((PrivilegedExceptionAction<CloseableHttpResponse>) () -> httpClient.execute(httpRequest));

            if (log.isDebugEnabled()) {
                log.debug("HTTP response for " + this + ": " + response + "\n" + response);
            }

            if (response.getStatusLine().getStatusCode() >= 400) {
                throw new CheckExecutionException(this,
                        "HTTP input web service returned error: " + response.getStatusLine() + "\n" + HttpUtils.getEntityAsDebugString(response));
            }

            this.request.checkHttpResponse(httpRequest, response);

            ObjectMapper contentTypeObjectMapper = getObjectMapper(response);

            if (contentTypeObjectMapper == null) {
                // just treat the response as plain text

                try {
                    setResult(ctx, HttpUtils.getEntityAsString(response));
                } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
                    throw new CheckExecutionException(this,
                            "HTTP response contained content encoding" + response.getEntity().getContentEncoding().getValue(), e);
                } catch (IOException e) {
                    throw new CheckExecutionException(this, "Error while decoding HTTP response", e);
                }
            } else {
                JsonNode tree = contentTypeObjectMapper.readTree(response.getEntity().getContent());
                Object object = JacksonTools.toObject(tree);

                setResult(ctx, object);
            }

            return true;
        } catch (CheckExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new CheckExecutionException(this, e.getMessage(), e);
        }
    }

    private ObjectMapper getObjectMapper(HttpResponse response) {
        String contentType = getContentType(response);

        if (contentType == null) {
            return null;
        }

        switch (contentType) {
        case "application/json":
        case "text/json":
            return new ObjectMapper();
        case "application/yaml":
        case "application/x-yaml":
        case "text/yaml":
        case "text/x-yaml":
            return new ObjectMapper(new YAMLFactory());
        default:
            return null;
        }

    }

    private String getContentType(HttpResponse response) {
        Header header = response.getEntity().getContentType();

        if (header == null) {
            return null;
        }

        ContentType contentType = ContentType.parse(header.getValue());

        return contentType.getMimeType().toLowerCase();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("type", "http");

        if (name != null) {
            builder.field("name", name);
        }

        if (target != null) {
            builder.field("target", target);
        }

        builder.field("request");
        request.toXContent(builder, params);

        httpClientConfig.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    static HttpInput create(WatchInitializationService watchInitService, DocNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String name = vJsonNode.get("name").asString();
        String target = vJsonNode.get("target").asString();
        HttpRequestConfig request = vJsonNode.get("request").required().by((n) -> HttpRequestConfig.create(watchInitService, n));
        HttpClientConfig httpClientConfig = null;
        
        try {
            httpClientConfig = HttpClientConfig.create(vJsonNode);
        } catch (ConfigValidationException e) {
            validationErrors.add(null, e);
        }

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        HttpInput result = new HttpInput(name, target, request, httpClientConfig);

        return result;

    }

}
