/*
 * Copyright 2021-2024 floragunn GmbH
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

package com.floragunn.searchguard.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContentObject;

import com.floragunn.codova.documents.ContentType;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format.UnknownDocTypeException;
import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;
import com.floragunn.searchguard.test.helper.cluster.EsClientProvider.UserCredentialsHolder;
import com.google.common.collect.Lists;

public class GenericRestClient implements AutoCloseable {
    private static final Logger log = LogManager.getLogger(GenericRestClient.class);

    public boolean enableHTTPClientSSL = true;
    public boolean enableHTTPClientSSLv3Only = false;
    private final InetSocketAddress nodeHttpAddress;
    private GenericSSLConfig sslConfig;
    private RequestConfig requestConfig;
    private final List<Header> headers = new ArrayList<>();
    private final Header CONTENT_TYPE_JSON = new BasicHeader("Content-Type", "application/json");
    private final Header CONTENT_TYPE_JSON_MERGE = new BasicHeader("Content-Type", "application/merge-patch+json");
    private final Header CONTENT_TYPE_NDJSON = new BasicHeader("Content-Type", "application/x-ndjson");

    private boolean trackResources = false;
    private GenericRestClient clientForTrackedResourceDeletion;
    private Consumer<RequestInfo> requestInfoConsumer;

    private final Set<String> puttedResourcesSet = new HashSet<>();
    private final List<String> puttedResourcesList = new ArrayList<>();
    private final SSLContext sslContext;

    /**
     * The UserCredentialsHolder is just used for logging. Headers and/or sslContext are expected to have proper authorization information.
     */
    private final UserCredentialsHolder user;

    public GenericRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers, SSLContext sslContext, UserCredentialsHolder user,
            Consumer<RequestInfo> requestInfoConsumer) {
        this.nodeHttpAddress = nodeHttpAddress;
        this.headers.addAll(headers);
        this.sslContext = sslContext;
        this.user = user;
        this.requestInfoConsumer = requestInfoConsumer;
    }

    public HttpResponse get(String path, Header... headers) throws Exception {
        return executeRequest(new HttpGet(getHttpServerUri() + "/" + path), new RequestInfo().path(path).method("GET"), headers);
    }

    public HttpResponse head(String path, Header... headers) throws Exception {
        return executeRequest(new HttpHead(getHttpServerUri() + "/" + path), new RequestInfo().path(path).method("HEAD"), headers);
    }

    public HttpResponse options(String path, Header... headers) throws Exception {
        return executeRequest(new HttpOptions(getHttpServerUri() + "/" + path), new RequestInfo().path(path).method("OPTIONS"), headers);
    }

    public HttpResponse putJson(String path, String body, Header... headers) throws Exception {
        HttpPut uriRequest = new HttpPut(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body, org.apache.http.entity.ContentType.APPLICATION_JSON));

        HttpResponse response = executeRequest(uriRequest, new RequestInfo().path(path).method("PUT").requestBody(body),
                mergeHeaders(CONTENT_TYPE_JSON, headers));

        if (response.getStatusCode() < 400 && trackResources && !puttedResourcesSet.contains(path)) {
            puttedResourcesSet.add(path);
            puttedResourcesList.add(path);
        }

        return response;
    }

    public HttpResponse putJson(String path, Document<?> body) throws Exception {
        return putJson(path, body.toJsonString());
    }

    public HttpResponse putJson(String path, ToXContentObject body) throws Exception {
        return putJson(path, Strings.toString(body));
    }

    public HttpResponse put(String path) throws Exception {
        HttpPut uriRequest = new HttpPut(getHttpServerUri() + "/" + path);
        HttpResponse response = executeRequest(uriRequest, new RequestInfo().path(path).method("PUT"));

        if (response.getStatusCode() < 400 && trackResources && !puttedResourcesSet.contains(path)) {
            puttedResourcesSet.add(path);
            puttedResourcesList.add(path);
        }

        return response;
    }

    public HttpResponse putNdJson(String path, Document<?>... bodyElements) throws Exception {
        HttpPut uriRequest = new HttpPut(getHttpServerUri() + "/" + path);
        StringBuilder body = new StringBuilder();
        for (Document<?> bodyElement : bodyElements) {
            body.append(bodyElement.toJsonString());
            body.append("\n");
        }

        uriRequest.setEntity(new StringEntity(body.toString(), org.apache.http.entity.ContentType.APPLICATION_JSON));

        HttpResponse response = executeRequest(uriRequest, new RequestInfo().path(path).method("PUT").requestBody(body.toString()),
                CONTENT_TYPE_NDJSON);

        if (response.getStatusCode() < 400 && trackResources && !puttedResourcesSet.contains(path)) {
            puttedResourcesSet.add(path);
            puttedResourcesList.add(path);
        }

        return response;
    }

    public HttpResponse delete(String path, Header... headers) throws Exception {
        return executeRequest(new HttpDelete(getHttpServerUri() + "/" + path), new RequestInfo().path(path).method("DELETE"), headers);
    }

    public HttpResponse postJson(String path, String body, Header... headers) throws Exception {
        HttpPost uriRequest = new HttpPost(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body, org.apache.http.entity.ContentType.APPLICATION_JSON));
        return executeRequest(uriRequest, new RequestInfo().path(path).method("POST").requestBody(body), mergeHeaders(CONTENT_TYPE_JSON, headers));
    }

    public HttpResponse postJson(String path, ToXContentObject body) throws Exception {
        return postJson(path, Strings.toString(body));
    }

    public HttpResponse postJson(String path, Map<String, Object> body, Header... headers) throws Exception {
        return postJson(path, DocWriter.json().writeAsString(body), headers);
    }

    public HttpResponse post(String path) throws Exception {
        HttpPost uriRequest = new HttpPost(getHttpServerUri() + "/" + path);
        return executeRequest(uriRequest, new RequestInfo().path(path).method("POST"));
    }

    public HttpResponse post(String path, Header... headers) throws Exception {
        HttpPost uriRequest = new HttpPost(getHttpServerUri() + "/" + path);
        return executeRequest(uriRequest, new RequestInfo().path(path).method("POST"), headers);
    }

    public HttpResponse patch(String path, String body) throws Exception {
        HttpPatch uriRequest = new HttpPatch(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));
        return executeRequest(uriRequest, new RequestInfo().path(path).method("PATCH").requestBody(body), CONTENT_TYPE_JSON);
    }

    public HttpResponse patch(String path, DocPatch docPatch, Header... headers) throws Exception {
        HttpPatch uriRequest = new HttpPatch(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(docPatch.toJsonString(), org.apache.http.entity.ContentType.APPLICATION_JSON));
        return executeRequest(uriRequest, new RequestInfo().path(path).method("PATCH").requestBody(docPatch.toJsonString()),
                mergeHeaders(new BasicHeader("Content-Type", docPatch.getMediaType()), headers));
    }

    public HttpResponse patchJsonMerge(String path, Document<?> body, Header... headers) throws Exception {
        return patchJsonMerge(path, body.toJsonString(), headers);
    }

    public HttpResponse patchJsonMerge(String path, String body, Header... headers) throws Exception {
        HttpPatch uriRequest = new HttpPatch(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body, org.apache.http.entity.ContentType.APPLICATION_JSON));
        return executeRequest(uriRequest, new RequestInfo().path(path).method("PUT").requestBody(body),
                mergeHeaders(CONTENT_TYPE_JSON_MERGE, headers));
    }

    public HttpResponse executeRequest(HttpUriRequest uriRequest, RequestInfo requestInfo, Header... requestSpecificHeaders) throws Exception {
        CloseableHttpClient httpClient = null;
        try {

            requestInfo.user(user);

            httpClient = getHTTPClient();

            if (requestSpecificHeaders != null && requestSpecificHeaders.length > 0) {
                for (int i = 0; i < requestSpecificHeaders.length; i++) {
                    Header h = requestSpecificHeaders[i];
                    uriRequest.addHeader(h);
                }
            }

            for (Header header : headers) {
                uriRequest.addHeader(header);
            }

            HttpResponse response = new HttpResponse(innerExecuteRequest(httpClient, uriRequest));
            requestInfo.response(response);
            log.debug(response.getBody());
            return response;
        } finally {

            if (this.requestInfoConsumer != null) {
                this.requestInfoConsumer.accept(requestInfo);
            }

            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    protected CloseableHttpResponse innerExecuteRequest(CloseableHttpClient httpClient, HttpUriRequest uriRequest)
            throws ClientProtocolException, IOException {

        return httpClient.execute(uriRequest);
    }

    public GenericRestClient trackResources() {
        this.trackResources = true;
        this.clientForTrackedResourceDeletion = this;
        return this;
    }

    public GenericRestClient trackResources(GenericRestClient clientForTrackedResourceDeletion) {
        this.trackResources = true;
        this.clientForTrackedResourceDeletion = clientForTrackedResourceDeletion;
        return this;
    }

    public GenericRestClient recordRequests(Consumer<RequestInfo> requestInfoConsumer) {
        this.requestInfoConsumer = requestInfoConsumer;
        return this;
    }

    public GenericRestClient deleteWhenClosed(String... paths) {
        if (this.clientForTrackedResourceDeletion == null) {
            this.clientForTrackedResourceDeletion = this;
        }
        
        for (String path : paths) {
            if (!puttedResourcesSet.contains(path)) {
                puttedResourcesSet.add(path);
                puttedResourcesList.add(path);
            }
        }
        return this;
    }

    private void cleanupResources() {
        if (puttedResourcesList.size() > 0) {
            log.info("Cleaning up " + puttedResourcesList);

            for (String resource : Lists.reverse(puttedResourcesList)) {
                try {
                    this.clientForTrackedResourceDeletion.delete(resource);
                } catch (Exception e) {
                    log.error("Error cleaning up created resources " + resource, e);
                }
            }
        }
    }

    protected final String getHttpServerUri() {
        return "http" + (enableHTTPClientSSL ? "s" : "") + "://" + nodeHttpAddress.getHostString() + ":" + nodeHttpAddress.getPort();
    }

    protected final CloseableHttpClient getHTTPClient() throws Exception {
        HttpClientBuilder clientBuilder = HttpClients.custom();

        if (sslConfig != null) {
            clientBuilder.setSSLSocketFactory(sslConfig.toSSLConnectionSocketFactory());
        } else if (enableHTTPClientSSL) {

            log.debug("Configure HTTP client with SSL");

            String[] protocols = null;

            if (enableHTTPClientSSLv3Only) {
                protocols = new String[] { "SSLv3" };
            } else {
                protocols = new String[] { "TLSv1", "TLSv1.1", "TLSv1.2" };
            }

            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, protocols, null, NoopHostnameVerifier.INSTANCE);

            clientBuilder.setSSLSocketFactory(sslsf);
        }

        clientBuilder.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(60 * 10000).build());

        if (requestConfig != null) {
            clientBuilder.setDefaultRequestConfig(requestConfig);
        }

        configureHttpClientBuilder(clientBuilder);

        return clientBuilder.build();
    }

    protected void configureHttpClientBuilder(HttpClientBuilder clientBuilder) {

    }

    private Header[] mergeHeaders(Header header, Header... headers) {

        if (headers == null || headers.length == 0) {
            return new Header[] { header };
        } else {
            Header[] result = new Header[headers.length + 1];
            result[0] = header;
            System.arraycopy(headers, 0, result, 1, headers.length);
            return result;
        }
    }

    public GenericSSLConfig getSslConfig() {
        return sslConfig;
    }

    public void setSslConfig(GenericSSLConfig sslConfig) {
        this.sslConfig = sslConfig;
    }

    @Override
    public String toString() {
        return "RestHelper [server=" + getHttpServerUri() + ", node=" + nodeHttpAddress + ", sslConfig=" + sslConfig + "]";
    }

    public RequestConfig getRequestConfig() {
        return requestConfig;
    }

    public void setRequestConfig(RequestConfig requestConfig) {
        this.requestConfig = requestConfig;
    }

    public void setLocalAddress(InetAddress inetAddress) {
        if (requestConfig == null) {
            requestConfig = RequestConfig.custom().setLocalAddress(inetAddress).build();
        } else {
            requestConfig = RequestConfig.copy(requestConfig).setLocalAddress(inetAddress).build();
        }
    }

    @Override
    public void close() throws IOException {
        cleanupResources();
    }

    public static class HttpResponse {
        private final CloseableHttpResponse inner;
        private final String body;
        private final Header[] headers;
        private final int statusCode;
        private final String statusReason;
        private DocNode parsedBody;

        public HttpResponse(CloseableHttpResponse inner) throws IllegalStateException, IOException {
            super();
            this.inner = inner;
            final HttpEntity entity = inner.getEntity();
            if (entity == null) { //head request does not have a entity
                this.body = "";
            } else {
                this.body = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
            }
            this.headers = inner.getAllHeaders();
            this.statusCode = inner.getStatusLine().getStatusCode();
            this.statusReason = inner.getStatusLine().getReasonPhrase();
            inner.close();
        }

        public String getContentType() {
            Header h = getInner().getFirstHeader("content-type");
            if (h != null) {
                return h.getValue();
            }
            return null;
        }

        public boolean isJsonContentType() {
            String ct = getContentType();
            if (ct == null) {
                return false;
            }
            return ct.contains("application/json");
        }

        public CloseableHttpResponse getInner() {
            return inner;
        }

        public String getBody() {
            return body;
        }

        public DocNode getBodyAsDocNode() throws DocumentParseException, UnknownDocTypeException {
            DocNode result = this.parsedBody;

            if (result != null) {
                return result;
            }

            result = DocNode.parse(ContentType.parseHeader(getContentType())).from(body);

            this.parsedBody = result;

            return result;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getStatusReason() {
            return statusReason;
        }

        public List<Header> getHeaders() {
            return headers == null ? Collections.emptyList() : Arrays.asList(headers);
        }

        public String getHeaderValue(String name) {
            for (Header header : this.headers) {
                if (header.getName().equalsIgnoreCase(name)) {
                    return header.getValue();
                }
            }

            return null;
        }

        @Override
        public String toString() {
            return "HttpResponse [inner=" + inner + ", body=" + body + ", header=" + Arrays.toString(headers) + ", statusCode=" + statusCode
                    + ", statusReason=" + statusReason + "]";
        }

    }

    public static class RequestInfo {
        private UserCredentialsHolder user;
        private String path;
        private String method;
        private String requestBody;
        private HttpResponse response;

        public RequestInfo() {
        }

        public UserCredentialsHolder getUser() {
            return user;
        }

        public RequestInfo user(UserCredentialsHolder user) {
            this.user = user;
            return this;
        }

        public String getPath() {
            return path;
        }

        public RequestInfo path(String path) {
            if (path.length() == 0) {
                path = "/";
            } else if (!path.startsWith("/")) {
                path = "/" + path;
            }

            this.path = path;
            return this;
        }

        public String getMethod() {
            return method;
        }

        public RequestInfo method(String method) {
            this.method = method;
            return this;
        }

        public String getRequestBody() {
            return requestBody;
        }

        public RequestInfo requestBody(String requestBody) {
            this.requestBody = requestBody;
            return this;
        }

        public HttpResponse getResponse() {
            return response;
        }

        public RequestInfo response(HttpResponse response) {
            this.response = response;
            return this;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();

            if (user != null) {
                result.append(user.getName()).append(": ");
            }

            result.append(method).append(" ").append(path).append("\n");

            if (requestBody != null) {
                result.append(requestBody).append("\n");
            }

            result.append("\n");

            if (response != null) {
                result.append(response.statusCode).append(" ").append(response.statusReason).append("\n");
                if (response.isJsonContentType()) {
                    try {
                        String prettyJson = response.getBodyAsDocNode().toPrettyJsonString();
                        
                        if (countOfLines(prettyJson) <= 25) {
                            result.append(prettyJson).append("\n");                            
                        } else {                            
                            result.append(response.getBody()).append("\n");                            
                        }
                        
                    } catch (DocumentParseException | UnknownDocTypeException e) {
                        result.append(response.getBody()).append("\n");
                    }
                } else {
                    result.append(response.getBody()).append("\n");
                }
            }

            return result.toString();
        }
        
        private int countOfLines(String s) {
            int result = 0;
            
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) == '\n') result++;
            }
            
            return result;
        }

    }

    public UserCredentialsHolder getUser() {
        return user;
    }
}
