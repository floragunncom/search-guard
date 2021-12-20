/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.rest;

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

import javax.net.ssl.SSLContext;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.documents.ContentType;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocParseException;
import com.floragunn.codova.documents.DocType.UnknownDocTypeException;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Document;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;
import com.google.common.collect.Lists;

public class GenericRestClient implements AutoCloseable {
    private static final Logger log = LogManager.getLogger(RestHelper.class);

    public boolean enableHTTPClientSSL = true;
    public boolean enableHTTPClientSSLv3Only = false;
    private final InetSocketAddress nodeHttpAddress;
    private GenericSSLConfig sslConfig;
    private RequestConfig requestConfig;
    private final List<Header> headers = new ArrayList<>();
    private final Header CONTENT_TYPE_JSON = new BasicHeader("Content-Type", "application/json");
    private final Header CONTENT_TYPE_JSON_MERGE = new BasicHeader("Content-Type", "application/merge-patch+json");

    private boolean trackResources = false;

    private final Set<String> puttedResourcesSet = new HashSet<>();
    private final List<String> puttedResourcesList = new ArrayList<>();
    private final SSLContext sslContext;

    public GenericRestClient(InetSocketAddress nodeHttpAddress, List<Header> headers, SSLContext sslContext) {
        this.nodeHttpAddress = nodeHttpAddress;
        this.headers.addAll(headers);
        this.sslContext = sslContext;
    }

    public GenericRestClient(InetSocketAddress nodeHttpAddress, boolean enableHTTPClientSSL, SSLContext sslContext) {
        this.nodeHttpAddress = nodeHttpAddress;
        this.enableHTTPClientSSL = enableHTTPClientSSL;
        this.sslContext = sslContext;
    }

    public HttpResponse get(String path, Header... headers) throws Exception {
        return executeRequest(new HttpGet(getHttpServerUri() + "/" + path), headers);
    }

    public HttpResponse head(String path, Header... headers) throws Exception {
        return executeRequest(new HttpHead(getHttpServerUri() + "/" + path), headers);
    }

    public HttpResponse options(String path, Header... headers) throws Exception {
        return executeRequest(new HttpOptions(getHttpServerUri() + "/" + path), headers);
    }

    public HttpResponse putJson(String path, String body, Header... headers) throws Exception {
        HttpPut uriRequest = new HttpPut(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));

        HttpResponse response = executeRequest(uriRequest, mergeHeaders(CONTENT_TYPE_JSON, headers));

        if (response.getStatusCode() < 400 && trackResources && !puttedResourcesSet.contains(path)) {
            puttedResourcesSet.add(path);
            puttedResourcesList.add(path);
        }

        return response;
    }

    public HttpResponse putJson(String path, Document body) throws Exception {
        return putJson(path, body.toJsonString());
    }
    
    public HttpResponse putJson(String path, ToXContentObject body) throws Exception {
        return putJson(path, Strings.toString(body));
    }

    public HttpResponse put(String path) throws Exception {
        HttpPut uriRequest = new HttpPut(getHttpServerUri() + "/" + path);
        HttpResponse response = executeRequest(uriRequest);

        if (response.getStatusCode() < 400 && trackResources && !puttedResourcesSet.contains(path)) {
            puttedResourcesSet.add(path);
            puttedResourcesList.add(path);
        }

        return response;
    }

    public HttpResponse delete(String path, Header... headers) throws Exception {
        return executeRequest(new HttpDelete(getHttpServerUri() + "/" + path), headers);
    }

    public HttpResponse postJson(String path, String body, Header... headers) throws Exception {
        HttpPost uriRequest = new HttpPost(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));
        return executeRequest(uriRequest, mergeHeaders(CONTENT_TYPE_JSON, headers));
    }

    public HttpResponse postJson(String path, ToXContentObject body) throws Exception {
        return postJson(path, Strings.toString(body));
    }

    public HttpResponse postJson(String path, Map<String, Object> body, Header... headers) throws Exception {
        return postJson(path, DocWriter.json().writeAsString(body), headers);
    }

    public HttpResponse post(String path) throws Exception {
        HttpPost uriRequest = new HttpPost(getHttpServerUri() + "/" + path);
        return executeRequest(uriRequest);
    }

    public HttpResponse patch(String path, String body) throws Exception {
        HttpPatch uriRequest = new HttpPatch(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));
        return executeRequest(uriRequest, CONTENT_TYPE_JSON);
    }
    
    public HttpResponse patchJsonMerge(String path, Document body, Header... headers) throws Exception {
        return patchJsonMerge(path, body.toJsonString(), headers);
    }
    
    public HttpResponse patchJsonMerge(String path, String body, Header... headers) throws Exception {
        HttpPatch uriRequest = new HttpPatch(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));
        return executeRequest(uriRequest, mergeHeaders(CONTENT_TYPE_JSON_MERGE, headers));
    }

    public HttpResponse executeRequest(HttpUriRequest uriRequest, Header... requestSpecificHeaders) throws Exception {
        CloseableHttpClient httpClient = null;
        try {

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

            HttpResponse res = new HttpResponse(httpClient.execute(uriRequest));
            log.debug(res.getBody());
            return res;
        } finally {

            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public GenericRestClient trackResources() {
        trackResources = true;
        return this;
    }

    private void cleanupResources() {
        if (puttedResourcesList.size() > 0) {
            log.info("Cleaning up " + puttedResourcesList);

            for (String resource : Lists.reverse(puttedResourcesList)) {
                try {
                    delete(resource);
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

        final HttpClientBuilder hcb = HttpClients.custom();

        if (sslConfig != null) {
            hcb.setSSLSocketFactory(sslConfig.toSSLConnectionSocketFactory());
        } else if (enableHTTPClientSSL) {

            log.debug("Configure HTTP client with SSL");

            String[] protocols = null;

            if (enableHTTPClientSSLv3Only) {
                protocols = new String[] { "SSLv3" };
            } else {
                protocols = new String[] { "TLSv1", "TLSv1.1", "TLSv1.2" };
            }

            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, protocols, null, NoopHostnameVerifier.INSTANCE);

            hcb.setSSLSocketFactory(sslsf);
        }

        hcb.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(60 * 1000).build());

        if (requestConfig != null) {
            hcb.setDefaultRequestConfig(requestConfig);
        }

        return hcb.build();
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

        public DocNode getBodyAsDocNode() throws DocParseException, UnknownDocTypeException {
            return DocNode.parse(ContentType.parseHeader(getContentType())).from(body);
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

        public JsonNode toJsonNode() throws JsonProcessingException, IOException {
            return DefaultObjectMapper.objectMapper.readTree(getBody());
        }

        @Override
        public String toString() {
            return "HttpResponse [inner=" + inner + ", body=" + body + ", header=" + Arrays.toString(headers) + ", statusCode=" + statusCode
                    + ", statusReason=" + statusReason + "]";
        }

    }
}
