package com.floragunn.searchguard.test.helper.rest;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;
import com.floragunn.searchguard.test.helper.file.FileHelper;

public class GenericRestClient implements AutoCloseable {
    private static final Logger log = LogManager.getLogger(RestHelper.class);

    public boolean enableHTTPClientSSL = true;
    public boolean enableHTTPClientSSLv3Only = false;
    public boolean sendHTTPClientCertificate = false;
    public boolean trustHTTPServerCertificate = true;
    public String keystore = "node-0-keystore.jks";
    public final String prefix;
    private ClusterInfo clusterInfo;
    private int nodeIndex = -1;
    private GenericSSLConfig sslConfig;
    private RequestConfig requestConfig;
    private List<Header> headers = new ArrayList<>();
    private Header CONTENT_TYPE_JSON = new BasicHeader("Content-Type", "application/json");

    public GenericRestClient(ClusterInfo clusterInfo, List<Header> headers, String prefix) {
        this.clusterInfo = clusterInfo;
        this.headers.addAll(headers);
        this.prefix = prefix;
    }

    public GenericRestClient(ClusterInfo clusterInfo, boolean enableHTTPClientSSL, boolean trustHTTPServerCertificate, String prefix) {
        this.clusterInfo = clusterInfo;
        this.enableHTTPClientSSL = enableHTTPClientSSL;
        this.trustHTTPServerCertificate = trustHTTPServerCertificate;
        this.prefix = prefix;
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

    public HttpResponse putJson(String path, String body) throws Exception {
        HttpPut uriRequest = new HttpPut(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));
        return executeRequest(uriRequest, CONTENT_TYPE_JSON);
    }

    public HttpResponse delete(String path, Header... headers) throws Exception {
        return executeRequest(new HttpDelete(getHttpServerUri() + "/" + path), headers);
    }

    public HttpResponse postJson(String path, String body) throws Exception {
        HttpPost uriRequest = new HttpPost(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));

        return executeRequest(uriRequest, CONTENT_TYPE_JSON);
    }

    public HttpResponse patch(String path, String body) throws Exception {
        HttpPatch uriRequest = new HttpPatch(getHttpServerUri() + "/" + path);
        uriRequest.setEntity(new StringEntity(body));
        return executeRequest(uriRequest, CONTENT_TYPE_JSON);
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

    protected final String getHttpServerUri() {
        if (nodeIndex == -1) {
            return "http" + (enableHTTPClientSSL ? "s" : "") + "://" + clusterInfo.httpHost + ":" + clusterInfo.httpPort;
        } else {
            return "http" + (enableHTTPClientSSL ? "s" : "") + "://" + clusterInfo.httpAdresses.get(nodeIndex).getAddress() + ":"
                    + clusterInfo.httpAdresses.get(nodeIndex).getPort();
        }
    }

    protected final CloseableHttpClient getHTTPClient() throws Exception {

        final HttpClientBuilder hcb = HttpClients.custom();

        if (sslConfig != null) {
            hcb.setSSLSocketFactory(sslConfig.toSSLConnectionSocketFactory());
        } else if (enableHTTPClientSSL) {

            log.debug("Configure HTTP client with SSL");

            if (prefix != null && !keystore.contains("/")) {
                keystore = prefix + "/" + keystore;
            }

            final String keyStorePath = FileHelper.getAbsoluteFilePathFromClassPath(keystore).toFile().getParent();

            final KeyStore myTrustStore = KeyStore.getInstance("JKS");
            myTrustStore.load(new FileInputStream(keyStorePath + "/truststore.jks"), "changeit".toCharArray());

            final KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath(keystore).toFile()), "changeit".toCharArray());

            final SSLContextBuilder sslContextbBuilder = SSLContexts.custom();

            if (trustHTTPServerCertificate) {
                sslContextbBuilder.loadTrustMaterial(myTrustStore, null);
            }

            if (sendHTTPClientCertificate) {
                sslContextbBuilder.loadKeyMaterial(keyStore, "changeit".toCharArray());
            }

            final SSLContext sslContext = sslContextbBuilder.build();

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

    public static class HttpResponse {
        private final CloseableHttpResponse inner;
        private final String body;
        private final Header[] header;
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
            this.header = inner.getAllHeaders();
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

        public Header[] getHeader() {
            return header;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getStatusReason() {
            return statusReason;
        }

        public List<Header> getHeaders() {
            return header == null ? Collections.emptyList() : Arrays.asList(header);
        }

        public JsonNode toJsonNode() throws JsonProcessingException, IOException {
            return DefaultObjectMapper.objectMapper.readTree(getBody());
        }

        @Override
        public String toString() {
            return "HttpResponse [inner=" + inner + ", body=" + body + ", header=" + Arrays.toString(header) + ", statusCode=" + statusCode
                    + ", statusReason=" + statusReason + "]";
        }

    }

    public int getNodeIndex() {
        return nodeIndex;
    }

    public void setNodeIndex(int nodeIndex) {
        this.nodeIndex = nodeIndex;
    }

    public GenericSSLConfig getSslConfig() {
        return sslConfig;
    }

    public void setSslConfig(GenericSSLConfig sslConfig) {
        this.sslConfig = sslConfig;
    }

    @Override
    public String toString() {
        return "RestHelper [server=" + getHttpServerUri() + ", nodeIndex=" + nodeIndex + ", sslConfig=" + sslConfig + "]";
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
        // TODO Auto-generated method stub
        
    }
}
