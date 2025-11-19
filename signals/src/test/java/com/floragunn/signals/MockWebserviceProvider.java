/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.signals;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.certificate.utils.CertificateAndPrivateKeyWriter;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.Header;
import org.apache.http.HttpConnectionFactory;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpInetConnection;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentLengthStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.ConnSupport;
import org.apache.http.impl.DefaultBHttpServerConnection;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.SSLServerSetupHandler;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.io.HttpMessageParserFactory;
import org.apache.http.io.HttpMessageWriterFactory;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.bouncycastle.cert.X509CertificateHolder;

import static com.floragunn.signals.truststore.rest.TruststoreLoader.storeTruststoreInPemFormat;

public class MockWebserviceProvider implements Closeable {

    private HttpServer httpServer;
    private final int port;
    private final String uri;
    private final boolean ssl;
    private int responseStatusCode;
    private byte[] responseBody;
    private String responseContentType;
    private String lastRequestBody;
    private List<Header> lastRequestHeaders;
    private InetAddress lastRequestClientAddress;
    private final AtomicInteger requestCount = new AtomicInteger();
    private long responseDelayMs;
    private Header requiredHttpHeader;
    private KeyStore trustStore;

    MockWebserviceProvider(
            String pathPattern, boolean ssl, boolean clientAuth, byte[] responseBody, String responseContentType,
            long responseDelayMs, int responseStatusCode, Header requiredHttpHeader) throws IOException {
        this.port = SocketUtils.findAvailableTcpPort();
        this.uri = buildUri(pathPattern, ssl, port);
        this.ssl = ssl;
        this.responseBody = responseBody;
        this.responseContentType = responseContentType;
        this.responseDelayMs = responseDelayMs;
        this.responseStatusCode = responseStatusCode;
        this.requiredHttpHeader = requiredHttpHeader;

        ServerBootstrap serverBootstrap = ServerBootstrap.bootstrap().setListenerPort(port).registerHandler(pathPattern, new HttpRequestHandler() {

            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {

                handleRequest(request, response, context);

            }
        });

        if (ssl) {
            serverBootstrap = serverBootstrap.setSslContext(createSSLContext()).setSslSetupHandler(new SSLServerSetupHandler() {

                @Override
                public void initialize(SSLServerSocket socket) throws SSLException {
                    socket.setNeedClientAuth(clientAuth);
                }
            }).setConnectionFactory(new HttpConnectionFactory<DefaultBHttpServerConnection>() {

                private ConnectionConfig cconfig = ConnectionConfig.DEFAULT;

                @Override
                public DefaultBHttpServerConnection createConnection(final Socket socket) throws IOException {
                    final SSLTestHttpServerConnection conn = new SSLTestHttpServerConnection(this.cconfig.getBufferSize(),
                            this.cconfig.getFragmentSizeHint(), ConnSupport.createDecoder(this.cconfig), ConnSupport.createEncoder(this.cconfig),
                            this.cconfig.getMessageConstraints(), null, null, null, null);
                    conn.bind(socket);
                    return conn;
                }
            });
        }

        this.httpServer = serverBootstrap.create();
        this.httpServer.start();

    }

    public MockWebserviceProvider acceptOnlyRequestsWithHeader(Header header) {
        this.requiredHttpHeader = header;
        return this;
    }
    
    public void start() throws IOException {
        httpServer.start();
    }

    @Override
    public void close() throws IOException {
        httpServer.stop();
    }

    public HttpServer getHttpServer() {
        return httpServer;
    }

    public String getUri() {
        return uri;
    }

    public int getPort() {
        return port;
    }

    public String trustedCertificatePem(String alias) {
        try {
            Certificate certificate = trustStore.getCertificate(alias);
            byte[] derCertificateBytes = certificate.getEncoded();
            X509CertificateHolder holder = new X509CertificateHolder(derCertificateBytes);
            return CertificateAndPrivateKeyWriter.writeCertificate(holder);
        } catch (KeyStoreException | CertificateEncodingException | IOException e) {
            throw new RuntimeException("Cannot write certificate as pem", e);
        }
    }

    private static String buildUri(String pathPattern, boolean ssl, int port) {
        StringBuilder result = new StringBuilder(ssl ? "https" : "http");
        result.append("://localhost:").append(port);

        if (pathPattern != null) {
            String path = pathPattern;

            if (path.endsWith("*")) {
                path = pathPattern.substring(0, pathPattern.length() - 1);
            }

            if (!path.startsWith("/")) {
                result.append("/");
            }

            result.append(path);
        }

        return result.toString();
    }

    protected void handleRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        if (!checkClientAddress(request, response, context)) {
            return;
        }

        if (responseDelayMs > 0) {
            try {
                Thread.sleep(responseDelayMs);
            } catch (InterruptedException e) {
               
            }
        }
        
        response.setStatusCode(responseStatusCode);
        response.setHeader("Content-Type", responseContentType);
        response.setEntity(new ByteArrayEntity(responseBody));

        if (request instanceof HttpEntityEnclosingRequest) {
            lastRequestBody = CharStreams
                    .toString(new InputStreamReader(((HttpEntityEnclosingRequest) request).getEntity().getContent(), Charsets.UTF_8));
        } else {
            lastRequestBody = null;
        }

        lastRequestHeaders = Arrays.asList(request.getAllHeaders());
                
        requestCount.incrementAndGet();
    }

    private SSLContext createSSLContext() {
        if (!this.ssl) {
            return null;
        }

        try {
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            this.trustStore = KeyStore.getInstance("JKS");
            InputStream trustStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("tls/truststore.jks").toFile());
            trustStore.load(trustStream, "secret".toCharArray());
            tmf.init(trustStore);

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            final KeyStore keyStore = KeyStore.getInstance("JKS");
            InputStream keyStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("tls/node1-keystore.jks").toFile());

            keyStore.load(keyStream, "secret".toCharArray());
            kmf.init(keyStore, "secret".toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return sslContext;
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private boolean checkClientAddress(HttpRequest request, HttpResponse response, HttpContext context) throws UnsupportedEncodingException {
        HttpInetConnection connection = (HttpInetConnection) context.getAttribute("http.connection");

        lastRequestClientAddress = connection.getRemoteAddress();

        if (requiredHttpHeader != null) {
            List<Header> requestHeaders = Arrays.asList(request.getHeaders(requiredHttpHeader.getName()));
            if (requestHeaders.stream().anyMatch(header -> requiredHttpHeader.getValue().equals(header.getValue()))) {
                return true;
            } else {
                response.setStatusCode(451);
                response.setEntity(new StringEntity(
                        "We are only accepting requests with the '" + requiredHttpHeader.getName() + "' header set to '" + requiredHttpHeader.getValue() + "'"));
                return false;
            }
        } else {
            return true;
        }
    }


    static class SSLTestHttpServerConnection extends DefaultBHttpServerConnection {
        public SSLTestHttpServerConnection(final int buffersize, final int fragmentSizeHint, final CharsetDecoder chardecoder,
                final CharsetEncoder charencoder, final MessageConstraints constraints, final ContentLengthStrategy incomingContentStrategy,
                final ContentLengthStrategy outgoingContentStrategy, final HttpMessageParserFactory<HttpRequest> requestParserFactory,
                final HttpMessageWriterFactory<HttpResponse> responseWriterFactory) {
            super(buffersize, fragmentSizeHint, chardecoder, charencoder, constraints, incomingContentStrategy, outgoingContentStrategy,
                    requestParserFactory, responseWriterFactory);
        }

        public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
            return ((SSLSocket) getSocket()).getSession().getPeerCertificates();
        }
    }

    public int getResponseStatusCode() {
        return responseStatusCode;
    }

    public void setResponseStatusCode(int responseStatusCode) {
        this.responseStatusCode = responseStatusCode;
    }

    public byte[] getResponseBody() {
        return responseBody;
    }

    public void setResponseBody(String  responseBody) {
        this.responseBody = responseBody.getBytes();
    }

    public void setResponseBody(byte[] responseBody) {
        this.responseBody = responseBody;
    }

    public String getLastRequestBody() {
        return lastRequestBody;
    }

    public void setLastRequestBody(String lastRequestBody) {
        this.lastRequestBody = lastRequestBody;
    }

    public List<Header> getLastRequestHeaders() {
        return lastRequestHeaders;
    }

    public void setLastRequestHeaders(List<Header> lastRequestHeaders) {
        this.lastRequestHeaders = lastRequestHeaders;
    }

    public Header getLastRequestHeader(String name) {
        return Optional.ofNullable(lastRequestHeaders).orElse(new ArrayList<>())
                .stream().filter(header -> header.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    public String getResponseContentType() {
        return responseContentType;
    }

    public void setResponseContentType(String responseContentType) {
        this.responseContentType = responseContentType;
    }

    public int getRequestCount() {
        return requestCount.get();
    }

    public long getResponseDelayMs() {
        return responseDelayMs;
    }

    public void setResponseDelayMs(long responseDelayMs) {
        this.responseDelayMs = responseDelayMs;
    }

    public InetAddress getLastRequestClientAddress() {
        return lastRequestClientAddress;
    }

    public void uploadMockServerCertificateAsTruststore(LocalCluster cluster, TestSgConfig.User user, String truststoreId)
        throws Exception {
        String pemCertificate = this.trustedCertificatePem("root-ca");
        try(GenericRestClient genericRestClient = cluster.getRestClient(user)) {
            storeTruststoreInPemFormat(genericRestClient, truststoreId, "name", pemCertificate);
        }
    }

    public static class Builder {
        private String path;
        private boolean ssl;
        private boolean clientAuth;
        private byte[] responseBody = "Mockery".getBytes();
        private String responseContentType = "text/plain";
        private long responseDelayMs = 0;
        private int responseStatusCode = 200;
        private Header requiredHttpHeader;

        public Builder(String path) {
            this.path = path;
        }

        public Builder ssl(boolean ssl) {
            this.ssl = ssl;
            return this;
        }

        public Builder clientAuth(boolean clientAuth) {
            this.clientAuth = clientAuth;
            return this;
        }

        public Builder responseBody(byte[] responseBody) {
            this.responseBody = responseBody;
            return this;
        }

        public Builder responseBody(String responseBody) {
            this.responseBody = responseBody.getBytes();
            return this;
        }

        public Builder responseContentType(String responseContentType) {
            this.responseContentType = responseContentType;
            return this;
        }

        public Builder responseDelayMs(long responseDelayMs) {
            this.responseDelayMs = responseDelayMs;
            return this;
        }

        public Builder responseStatusCode(int responseStatusCode) {
            this.responseStatusCode = responseStatusCode;
            return this;
        }

        public Builder requiredHttpHeader(Header requiredHttpHeader) {
            this.requiredHttpHeader = requiredHttpHeader;
            return this;
        }

        public MockWebserviceProvider build() throws IOException {
            return new MockWebserviceProvider(path, ssl, clientAuth, responseBody, responseContentType, responseDelayMs, responseStatusCode, requiredHttpHeader);
        }
    }
}