package com.floragunn.signals;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.apache.http.HttpConnectionFactory;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentLengthStrategy;
import org.apache.http.impl.ConnSupport;
import org.apache.http.impl.DefaultBHttpServerConnection;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.SSLServerSetupHandler;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.io.HttpMessageParserFactory;
import org.apache.http.io.HttpMessageWriterFactory;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public class MockWebserviceProvider implements Closeable {

    private HttpServer httpServer;
    private final int port;
    private final String uri;
    private final boolean ssl;
    private int responseStatusCode = 200;
    private byte[] responseBody = "Mockery".getBytes();
    private String responseContentType = "text/plain";
    private String lastRequestBody;
    private final AtomicInteger requestCount = new AtomicInteger();
    private long responseDelayMs = 0;

    MockWebserviceProvider(String path) throws IOException {
        this(path, SocketUtils.findAvailableTcpPort());
    }

    MockWebserviceProvider(String path, byte[] body, String contentType) throws IOException {
        this(path, SocketUtils.findAvailableTcpPort());
        responseContentType = contentType;
        responseBody = body;
    }

    MockWebserviceProvider(String path, boolean ssl) throws IOException {
        this(path, SocketUtils.findAvailableTcpPort(), ssl, ssl);
    }

    MockWebserviceProvider(String path, boolean ssl, boolean clientAuth) throws IOException {
        this(path, SocketUtils.findAvailableTcpPort(), ssl, clientAuth);
    }

    MockWebserviceProvider(String path, int port) throws IOException {
        this(path, port, false, false);
    }

    MockWebserviceProvider(String pathPattern, int port, boolean ssl, boolean clientAuth) throws IOException {
        this.port = port;
        this.uri = buildUri(pathPattern, ssl, port);
        this.ssl = ssl;

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
                
        requestCount.incrementAndGet();
    }

    private SSLContext createSSLContext() {
        if (!this.ssl) {
            return null;
        }

        try {
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            final KeyStore trustStore = KeyStore.getInstance("JKS");
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
}