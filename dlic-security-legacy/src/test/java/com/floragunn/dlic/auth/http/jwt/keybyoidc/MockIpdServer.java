package com.floragunn.dlic.auth.http.jwt.keybyoidc;
/*
 * Copyright 2016-2020 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */



import static com.floragunn.dlic.auth.http.jwt.keybyoidc.CxfTestTools.toJson;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.apache.http.Header;
import org.apache.http.HttpConnectionFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ContentLengthStrategy;
import org.apache.http.entity.ContentType;
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
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.google.common.collect.ImmutableMap;

public class MockIpdServer implements Closeable {
    private final static Logger log = LogManager.getLogger(MockIpdServer.class);

    final static String CTX_DISCOVER = "/discover";
    final static String CTX_KEYS = "/api/oauth/keys";
    final static String CTX_TOKEN = "/token";

    private final HttpServer httpServer;
    private final int port;
    private final String uri;
    private final boolean ssl;
    private final JsonWebKeys jwks;
    private boolean requireValidCodes = true;

    private Map<String, String> validCodes = new ConcurrentHashMap<>();
    private Map<String, String> validCodesToRedirectUri = new ConcurrentHashMap<>();

    private Header requiredHttpHeader;

    public static MockIpdServer start(JsonWebKeys jwks) throws IOException {

        int i = 0;

        for (;;) {
            try {
                return new MockIpdServer(jwks);
            } catch (BindException e) {
                if (i >= 10) {
                    throw e;
                } else {
                    i++;
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                }
            }
        }
    }

    MockIpdServer(JsonWebKeys jwks) throws IOException {
        this(jwks, SocketUtils.findAvailableTcpPort(), false);
    }

    MockIpdServer(JsonWebKeys jwks, int port, boolean ssl) throws IOException {
        this.port = port;
        this.uri = (ssl ? "https" : "http") + "://localhost:" + port;
        this.ssl = ssl;
        this.jwks = jwks;

        ServerBootstrap serverBootstrap = ServerBootstrap.bootstrap().setListenerPort(port).registerHandler(CTX_DISCOVER, new HttpRequestHandler() {

            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {

                handleDiscoverRequest(request, response, context);

            }
        }).registerHandler(CTX_KEYS, new HttpRequestHandler() {

            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {

                handleKeysRequest(request, response, context);

            }
        }).registerHandler(CTX_TOKEN, new HttpRequestHandler() {

            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {

                handleTokenRequest(request, response, context);

            }
        });

        if (ssl) {
            serverBootstrap = serverBootstrap.setSslContext(createSSLContext()).setSslSetupHandler(new SSLServerSetupHandler() {

                @Override
                public void initialize(SSLServerSocket socket) throws SSLException {
                    socket.setNeedClientAuth(true);
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

        httpServer.start();
    }

    public MockIpdServer acceptOnlyRequestsWithHeader(Header header) {
        this.requiredHttpHeader = header;
        return this;
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

    public URI getDiscoverUri() {
        return URI.create(uri + CTX_DISCOVER);
    }

    public int getPort() {
        return port;
    }

    protected void handleDiscoverRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {

        if (!checkClientAddress(request, response, context)) {
            return;
        }

        response.setStatusCode(200);
        response.setHeader("Cache-Control", "public, max-age=31536000");
        response.setEntity(new StringEntity("{\"jwks_uri\": \"" + uri + CTX_KEYS + "\",\n" + "\"issuer\": \"" + uri
                + "\", \"unknownPropertyToBeIgnored\": 42, \"token_endpoint\": \"" + uri + CTX_TOKEN + "\", \"authorization_endpoint\": \"" + uri
                + "/auth\"}"));
    }

    protected void handleKeysRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        if (!checkClientAddress(request, response, context)) {
            return;
        }

        response.setStatusCode(200);
        response.setEntity(new StringEntity(toJson(jwks)));
    }

    protected void handleTokenRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        if (!checkClientAddress(request, response, context)) {
            return;
        }

        if (!"POST".equalsIgnoreCase(request.getRequestLine().getMethod())) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Not a POST request"));
            return;
        }

        if (request.getFirstHeader("Content-Type") == null) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Content-Type header is missing"));
            return;
        }

        if (!request.getFirstHeader("Content-Type").getValue().toLowerCase().startsWith("application/x-www-form-urlencoded")) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Content-Type is not application/x-www-form-urlencoded"));
            return;
        }

        if (!(request instanceof HttpEntityEnclosingRequest)) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Missing entity"));
            return;
        }

        HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
        String entityBody = EntityUtils.toString(entity);

        Map<String, String> entityParams = getFormUrlEncodedValues(entityBody);

        log.info("Got entity params: " + entityParams + "; " + entityBody);

        if (!entityParams.containsKey("grant_type")) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Missing grant_type"));
            return;
        }

        String code = entityParams.get("code");
        String idToken;

        if (requireValidCodes) {
            idToken = this.validCodes.remove(code);

            if (idToken == null) {
                response.setStatusCode(400);
                response.setEntity(new StringEntity("Invalid code " + code));
                return;
            }

            String oldRedirectUri = this.validCodesToRedirectUri.remove(code);
            String currentRedirectUri = entityParams.get("redirect_uri");

            //System.out.println("redirect uri: " + oldRedirectUri + "; " + currentRedirectUri);
            
            if (!oldRedirectUri.equals(currentRedirectUri)) {
                response.setStatusCode(400);
                response.setEntity(new StringEntity("Invalid redirect_uri " + currentRedirectUri + "; expected: " + oldRedirectUri));
                return;
            }
        } else {
            idToken = TestJwts.MC_COY_SIGNED_OCT_1;
        }

        response.setStatusCode(200);

        Map<String, Object> responseBody = ImmutableMap.of("access_token", "totototototo", "token_type", "bearer", "expires_in", 3600, "scope",
                "profile app:read app:write", "id_token", idToken);

        response.setEntity(new StringEntity(DocWriter.json().writeAsString(responseBody), ContentType.APPLICATION_JSON));
    }

    private SSLContext createSSLContext() {
        if (!this.ssl) {
            return null;
        }

        try {
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            final KeyStore trustStore = KeyStore.getInstance("JKS");
            InputStream trustStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("jwt/truststore.jks").toFile());
            trustStore.load(trustStream, "changeit".toCharArray());
            tmf.init(trustStore);

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            final KeyStore keyStore = KeyStore.getInstance("JKS");
            InputStream keyStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("jwt/node-0-keystore.jks").toFile());

            keyStore.load(keyStream, "changeit".toCharArray());
            kmf.init(keyStore, "changeit".toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return sslContext;
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkClientAddress(HttpRequest request, HttpResponse response, HttpContext context) throws UnsupportedEncodingException {

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

    public String handleSsoGetRequestURI(String ssoLocation, String userJwt) throws URISyntaxException, UnsupportedEncodingException {
        Map<String, String> ssoParams = getUriParams(ssoLocation);

        String scope = ssoParams.get("scope");

        Assert.assertNotNull(ssoLocation, scope);
        Assert.assertTrue(ssoLocation, scope.contains("openid"));

        String state = ssoParams.get("state");
        Assert.assertNotNull(ssoLocation, state);

        String redirectUri = ssoParams.get("redirect_uri");
        Assert.assertNotNull(ssoLocation, redirectUri);

        String code = RandomStringUtils.randomAlphanumeric(8);
        validCodes.put(code, userJwt);
        validCodesToRedirectUri.put(code, redirectUri);

        URIBuilder uriBuilder = new URIBuilder(redirectUri);
        uriBuilder.addParameter("code", code);
        uriBuilder.addParameter("state", state);

        return uriBuilder.build().toASCIIString();
    }

    private Map<String, String> getUriParams(String uriString) {
        URI uri = URI.create(uriString);

        return getFormUrlEncodedValues(uri.getRawQuery());
    }

    private Map<String, String> getFormUrlEncodedValues(String formUrlencoded) {
        List<NameValuePair> nameValuePairs = URLEncodedUtils.parse(formUrlencoded, Charset.forName("utf-8"));

        HashMap<String, String> result = new HashMap<>(nameValuePairs.size());

        for (NameValuePair nameValuePair : nameValuePairs) {
            result.put(nameValuePair.getName(), nameValuePair.getValue());
        }

        return result;

    }

    public boolean isRequireValidCodes() {
        return requireValidCodes;
    }

    public void setRequireValidCodes(boolean requireValidCodes) {
        this.requireValidCodes = requireValidCodes;
    }
}
