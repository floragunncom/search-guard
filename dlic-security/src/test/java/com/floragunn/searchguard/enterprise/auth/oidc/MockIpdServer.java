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

package com.floragunn.searchguard.enterprise.auth.oidc;

import static com.floragunn.searchguard.enterprise.auth.oidc.CxfTestTools.toJson;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.apache.http.ExceptionLogger;
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
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.test.helper.network.SocketUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;

public class MockIpdServer implements Closeable {
    private final static Logger log = LogManager.getLogger(MockIpdServer.class);

    final static String CTX_DISCOVER = "/discover";
    final static String CTX_KEYS = "/api/oauth/keys";
    final static String CTX_TOKEN = "/token";
    final static String CTX_USERINFO = "/userinfo";

    public static Builder forKeySet(JsonWebKeys jwks) {
        return new Builder(jwks);
    }

    private HttpServer httpServer;
    private int port;
    private String uri;
    private boolean requireTlsClientCertAuth;
    private String requireTlsClientCertFingerprint;
    private JsonWebKeys jwks;
    private boolean requireValidCodes = true;
    private boolean requirePkce = false;

    private Map<String, AuthCodeContext> validCodes = new ConcurrentHashMap<>();
    private Map<String, Map<String, Object>> accessTokenToUserInfoMap = new ConcurrentHashMap<>();

    private Header requiredHttpHeader;
    private TLSConfig tlsConfig;

    private MockIpdServer() {

    }

    private void start() throws IOException {

        int i = 0;

        for (;;) {
            try {
                start(SocketUtils.findAvailableTcpPort());
                return;
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

    private void start(int port) throws IOException {
        this.port = port;
        this.uri = (tlsConfig != null ? "https" : "http") + "://localhost:" + port;

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
        }).registerHandler(CTX_USERINFO, new HttpRequestHandler() {

            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {

                handleUserInfoRequest(request, response, context);

            }
        }).setExceptionLogger(new ExceptionLogger() {
            @Override
            public void log(Exception ex) {
                if (ex instanceof org.apache.http.ConnectionClosedException) {
                    log.debug("Error in MockIdpServer", ex);                                        
                } else {
                    log.error("Error in MockIdpServer", ex);                    
                }
            }            
        });

        if (tlsConfig != null) {
            serverBootstrap = serverBootstrap.setSslContext(createSSLContext()).setSslSetupHandler(new SSLServerSetupHandler() {

                @Override
                public void initialize(SSLServerSocket socket) throws SSLException {
                    socket.setNeedClientAuth(requireTlsClientCertAuth);
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
        if (!checkAccess(request, response, context)) {
            return;
        }

        response.setStatusCode(200);
        response.setHeader("Cache-Control", "public, max-age=31536000");
        response.setEntity(new StringEntity(DocNode
                .of("jwks_uri", uri + CTX_KEYS, "issuer", uri, "unknownPropertyToBeIgnored", 42, "token_endpoint", uri + CTX_TOKEN,
                        "authorization_endpoint", uri + "/auth", "end_session_endpoint", uri + "/logout", "userinfo_endpoint", uri + CTX_USERINFO)
                .toJsonString(), ContentType.APPLICATION_JSON));
    }

    protected void handleKeysRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        if (!checkAccess(request, response, context)) {
            return;
        }

        response.setStatusCode(200);
        response.setEntity(new StringEntity(toJson(jwks)));
    }

    protected void handleTokenRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        try {
            if (!checkAccess(request, response, context)) {
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
            String accessToken;

            if (requireValidCodes) {
                AuthCodeContext authCodeContext = this.validCodes.remove(code);

                if (authCodeContext == null) {
                    response.setStatusCode(400);
                    response.setEntity(new StringEntity("Invalid code " + code));
                    return;
                }

                if (authCodeContext.codeChallenge != null) {
                    String codeVerifier = entityParams.get("code_verifier");
                    String hashed = applySHA256(codeVerifier);

                    if (!hashed.equals(authCodeContext.codeChallenge)) {
                        response.setStatusCode(400);
                        response.setEntity(new StringEntity(
                                "Invalid code_challenge " + authCodeContext.codeChallenge + "; expected: " + hashed + " (" + codeVerifier + ")"));
                        return;
                    }
                }

                idToken = authCodeContext.userJwt;
                accessToken = RandomStringUtils.randomAlphabetic(20);

                if (authCodeContext.userInfo != null) {
                    accessTokenToUserInfoMap.put(accessToken, authCodeContext.userInfo);
                }

                String oldRedirectUri = authCodeContext.redirectUri;
                String currentRedirectUri = entityParams.get("redirect_uri");

                System.out.println("redirect uri: " + oldRedirectUri + "; " + currentRedirectUri);

                if (!oldRedirectUri.equals(currentRedirectUri)) {
                    response.setStatusCode(400);
                    response.setEntity(new StringEntity("Invalid redirect_uri " + currentRedirectUri + "; expected: " + oldRedirectUri));
                    return;
                }
            } else {
                idToken = TestJwts.MC_COY_SIGNED_OCT_1;
                accessToken = "dummy_access_token";
            }

            response.setStatusCode(200);

            Map<String, Object> responseBody = ImmutableMap.of("access_token", accessToken, "token_type", "bearer", "expires_in", 3600, "scope",
                    "profile app:read app:write", "id_token", idToken);

            response.setEntity(new StringEntity(DocWriter.json().writeAsString(responseBody), ContentType.APPLICATION_JSON));
        } catch (Exception e) {
            log.error("Error in handleTokenRequest()", e);
            response.setStatusCode(500);
            response.setEntity(new StringEntity(e.toString()));
        }
    }

    protected void handleUserInfoRequest(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        if (!checkAccess(request, response, context)) {
            return;
        }

        if (!"POST".equalsIgnoreCase(request.getRequestLine().getMethod()) && !"GET".equalsIgnoreCase(request.getRequestLine().getMethod())) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Not a GET or POST request"));
            return;
        }

        if (request.getFirstHeader("Authorization") == null) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Authorization header is missing"));
            return;
        }

        String authorization = request.getFirstHeader("Authorization").getValue();

        if (!authorization.toLowerCase().startsWith("bearer ")) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Needs to use bearer authorization"));
            return;
        }

        String accessToken = authorization.substring("bearer ".length());

        Map<String, Object> userInfo = accessTokenToUserInfoMap.get(accessToken);

        if (userInfo == null) {
            response.setStatusCode(400);
            response.setEntity(new StringEntity("Invalid access token " + accessToken));
            return;
        }

        response.setEntity(new StringEntity(DocWriter.json().writeAsString(userInfo), ContentType.APPLICATION_JSON));
    }

    private SSLContext createSSLContext() {
        return tlsConfig.getUnrestrictedSslContext();
    }

    private boolean checkAccess(HttpRequest request, HttpResponse response, HttpContext context)
            throws UnsupportedEncodingException, SSLPeerUnverifiedException {

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
        }

        if (requireTlsClientCertFingerprint != null) {
            MockIpdServer.SSLTestHttpServerConnection connection = (MockIpdServer.SSLTestHttpServerConnection) ((HttpCoreContext) context)
                    .getConnection();

            X509Certificate peerCert = (X509Certificate) connection.getPeerCertificates()[0];

            try {
                String sha256Fingerprint = Hashing.sha256().hashBytes(peerCert.getEncoded()).toString();

                if (!requireTlsClientCertFingerprint.equals(sha256Fingerprint)) {
                    response.setStatusCode(401);
                    response.setEntity(new StringEntity("Client certificate is not allowed: " + sha256Fingerprint + "\n" + peerCert));
                    return false;
                }

            } catch (CertificateEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    private String applySHA256(String string) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(string.getBytes(StandardCharsets.US_ASCII));

            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
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

    public String handleSsoGetRequestURI(String ssoLocation, String userJwt) throws URISyntaxException, UnsupportedEncodingException {
        return handleSsoGetRequestURI(ssoLocation, userJwt, null);
    }

    public String handleSsoGetRequestURI(String ssoLocation, String userJwt, Map<String, Object> userInfo)
            throws URISyntaxException, UnsupportedEncodingException {
        Map<String, String> ssoParams = getUriParams(ssoLocation);

        String scope = ssoParams.get("scope");

        Assert.assertNotNull(ssoLocation, scope);
        Assert.assertTrue(ssoLocation, scope.contains("openid"));

        String state = ssoParams.get("state");
        Assert.assertNotNull(ssoLocation, state);

        String redirectUri = ssoParams.get("redirect_uri");
        Assert.assertNotNull(ssoLocation, redirectUri);

        String codeChallenge = ssoParams.get("code_challenge");
        String codeChallengeMethod = ssoParams.get("code_challenge_method");

        String code = RandomStringUtils.randomAlphanumeric(8);
        AuthCodeContext authCodeContext = new AuthCodeContext();
        authCodeContext.userJwt = userJwt;
        authCodeContext.redirectUri = redirectUri;
        authCodeContext.codeChallenge = codeChallenge;
        authCodeContext.userInfo = userInfo;

        if (codeChallenge != null) {
            Assert.assertEquals(ssoLocation, "S256", codeChallengeMethod);
        } else if (requirePkce) {
            return null;
        }

        validCodes.put(code, authCodeContext);

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

    public static class Builder {
        private MockIpdServer mockIdpServer = new MockIpdServer();

        public Builder(JsonWebKeys jwks) {
            mockIdpServer.jwks = jwks;
        }

        public Builder useCustomTlsConfig(TLSConfig tlsConfig) {
            mockIdpServer.tlsConfig = tlsConfig;
            return this;
        }

        public Builder requirePkce(boolean requirePkce) {
            mockIdpServer.requirePkce = requirePkce;
            return this;
        }

        public Builder requireTlsClientCertAuth() {
            mockIdpServer.requireTlsClientCertAuth = true;
            return this;
        }

        public Builder acceptOnlyRequestsWithHeader(Header header) {
            mockIdpServer.requiredHttpHeader = header;
            return this;
        }

        public Builder requireValidCodes(boolean requireValidCodes) {
            mockIdpServer.requireValidCodes = requireValidCodes;
            return this;
        }

        public Builder requireTlsClientCertFingerprint(String fingerprint) {
            mockIdpServer.requireTlsClientCertAuth = true;
            mockIdpServer.requireTlsClientCertFingerprint = fingerprint;
            return this;
        }

        public MockIpdServer start() throws IOException {
            mockIdpServer.start();
            return mockIdpServer;
        }
    }

    private static class AuthCodeContext {
        private String userJwt;
        private String redirectUri;
        private String codeChallenge;
        private Map<String, Object> userInfo;

    }
}
