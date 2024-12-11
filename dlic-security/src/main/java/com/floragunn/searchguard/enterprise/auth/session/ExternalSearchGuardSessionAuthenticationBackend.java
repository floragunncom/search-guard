/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.session;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.floragunn.codova.documents.Parser;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.net.ProxyConfig;
import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.PrivilegedCode;
import com.floragunn.searchsupport.PrivilegedCode.PrivilegedSupplierThrowing2;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.metrics.Meter;

public class ExternalSearchGuardSessionAuthenticationBackend implements AuthenticationBackend, AutoCloseable {
    private final static Logger log = LogManager.getLogger(ExternalSearchGuardSessionAuthenticationBackend.class);

    private final ComponentState componentState = new ComponentState(0, "authentication_backend", TYPE,
            ExternalSearchGuardSessionAuthenticationBackend.class).initialized().requiresEnterpriseLicense();

    private final ImmutableList<HttpHost> hosts;
    private final TLSConfig tlsConfig;
    private final ProxyConfig proxyConfig;
    private final int maxConnectionsPerHost;
    private PoolingHttpClientConnectionManager connectionManager;
    private CloseableHttpClient httpClient;
    private Thread idleConnectionMonitorThread;

    public ExternalSearchGuardSessionAuthenticationBackend(Map<String, Object> config, ConfigurationRepository.Context context)
            throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors, context);

        ImmutableList<URI> hostUris = vNode.get("hosts").required().asList().minElements(1).ofURIs();
        this.tlsConfig = vNode.get("tls").by((Parser<TLSConfig, Parser.Context>) TLSConfig::parse);
        this.proxyConfig = vNode.get("proxy").by((ValidatingFunction<DocNode, ProxyConfig>) ProxyConfig::parse);
        this.maxConnectionsPerHost = vNode.get("connection_pool.max_connections_per_host").withDefault(6).asInt();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        this.hosts = hostUris.map((uri) -> new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()));

        if (this.tlsConfig != null) {
            Registry<ConnectionSocketFactory> connectionSocketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", this.tlsConfig.toSSLConnectionSocketFactory()).build();
            this.connectionManager = new PoolingHttpClientConnectionManager(connectionSocketFactoryRegistry);
        } else {
            this.connectionManager = new PoolingHttpClientConnectionManager();
        }

        this.connectionManager.setMaxTotal(maxConnectionsPerHost * hosts.size());
        this.connectionManager.setDefaultMaxPerRoute(maxConnectionsPerHost);
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager);

        if (this.proxyConfig != null) {
            this.proxyConfig.apply(builder);
        }

        this.httpClient = builder.build();

        this.idleConnectionMonitorThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (connectionManager != null) {
                        synchronized (this) {
                            wait(5000);

                            if (connectionManager != null) {
                                connectionManager.closeExpiredConnections();
                                connectionManager.closeIdleConnections(60, TimeUnit.SECONDS);
                            }
                        }
                    }
                } catch (InterruptedException ex) {
                    // terminate
                }
            }
        };
    }

    @Override
    public CompletableFuture<AuthCredentials> authenticate(AuthCredentials authCredentials, Meter meter)
            throws AuthenticatorUnavailableException, CredentialsException {
        if (!(authCredentials.getNativeCredentials() instanceof String)) {
            throw new AuthenticatorUnavailableException("Configuration Error", TYPE + " must be combined with a JWT authentication frontend");
        }

        String jwt = (String) authCredentials.getNativeCredentials();
        HttpGet httpGet = new HttpGet("/_searchguard/auth/session/extended");
        httpGet.addHeader("Authorization", "bearer " + jwt);

        return PrivilegedCode.execute(
                (PrivilegedSupplierThrowing2<CompletableFuture<AuthCredentials>, CredentialsException, AuthenticatorUnavailableException>) () -> {
                    try (CloseableHttpResponse response = executeWithRetry(httpGet, meter)) {
                        if (response.getStatusLine().getStatusCode() == 401) {
                            throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false, "Failed to authenticate with JWT",
                                    ImmutableMap.of("response_status", response.getStatusLine())));
                        }

                        if (response.getStatusLine().getStatusCode() != 200) {
                            throw new AuthenticatorUnavailableException("Authentication failed", "session cluster is unavailable")
                                    .details("response_status", response.getStatusLine());
                        }

                        DocNode responseBody = DocNode.parse(Format.JSON).from(response.getEntity().getContent());
                        DocNode user = responseBody.getAsNode("user");

                        if (user == null) {
                            throw new AuthenticatorUnavailableException("Authentication failed", "session cluster returned invalid result")
                                    .details("response_status", response.getStatusLine(), "response_body", responseBody.toJsonString());
                        }

                        String userName = user.getAsString("name");

                        if (userName == null) {
                            throw new AuthenticatorUnavailableException("Authentication failed", "session cluster returned invalid result")
                                    .details("response_status", response.getStatusLine(), "response_body", responseBody.toJsonString());
                        }

                        List<String> backendRoles = user.hasNonNull("backend_roles") ? user.getAsListOfStrings("backend_roles")
                                : ImmutableList.empty();
                        List<String> searchGuardRoles = user.hasNonNull("search_guard_roles") ? user.getAsListOfStrings("search_guard_roles")
                                : ImmutableList.empty();
                        Map<String, Object> attributes = user.hasNonNull("attributes") ? user.getAsNode("attributes").toMap() : ImmutableMap.empty();

                        return CompletableFuture.completedFuture(authCredentials.with(AuthCredentials.forUser(userName).backendRoles(backendRoles)
                                .searchGuardRoles(searchGuardRoles).attributes(attributes).build()));

                    } catch (IOException e) {
                        throw new AuthenticatorUnavailableException("Authentication failed", "session cluster is unavailable", e);
                    } catch (DocumentParseException e) {
                        throw new AuthenticatorUnavailableException("Authentication failed", "session cluster returned invalid result", e);
                    }
                }, CredentialsException.class, AuthenticatorUnavailableException.class);
    }

    @Override
    public void close() {
        if (this.httpClient != null) {
            try {
                this.httpClient.close();
            } catch (IOException e) {
                log.warn("Error while closing client", e);
            }
        }

        if (this.connectionManager != null) {
            this.connectionManager.close();
            this.connectionManager = null;
        }

        if (this.idleConnectionMonitorThread != null) {
            this.idleConnectionMonitorThread.interrupt();
            this.idleConnectionMonitorThread = null;
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private CloseableHttpResponse executeWithRetry(HttpGet httpGet, Meter meter) throws ClientProtocolException, IOException {
        int hostCount = this.hosts.size();

        if (hostCount == 1) {
            try (Meter subMeter = meter.detail(this.hosts.get(0).toString())) {
                return this.httpClient.execute(this.hosts.get(0), httpGet);
            }
        } else {
            int first = ThreadLocalRandom.current().nextInt(hostCount);

            for (int i = 0; i < hostCount - 1; i++) {
                int hostIndex = (i + first) % hostCount;

                try (Meter subMeter = meter.detail(this.hosts.get(hostIndex).toString())) {
                    CloseableHttpResponse response = this.httpClient.execute(this.hosts.get(hostIndex), httpGet);

                    if (response.getStatusLine().getStatusCode() < 500) {
                        return response;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Got status {} while executing {}. Retrying with next host", response.getStatusLine().getStatusCode(), httpGet);
                        }

                        response.close();
                    }
                } catch (IOException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Got exception while executing " + httpGet + ". Retrying with next host", e);
                    }
                }
            }

            return this.httpClient.execute(this.hosts.get(first == 0 ? hostCount - 1 : first - 1), httpGet);
        }
    }

    public static final String TYPE = "external_session";

    public static TypedComponent.Info<AuthenticationBackend> INFO = new TypedComponent.Info<AuthenticationBackend>() {

        @Override
        public Class<AuthenticationBackend> getType() {
            return AuthenticationBackend.class;
        }

        @Override
        public String getName() {
            return TYPE;
        }

        @Override
        public Factory<AuthenticationBackend> getFactory() {
            return ExternalSearchGuardSessionAuthenticationBackend::new;
        }
    };

}
