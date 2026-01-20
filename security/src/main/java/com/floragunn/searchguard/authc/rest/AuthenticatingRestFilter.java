/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.rest;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import com.floragunn.searchguard.ssl.http.AttributedHttpRequest;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.diag.DiagnosticContext;

import static com.floragunn.searchguard.ssl.util.SSLCertificateHelper.validate;

public class AuthenticatingRestFilter implements ComponentStateProvider {

    private static final Logger log = LogManager.getLogger(AuthenticatingRestFilter.class);

    private final AuditLog auditLog;
    private final ThreadContext threadContext;
    private final PrincipalExtractor principalExtractor;
    private final Settings settings;
    private final Path configPath;
    private final DiagnosticContext diagnosticContext;
    private final AdminDNs adminDns;
    private final ComponentState componentState = new ComponentState(1, "authc", "rest_filter");
    private final ThreadPool threadPool;

    private volatile RestAuthenticationProcessor authenticationProcessor;

    public AuthenticatingRestFilter(ConfigurationRepository configurationRepository, SearchGuardModulesRegistry modulesRegistry, AdminDNs adminDns,
            BlockedIpRegistry blockedIpRegistry, BlockedUserRegistry blockedUserRegistry, AuditLog auditLog, ThreadPool threadPool,
            PrincipalExtractor principalExtractor, PrivilegesEvaluator privilegesEvaluator, Settings settings, Path configPath,
            DiagnosticContext diagnosticContext) {
        this.adminDns = adminDns;
        this.auditLog = auditLog;
        this.threadPool = threadPool;
        this.threadContext = threadPool.getThreadContext();
        this.principalExtractor = principalExtractor;
        this.settings = settings;
        this.configPath = configPath;
        this.diagnosticContext = diagnosticContext;

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<RestAuthcConfig> config = configMap.get(CType.AUTHC);

                if (config != null && config.getCEntry("default") != null) {
                    RestAuthenticationProcessor authenticationProcessor = new RestAuthenticationProcessor.Default(config.getCEntry("default"),
                            modulesRegistry, adminDns, blockedIpRegistry, blockedUserRegistry, auditLog, threadPool, privilegesEvaluator);

                    AuthenticatingRestFilter.this.authenticationProcessor = authenticationProcessor;

                    componentState.replacePartsWithType("config", config.getComponentState());
                    componentState.replacePartsWithType("rest_authentication_processor", authenticationProcessor.getComponentState());
                    componentState.updateStateFromParts();

                    if (log.isDebugEnabled()) {
                        log.debug("New configuration:\n" + config.getCEntry("default").toYamlString());
                    }
                } else {
                    componentState.setState(State.SUSPENDED, "no_configuration");
                }
            }
        });

    }

    public HttpServerTransport.Dispatcher wrap(HttpServerTransport.Dispatcher original) {
        return new AuthenticatingRestHandler(new ExecuteInNettyEventLoopDispatcher(original, threadPool));
    }

    public RestAuthenticationProcessor getAuthenticationProcessor() {
        return authenticationProcessor;
    }

    class AuthenticatingRestHandler implements HttpServerTransport.Dispatcher {
        private final HttpServerTransport.Dispatcher original;

        AuthenticatingRestHandler(HttpServerTransport.Dispatcher original) {
            this.original = original;
        }

        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            handleRequest(request, channel);
        }

        @Override
        public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
            handleRequest(channel.request(), channel); //TODO log cause
        }

        private void handleRequest(RestRequest request, RestChannel channel) {
            org.apache.logging.log4j.ThreadContext.clearAll();
            diagnosticContext.traceActionStack(request.getHttpRequest().method() + " " + request.getHttpRequest().uri());

            if (!checkRequest(request, channel)) {
                return;
            }

            if (isAuthcRequired(request)) {
                String sslPrincipal = threadContext.getTransient(SSLConfigConstants.SG_SSL_PRINCIPAL);

                // Admin Cert authentication works also without a valid configuration; that's why it is not done inside of AuthenticationProcessor
                if (adminDns.isAdminDN(sslPrincipal)) {
                    // PKI authenticated REST call

                    User user = new User(sslPrincipal, AuthDomainInfo.TLS_CERT);
                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                    auditLog.logSucceededLogin(user, true, null, request);
                    original.dispatchRequest(request, channel, threadContext);
                    return;
                }

                if (authenticationProcessor == null) {
                    log.error("Not yet initialized (you may need to run sgctl)");
                    channel.sendResponse(new RestResponse(RestStatus.SERVICE_UNAVAILABLE,
                            "Search Guard not initialized (SG11). See https://docs.search-guard.com/latest/sgctl"));
                    return;
                }
                RestChannel channelWrapper = new SendOnceRestChannelWrapper(channel);
                authenticationProcessor.authenticate(request, channelWrapper, (result) -> {
                    if (authenticationProcessor.isDebugEnabled() && DebugApi.PATH.equals(request.path())) {
                        sendDebugInfo(channelWrapper, result);
                        return;
                    }

                    if (result.getStatus() == AuthcResult.Status.PASS) {
                        // make it possible to filter logs by username
                        threadContext.putTransient(ConfigConstants.SG_USER, result.getUser());
                        org.apache.logging.log4j.ThreadContext.clearAll();
                        org.apache.logging.log4j.ThreadContext.put("user", result.getUser() != null ? result.getUser().getName() : null);

                        try {
                            original.dispatchRequest(request, channel, threadContext);
                        } catch (Exception e) {
                            log.error("Error in " + original, e);
                            try {
                                channelWrapper.sendResponse(new RestResponse(channel, e));
                            } catch (IOException e1) {
                                log.error(e1);
                            }
                        }
                    } else {
                        org.apache.logging.log4j.ThreadContext.remove("user");

                        if (result.getRestStatus() != null) {

                            final RestResponse response = createResponse(request, result);

                            if (!result.getHeaders().isEmpty()) {
                                result.getHeaders().forEach((k, v) -> v.forEach((e) -> response.addHeader(k, e)));
                            }

                            channelWrapper.sendResponse(response);
                        }
                    }
                }, (e) -> {
                    try {
                        channelWrapper.sendResponse(new RestResponse(channelWrapper, e));
                    } catch (IOException e1) {
                        log.error(e1);
                    }
                });
            } else {
                original.dispatchRequest(request, channel, threadContext);
            }
        }

        private boolean isAuthcRequired(RestRequest request) {
            return request.method() != Method.OPTIONS && !"/_searchguard/license".equals(request.path())
                    && !"/_searchguard/license".equals(request.path()) && !"/_searchguard/health".equals(request.path())
                    && !("/_searchguard/auth/session".equals(request.path()) && request.method() == Method.POST);
        }

        private boolean checkRequest(RestRequest request, RestChannel channel) {

            threadContext.putTransient(ConfigConstants.SG_ORIGIN, Origin.REST.toString());

            if (containsBadHeader(request, threadContext)) {
                final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                log.error(exception);
                auditLog.logBadHeaders(request);
                try {
                    channel.sendResponse(new RestResponse(channel, RestStatus.FORBIDDEN, exception));
                } catch (IOException e) {
                    log.error(e,e);
                    channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                }
                return false;
            }

            final SslHandler sslhandler = Optional.ofNullable(request.getHttpRequest())
                    .filter(AttributedHttpRequest.class::isInstance)
                    .map(AttributedHttpRequest.class::cast)
                    .flatMap(attributedRequest -> Optional.ofNullable(attributedRequest.getSslHandler()))
                    .orElse(null);
            if (sslhandler != null) {
                final SSLEngine engine = sslhandler.engine();
                final SSLSession session = engine.getSession();
                if (engine.getNeedClientAuth() || engine.getWantClientAuth()) {
                    boolean validationFailure = false;
                    try {
                        final Certificate[] certs = session.getPeerCertificates();
                        if (certs != null && certs.length > 0 && certs[0] instanceof X509Certificate) {
                            final X509Certificate[] x509Certs = Arrays.copyOf(certs, certs.length, X509Certificate[].class);
                            final SecurityManager securityManager = System.getSecurityManager();
                            if (securityManager != null) {
                                securityManager.checkPermission(new SpecialPermission());
                            }
                            validationFailure = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> !validate(x509Certs, settings, configPath));
                            if(validationFailure) {
                                throw new SSLPeerUnverifiedException("Unable to validate certificate (CRL)");
                            }
                            String principal = principalExtractor == null ? null : principalExtractor.extractPrincipal(x509Certs[0], PrincipalExtractor.Type.HTTP);
                            if (principal != null) {
                                threadContext.putTransient(SSLConfigConstants.SG_SSL_PRINCIPAL, principal);
                            }
                            Certificate[] localCerts = session.getLocalCertificates();
                            if (localCerts != null) {
                                threadContext.putTransient(SSLConfigConstants.SG_SSL_LOCAL_CERTIFICATES, Arrays.copyOf(localCerts, localCerts.length, X509Certificate[].class));
                            }
                            threadContext.putTransient(SSLConfigConstants.SG_SSL_PEER_CERTIFICATES, x509Certs.clone());
                            threadContext.putTransient(SSLConfigConstants.SG_SSL_PROTOCOL, session.getProtocol());
                            threadContext.putTransient(SSLConfigConstants.SG_SSL_CIPHER, session.getCipherSuite());
                        } else if (engine.getNeedClientAuth()) {
                            throw new ElasticsearchException("No client certificates found but such are needed (SG 9).");
                        }
                    } catch (SSLPeerUnverifiedException e) {
                        if (engine.getNeedClientAuth() || validationFailure) {
                            log.error("No ssl info", e);
                            auditLog.logSSLException(request, e);
                            try {
                                channel.sendResponse(new RestResponse(channel, RestStatus.FORBIDDEN, e));
                            } catch (IOException ex) {
                                log.error(e,e);
                                channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                            }
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        private void sendDebugInfo(RestChannel channel, AuthcResult authcResult) {
            RestResponse response = new RestResponse(authcResult.getRestStatus(), "application/json", authcResult.toPrettyJsonString());
            if (!authcResult.getHeaders().isEmpty()) {
                authcResult.getHeaders().forEach((k, v) -> v.forEach((e) -> response.addHeader(k, e)));
            }
            channel.sendResponse(response);
        }
    }

    public static RestResponse createUnauthorizedResponse(RestRequest request) {
        return createResponse(request, AuthcResult.stop(RestStatus.UNAUTHORIZED, ConfigConstants.UNAUTHORIZED));
    }

    public static RestResponse createResponse(RestRequest request, AuthcResult result) {
        final String statusMessage = result.getRestStatusMessage()==null?result.getRestStatus().name():result.getRestStatusMessage();
        final String accept = request.header("accept");

        if(accept == null || (!accept.startsWith("application/json") && !accept.startsWith("application/vnd.elasticsearch+json"))) {
            return new RestResponse(result.getRestStatus(), statusMessage);
        }

        return new RestResponse(result.getRestStatus(), "application/json", DocNode.of(//
                "status", result.getRestStatus().getStatus(), //
                "error.reason", statusMessage, //
                "error.type", "authentication_exception" //
        ).toJsonString());
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public static class DebugApi extends RestApi {
        public static final String PATH = "/_searchguard/auth/debug";

        public DebugApi() {
            name(PATH);
            handlesGet(PATH).with((r, c) -> {
                return (RestChannel channel) -> {
                    channel.sendResponse(new StandardResponse(404,
                            new StandardResponse.Error("The /_searchguard/auth/debug API is not enabled. See the sg_authc configuration."))
                                    .toRestResponse());
                };
            });
        }

    }

    private static boolean containsBadHeader(RestRequest request, ThreadContext threadContext) {
        for (String key : request.getHeaders().keySet()) {
            if (key != null && key.trim().toLowerCase().startsWith(ConfigConstants.SG_CONFIG_PREFIX)) {
                return true;
            }
        }
        for (final Map.Entry<String, String> header : threadContext.getHeaders().entrySet()) {
            if (header != null && header.getKey() != null && header.getKey().trim().toLowerCase().startsWith(ConfigConstants.SG_CONFIG_PREFIX)) {
                return true;
            }
        }
        return false;
    }
}
