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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLPeerUnverifiedException;

import io.netty.channel.EventLoop;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest.Method;
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
import com.floragunn.searchguard.ssl.util.SSLRequestHelper;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper.SSLInfo;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.diag.DiagnosticContext;

public class AuthenticatingRestFilter implements ComponentStateProvider {

    // TODO ES 9.1.x: better name for the interface is needed
    @FunctionalInterface
    public interface RequestAcceptor {
        void incomingRequest(HttpRequest httpRequest, HttpChannel httpChannel);
    }

    private static final Logger log = LogManager.getLogger(AuthenticatingRestFilter.class);

    private final AuditLog auditLog;
    private final ThreadContext threadContext;
    private final PrincipalExtractor principalExtractor;
    private final Settings settings;
    private final Path configPath;
    private final DiagnosticContext diagnosticContext;
    private final AdminDNs adminDns;
    private final ComponentState componentState = new ComponentState(1, "authc", "rest_filter");

    private volatile RestAuthenticationProcessor authenticationProcessor;

    public AuthenticatingRestFilter(ConfigurationRepository configurationRepository, SearchGuardModulesRegistry modulesRegistry, AdminDNs adminDns,
            BlockedIpRegistry blockedIpRegistry, BlockedUserRegistry blockedUserRegistry, AuditLog auditLog, ThreadPool threadPool,
            PrincipalExtractor principalExtractor, PrivilegesEvaluator privilegesEvaluator, Settings settings, Path configPath,
            DiagnosticContext diagnosticContext) {
        this.adminDns = adminDns;
        this.auditLog = auditLog;
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
        return original;
        // return new AuthenticatingRestHandler(original);
    }
    
    public RestAuthenticationProcessor getAuthenticationProcessor() {
        return authenticationProcessor;
    }

    public void authenticate(RequestAcceptor requestAcceptor, HttpRequest httpRequest, HttpChannel httpChannel, EventLoop eventLoop) {
        var handler = new AuthenticatingRestHandler(requestAcceptor, eventLoop);
        handler.handleRequest(httpRequest, httpChannel);
    }

    class AuthenticatingRestHandler  {
        private final RequestAcceptor original;
        private final EventLoop eventLoop;

        AuthenticatingRestHandler(RequestAcceptor original, EventLoop eventLoop) {
            this.original = original;
            this.eventLoop = eventLoop;
        }

        private void handleRequest(HttpRequest request, HttpChannel channel) {
            org.apache.logging.log4j.ThreadContext.clearAll();
            diagnosticContext.traceActionStack(request.method() + " " + request.rawPath());

            if (!checkRequest(request, channel)) {
                return;
            }

            if (isAuthcRequired(request)) {
                String sslPrincipal = threadContext.getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

                // Admin Cert authentication works also without a valid configuration; that's why it is not done inside of AuthenticationProcessor
                if (adminDns.isAdminDN(sslPrincipal)) {
                    // PKI authenticated REST call

                    User user = new User(sslPrincipal, AuthDomainInfo.TLS_CERT);
                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                    // TODO ES 9.1.x restore auditlog call
//                    auditLog.logSucceededLogin(user, true, null, request);
                    original.incomingRequest(request, channel);
                    return;
                }

                if (authenticationProcessor == null) {
                    log.error("Not yet initialized (you may need to run sgctl)");
                    createAndSendResponse(channel, request, RestStatus.SERVICE_UNAVAILABLE, "text/plain); charset=UTF-8",
                            "Search Guard not initialized (SG11). See https://docs.search-guard.com/latest/sgctl");
                    return;
                }
                authenticationProcessor.authenticate(request, channel, (result) -> {
                    if (authenticationProcessor.isDebugEnabled() && DebugApi.PATH.equals(request.rawPath())) {
                        sendDebugInfo(channel, request, result);
                        return;
                    }

                    if (result.getStatus() == AuthcResult.Status.PASS) {
                        // make it possible to filter logs by username
                        threadContext.putTransient(ConfigConstants.SG_USER, result.getUser());
                        org.apache.logging.log4j.ThreadContext.clearAll();
                        org.apache.logging.log4j.ThreadContext.put("user", result.getUser() != null ? result.getUser().getName() : null);

                        try {
                            original.incomingRequest(request, channel);
                        } catch (Exception e) {
                            log.error("Error in " + original, e);
                            createAndSendResponseException(channel, request, e);
                        }
                    } else {
                        org.apache.logging.log4j.ThreadContext.remove("user");

                        if (result.getRestStatus() != null) {

                            createResponse(channel, request, result);
                        }
                    }
                }, (e) -> {
                    createAndSendResponseException(channel, request, e);
                });
            } else {
                original.incomingRequest(request, channel);
            }
        }

        private boolean isAuthcRequired(HttpRequest request) {
            String path = request.rawPath();
            return request.method() != Method.OPTIONS && !"/_searchguard/license".equals(path)
                    && !"/_searchguard/license".equals(path) && !"/_searchguard/health".equals(path)
                    && !("/_searchguard/auth/session".equals(path) && request.method() == Method.POST);
        }

        private boolean checkRequest(HttpRequest request, HttpChannel channel) {

            threadContext.putTransient(ConfigConstants.SG_ORIGIN, Origin.REST.toString());

            if (containsBadHeader(request)) {
                final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                log.error(exception);
                // TODO ES 9.1.x restore auditlog call
//                auditLog.logBadHeaders(request);
                createAndSendResponseException(channel, request, exception, RestStatus.FORBIDDEN);

                return false;
            }

            if (SSLRequestHelper.containsBadHeader(threadContext, ConfigConstants.SG_CONFIG_PREFIX)) {
                final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                log.error(exception);
                // TODO ES 9.1.x restore auditlog call
//                auditLog.logBadHeaders(request);
                    createAndSendResponseException(channel, request, exception, RestStatus.FORBIDDEN);
                return false;
            }

            final SSLInfo sslInfo;
            try {
                if ((sslInfo = SSLRequestHelper.getSSLInfo(settings, configPath, request, principalExtractor)) != null) {
                    if (sslInfo.getPrincipal() != null) {
                        threadContext.putTransient("_sg_ssl_principal", sslInfo.getPrincipal());
                    }

                    if (sslInfo.getX509Certs() != null) {
                        threadContext.putTransient("_sg_ssl_peer_certificates", sslInfo.getX509Certs());
                    }
                    threadContext.putTransient("_sg_ssl_protocol", sslInfo.getProtocol());
                    threadContext.putTransient("_sg_ssl_cipher", sslInfo.getCipher());
                }
            } catch (SSLPeerUnverifiedException e) {
                log.error("No ssl info", e);
                // TODO ES 9.1.x restore auditlog call
//                auditLog.logSSLException(request, e);
                    createAndSendResponseException(channel, request, e, RestStatus.FORBIDDEN);
                return false;
            }

            return true;
        }

        private void sendDebugInfo(HttpChannel channel, HttpRequest request, AuthcResult authcResult) {
            createAndSendResponse(channel, request, authcResult.getRestStatus(), "application/json", authcResult.getHeaders(), authcResult.toPrettyJsonString());
        }


    }

    public static void createUnauthorizedResponse(HttpChannel channel, HttpRequest request) {
        createResponse(channel, request, AuthcResult.stop(RestStatus.UNAUTHORIZED, ConfigConstants.UNAUTHORIZED));
    }

    public static void createResponse(HttpChannel channel, HttpRequest request, AuthcResult result) {
        final String statusMessage = result.getRestStatusMessage()==null?result.getRestStatus().name():result.getRestStatusMessage();
        final String accept = request.header("accept");

        if(accept == null || (!accept.startsWith("application/json") && !accept.startsWith("application/vnd.elasticsearch+json"))) {
            createAndSendResponse(channel, request, result.getRestStatus(), "text/plain); charset=UTF-8", statusMessage);
        }

        String bodyString = DocNode.of(//
                "status", result.getRestStatus().getStatus(), //
                "error.reason", statusMessage, //
                "error.type", "authentication_exception" //
        ).toJsonString();
        createAndSendResponse(channel, request, result.getRestStatus(), "application/json; charset=UTF-8", result.getHeaders(), bodyString);
    }

    public static void createAndSendResponse(HttpChannel channel, HttpRequest request, RestStatus restStatus, String contentType, Map<String, List<String>> additionalHeaders, String body) {
        HttpResponse httpResponse = createResponse(request, restStatus, contentType, body);
        additionalHeaders.forEach((k, v) -> v.forEach((e) -> httpResponse.addHeader(k, e)));
        channel.sendResponse(httpResponse, new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                log.debug("Response sent");
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Cannot send response with status '{}' for request {} {}", restStatus,  request.method(), request.rawPath(), e);
            }
        });
    }

    static private void createAndSendResponse(HttpChannel channel, HttpRequest request, RestStatus restStatus, String contentType, String body) {
        createAndSendResponse(channel, request, restStatus, contentType, Map.of(), body);
    }

    private static HttpResponse createResponse(HttpRequest request, RestStatus restStatus, String contentType, String body) {
        BytesArray responseBody = new BytesArray(body);
        HttpResponse httpResponse = request.createResponse(restStatus, responseBody);
        httpResponse.addHeader("Content-Type", contentType);
        return httpResponse;
    }

    static private void createAndSendResponseException(HttpChannel channel, HttpRequest request, Exception e) {
        RestStatus status = ExceptionsHelper.status(e);
        createAndSendResponseException(channel, request, e, status);
    }

    static private void createAndSendResponseException(HttpChannel channel, HttpRequest request, Exception e, RestStatus status) {
        String message = e.getMessage();
        // TODO ES 9.1.x the code is simplified in compersion to previous version.
        createAndSendResponse(channel, request, status, "text/plain); charset=UTF-8", message);
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

    private static boolean containsBadHeader(HttpRequest request) {
        for (String key : request.getHeaders().keySet()) {
            if (key != null && key.trim().toLowerCase().startsWith(ConfigConstants.SG_CONFIG_PREFIX.toLowerCase())) {
                return true;
            }
        }

        return false;
    }
}
