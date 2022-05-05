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

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthFailureListener;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.base.IPAddressAcceptanceRules;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer.ClientIpInfo;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.cache.Cache;

import inet.ipaddr.IPAddress;

public interface RestAuthenticationProcessor extends ComponentStateProvider {

    void authenticate(RestHandler restHandler, RestRequest request, RestChannel channel, Consumer<AuthcResult> onResult,
            Consumer<Exception> onFailure);

    boolean isDebugEnabled();

    public static class Default implements RestAuthenticationProcessor {

        private static final Logger log = LogManager.getLogger(RestAuthenticationProcessor.class);

        private final AuditLog auditLog;
        private final ThreadContext threadContext;
        private final AdminDNs adminDns;
        private final Cache<AuthCredentials, User> userCache;
        private final Cache<String, User> impersonationCache;
        private final PrivilegesEvaluator privilegesEvaluator;
        private final BlockedIpRegistry blockedIpRegistry;
        private final BlockedUserRegistry blockedUserRegistry;
        private final boolean debug;

        private final RestAuthcConfig authcConfig;
        private final List<AuthenticationDomain<HTTPAuthenticator>> authenticationDomains;
        private final ClientAddressAscertainer clientAddressAscertainer;
        private final IPAddressAcceptanceRules ipAddressAcceptanceRules;
        private final List<String> requiredLoginPrivileges = Collections.emptyList();
        private final ComponentState componentState = new ComponentState("rest_authentication_processor");

        private List<AuthFailureListener> ipAuthFailureListeners = ImmutableList.empty();

        public Default(RestAuthcConfig config, SearchGuardModulesRegistry modulesRegistry, AdminDNs adminDns, BlockedIpRegistry blockedIpRegistry,
                BlockedUserRegistry blockedUserRegistry, AuditLog auditLog, ThreadPool threadPool, PrivilegesEvaluator privilegesEvaluator) {
            this.authcConfig = config;
            this.authenticationDomains = authcConfig.getAuthenticators().with(modulesRegistry.getImplicitHttpAuthenticationDomains());
            this.clientAddressAscertainer = ClientAddressAscertainer.create(authcConfig.getNetwork());
            this.ipAddressAcceptanceRules = authcConfig.getNetwork() != null ? authcConfig.getNetwork().getIpAddressAcceptanceRules()
                    : IPAddressAcceptanceRules.ANY;
            this.debug = config.isDebugEnabled();

            this.auditLog = auditLog;
            this.threadContext = threadPool.getThreadContext();
            this.adminDns = adminDns;
            this.privilegesEvaluator = privilegesEvaluator;
            this.blockedIpRegistry = blockedIpRegistry;
            this.blockedUserRegistry = blockedUserRegistry;

            this.userCache = authcConfig.getUserCacheConfig().build();
            this.impersonationCache = authcConfig.getUserCacheConfig().build();
            
            for (AuthenticationDomain<HTTPAuthenticator> authenticationDomain : this.authenticationDomains) {
                componentState.addPart(authenticationDomain.getComponentState());
            }
        }

        public void authenticate(RestHandler restHandler, RestRequest request, RestChannel channel, Consumer<AuthcResult> onResult,
                Consumer<Exception> onFailure) {
            String sslPrincipal = threadContext.getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

            ClientIpInfo clientInfo = clientAddressAscertainer.getActualRemoteAddress(request);
            RequestMetaData<RestRequest> requestMetaData = new RequestMetaData<RestRequest>(request, clientInfo, sslPrincipal);
            IPAddress remoteIpAddress = clientInfo.getOriginatingIpAddress();

            if (!ipAddressAcceptanceRules.accept(requestMetaData)) {
                log.info("Not accepting request from {}", requestMetaData);
                onResult.accept(AuthcResult.stop(RestStatus.FORBIDDEN, "Forbidden",
                        ImmutableList.of(new AuthcResult.DebugInfo("-/-", false,
                                "Request denied because client IP address is denied by authc.network.accept configuration",
                                ImmutableMap.of("direct_ip_address", clientInfo.getDirectIpAddress(), "originating_ip_address",
                                        clientInfo.getOriginatingIpAddress(), "trusted_proxy", clientInfo.isTrustedProxy())))));
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("Rest authentication request from {} [original: {}]", remoteIpAddress, request.getHttpChannel().getRemoteAddress());
            }

            if (clientInfo.isTrustedProxy()) {
                threadContext.putTransient(ConfigConstants.SG_XFF_DONE, Boolean.TRUE);
            }

            threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, clientInfo.getOriginatingTransportAddress());

            if (blockedIpRegistry.isIpBlocked(remoteIpAddress)) {
                if (log.isDebugEnabled()) {
                    log.debug("Rejecting REST request because of blocked address: " + request.getHttpChannel().getRemoteAddress());
                }
                auditLog.logBlockedIp(request, request.getHttpChannel().getRemoteAddress());
                channel.sendResponse(new BytesRestResponse(RestStatus.UNAUTHORIZED, "Authentication finally failed"));
                onResult.accept(new AuthcResult(AuthcResult.Status.STOP));
                return;
            }

            new RestRequestAuthenticationProcessor(restHandler, requestMetaData, channel, threadContext, authenticationDomains, adminDns,
                    privilegesEvaluator, userCache, impersonationCache, auditLog, blockedUserRegistry, ipAuthFailureListeners,
                    requiredLoginPrivileges, debug).authenticate(onResult, onFailure);

        }

        @Override
        public boolean isDebugEnabled() {
            return debug;
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }
    }
}
