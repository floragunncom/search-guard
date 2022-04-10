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

package com.floragunn.searchguard.authc.legacy;

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
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthFailureListener;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.AuthczResult;
import com.floragunn.searchguard.authc.base.IPAddressAcceptanceRules;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer;
import com.floragunn.searchguard.authc.rest.ClientAddressAscertainer.ClientIpInfo;
import com.floragunn.searchguard.authc.rest.RestAuthcConfig;
import com.floragunn.searchguard.authc.rest.RestAuthenticationProcessor;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.cache.Cache;

import inet.ipaddr.IPAddress;

public class LegacyRestAuthenticationProcessor implements RestAuthenticationProcessor {

    private static final Logger log = LogManager.getLogger(LegacyRestAuthenticationProcessor.class);

    private final AuditLog auditLog;
    private final ThreadContext threadContext;
    private final AdminDNs adminDns;
    private final Cache<AuthCredentials, User> userCache;
    private final Cache<String, User> impersonationCache;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final BlockedIpRegistry blockedIpRegistry;
    private final BlockedUserRegistry blockedUserRegistry;

    private final RestAuthcConfig authczConfig;
    private final List<AuthenticationDomain<HTTPAuthenticator>> authenticationDomains;
    private final ClientAddressAscertainer clientAddressAscertainer;
    private final IPAddressAcceptanceRules ipAddressAcceptanceRules;
    private final List<String> requiredLoginPrivileges = Collections.emptyList();
    private final ComponentState componentState = new ComponentState("legacy_rest_authentication_processor");

    private List<AuthFailureListener> ipAuthFailureListeners = ImmutableList.empty();

    public LegacyRestAuthenticationProcessor(LegacySgConfig legacyConfig, SearchGuardModulesRegistry modulesRegistry, AdminDNs adminDns,
            BlockedIpRegistry blockedIpRegistry, BlockedUserRegistry blockedUserRegistry, AuditLog auditLog, ThreadPool threadPool,
            PrivilegesEvaluator privilegesEvaluator) {
        this.authczConfig = legacyConfig.getRestAuthczConfig();
        this.authenticationDomains = authczConfig.getAuthenticators().with(modulesRegistry.getImplicitHttpAuthenticationDomains());
        this.clientAddressAscertainer = ClientAddressAscertainer.create(authczConfig.getNetwork());
        this.ipAddressAcceptanceRules = IPAddressAcceptanceRules.ANY;

        this.auditLog = auditLog;
        this.threadContext = threadPool.getThreadContext();
        this.adminDns = adminDns;
        this.privilegesEvaluator = privilegesEvaluator;
        this.blockedIpRegistry = blockedIpRegistry;
        this.blockedUserRegistry = blockedUserRegistry;

        this.userCache = authczConfig.getUserCacheConfig().build();
        this.impersonationCache = authczConfig.getUserCacheConfig().build();

        for (AuthenticationDomain<HTTPAuthenticator> authenticationDomain : this.authenticationDomains) {
            componentState.addPart(authenticationDomain.getComponentState());
        }
    }

    public void authenticate(RestHandler restHandler, RestRequest request, RestChannel channel, Consumer<AuthczResult> onResult,
            Consumer<Exception> onFailure) {
        String sslPrincipal = threadContext.getTransient(ConfigConstants.SG_SSL_PRINCIPAL);

        ClientIpInfo clientInfo = clientAddressAscertainer.getActualRemoteAddress(request);
        RequestMetaData<RestRequest> requestMetaData = new RequestMetaData<RestRequest>(request, clientInfo, sslPrincipal);
        IPAddress remoteIpAddress = clientInfo.getOriginatingIpAddress();

        if (!ipAddressAcceptanceRules.accept(requestMetaData)) {
            log.info("Not accepting request from {}", requestMetaData);
            channel.sendResponse(new BytesRestResponse(RestStatus.FORBIDDEN, "Forbidden"));
            onResult.accept(new AuthczResult(AuthczResult.Status.STOP));
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
            onResult.accept(new AuthczResult(AuthczResult.Status.STOP));
            return;
        }

        new LegacyRestRequestAuthenticationProcessor(restHandler, requestMetaData, channel, threadContext, authenticationDomains, adminDns, privilegesEvaluator,
                userCache, impersonationCache, auditLog, blockedUserRegistry, ipAuthFailureListeners,
                requiredLoginPrivileges, false).authenticate(onResult, onFailure);

    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
