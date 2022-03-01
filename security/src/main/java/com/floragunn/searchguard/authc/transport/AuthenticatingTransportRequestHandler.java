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
 */

package com.floragunn.searchguard.authc.transport;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequest;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthenticationDebugLogger;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.IPAddressAcceptanceRules;
import com.floragunn.searchguard.authc.blocking.BlockedIpRegistry;
import com.floragunn.searchguard.authc.blocking.BlockedUserRegistry;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authc.transport.TransportAuthenticationDomain.TransportAuthenticationFrontend;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.HTTPHelper;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.google.common.cache.Cache;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressNetwork.IPAddressGenerator;

public class AuthenticatingTransportRequestHandler implements ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(AuthenticatingTransportRequestHandler.class);

    private final IPAddressGenerator ipAddressGenerator = new IPAddressGenerator();

    private final AdminDNs adminDns;
    private final AuditLog auditLog;
    private final ThreadContext threadContext;
    private volatile Cache<AuthCredentials, User> authenticatedUserCache;
    private volatile Cache<String, User> transportImpersonationCache; //used for transport impersonation
    private final ComponentState componentState = new ComponentState(0, "authc", "transport_filter");
    private final BlockedIpRegistry blockedIpRegistry;
    private final BlockedUserRegistry blockedUserRegistry;

    private volatile TransportAuthcConfig authczConfig;
    private volatile IPAddressAcceptanceRules ipAddressAcceptanceRules = IPAddressAcceptanceRules.ANY;

    public AuthenticatingTransportRequestHandler(ConfigurationRepository configurationRepository, Settings settings, AuditLog auditLog,
            AdminDNs adminDNs, BlockedIpRegistry blockedIpRegistry, BlockedUserRegistry blockedUserRegistry, ThreadContext threadContext) {
        this.adminDns = adminDNs;
        this.auditLog = auditLog;
        this.threadContext = threadContext;
        this.blockedIpRegistry = blockedIpRegistry;
        this.blockedUserRegistry = blockedUserRegistry;

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<TransportAuthcConfig> config = configMap.get(CType.AUTHC_TRANSPORT);
                SgDynamicConfiguration<LegacySgConfig> legacyConfig = configMap.get(CType.CONFIG);

                if (config != null && config.getCEntry("default") != null) {
                    TransportAuthcConfig authczConfig = config.getCEntry("default");
                    AuthenticatingTransportRequestHandler.this.authczConfig = authczConfig;
                    componentState.setState(State.INITIALIZED, "using_authcz_config");
                    componentState.setConfigVersion(config.getDocVersion());
                    authenticatedUserCache = authczConfig.getUserCacheConfig().build();
                    transportImpersonationCache = authczConfig.getUserCacheConfig().build();
                    ipAddressAcceptanceRules = authczConfig.getNetwork() != null ? authczConfig.getNetwork().getIpAddressAcceptanceRules()
                            : IPAddressAcceptanceRules.ANY;
                } else if (legacyConfig != null && legacyConfig.getCEntry("sg_config") != null) {
                    TransportAuthcConfig authczConfig = legacyConfig.getCEntry("sg_config").getTransportAuthczConfig();
                    AuthenticatingTransportRequestHandler.this.authczConfig = authczConfig;
                    componentState.setState(State.INITIALIZED, "using_legacy_config");
                    componentState.setConfigVersion(legacyConfig.getDocVersion());
                    authenticatedUserCache = authczConfig.getUserCacheConfig().build();
                    transportImpersonationCache = authczConfig.getUserCacheConfig().build();
                    ipAddressAcceptanceRules = IPAddressAcceptanceRules.ANY;
                } else {
                    componentState.setState(State.SUSPENDED, "no_configuration");
                }

            }
        });
    }

    public User authenticate(final TransportRequest request, final String sslPrincipal, final Task task, final String action) {
        if (log.isDebugEnabled() && request.remoteAddress() != null) {
            log.debug("Transport authentication request from {}", request.remoteAddress());
        }

        User origPKIUser = new User(sslPrincipal, AuthDomainInfo.TLS_CERT);

        if (adminDns.isAdmin(origPKIUser)) {
            auditLog.logSucceededLogin(origPKIUser, true, null, request, action, task);
            return origPKIUser;
        }

        IPAddress remoteIpAddress = null;

        if (request.remoteAddress() != null) {
            remoteIpAddress = ipAddressGenerator.from(request.remoteAddress().address().getAddress());
        }

        if (authczConfig == null) {
            log.error("Not yet initialized (you may need to run sgadmin)");
            return null;
        }

        RequestMetaData<TransportRequest> requestMetaData = new RequestMetaData<TransportRequest>(request, remoteIpAddress, action);

        if (!ipAddressAcceptanceRules.accept(requestMetaData)) {
            log.info("Not accepting request due to acceptance rules {}", requestMetaData);
            return null;
        }

        if (blockedIpRegistry.isIpBlocked(remoteIpAddress)) {
            if (log.isDebugEnabled()) {
                log.debug("Rejecting transport request because of blocked address: " + request.remoteAddress());
            }
            auditLog.logBlockedIp(request, action, request.remoteAddress(), task);
            return null;
        }

        final String authorizationHeader = threadContext.getHeader("Authorization");
        //Use either impersonation OR credentials authentication
        //if both is supplied credentials authentication win
        AuthCredentials.Builder credentialBuilder = HTTPHelper.extractCredentials(authorizationHeader, log);
        AuthCredentials creds = credentialBuilder != null ? credentialBuilder.authenticatorType("transport_basic").build() : null;

        User impersonatedTransportUser = null;

        if (creds != null) {
            if (log.isDebugEnabled()) {
                log.debug("User {} submitted also basic credentials: {}", origPKIUser.getName(), creds);
            }
        }

        String impersonateUser = threadContext.getHeader("sg_impersonate_as");

        for (final AuthenticationDomain<TransportAuthenticationFrontend> authenticationDomain : authczConfig.getAuthenticators()) {

            if (!authenticationDomain.accept(requestMetaData)) {
                continue;
            }

            if (!authenticationDomain.accept(creds)) {
                continue;
            }

            User authenticatedUser;

            if (creds != null) {
                //auth credentials submitted
                //impersonation not possible, if requested it will be ignored
                authenticatedUser = authcz(creds, authenticationDomain);
            } else if (impersonateUser != null) {
                //no credentials submitted
                //impersonation possible
                authenticatedUser = impersonate(origPKIUser, impersonateUser, authenticationDomain);
            } else {
                // The user impersonates as themselves
                authenticatedUser = checkExistsAndAuthz(origPKIUser,
                        AuthCredentials.forUser(origPKIUser.getName()).authDomainInfo(AuthDomainInfo.TLS_CERT).build(), authenticationDomain);
            }

            if (authenticatedUser == null) {
                /* TODO
                for (AuthFailureListener authFailureListener : authBackendFailureListeners
                        .get(authenticationDomain.getBackend().getClass().getName())) {
                    authFailureListener.onAuthFailure(request.remoteAddress() != null ? request.remoteAddress().address().getAddress() : null, creds,
                            request);
                }
                */

                if (log.isDebugEnabled()) {
                    log.debug("Cannot authenticate transport user {} (or add roles) with authdomain {}, try next",
                            creds == null ? (impersonatedTransportUser == null ? origPKIUser.getName() : impersonatedTransportUser.getName())
                                    : creds.getUsername(),
                            authenticationDomain);
                }
                continue;
            }

            if (adminDns.isAdmin(authenticatedUser)) {
                log.error("Cannot authenticate transport user because admin user is not permitted to login");
                auditLog.logFailedLogin(authenticatedUser, true, null, request, task);
                return null;
            }

            if (blockedUserRegistry.isUserBlocked(authenticatedUser.getName())) {
                if (log.isDebugEnabled()) {
                    log.debug("Rejecting TRANSPORT request because of blocked user: " + authenticatedUser.getName() + "; authDomain: "
                            + authenticationDomain);
                }
                auditLog.logBlockedUser(authenticatedUser, false, origPKIUser, request, task);
                continue;

            }

            if (log.isDebugEnabled()) {
                log.debug("Transport user '{}' is authenticated", authenticatedUser);
            }

            auditLog.logSucceededLogin(authenticatedUser, false, impersonatedTransportUser == null ? null : origPKIUser, request, action, task);

            return authenticatedUser;
        } //end looping auth domains

        //auditlog
        if (creds == null) {
            auditLog.logFailedLogin(impersonatedTransportUser == null ? origPKIUser : impersonatedTransportUser, false,
                    impersonatedTransportUser == null ? null : origPKIUser, request, task);
        } else {
            auditLog.logFailedLogin(creds, false, null, request, task);
        }

        log.warn("Transport authentication finally failed for {} from {}",
                creds == null ? impersonatedTransportUser == null ? origPKIUser.getName() : impersonatedTransportUser.getName() : creds.getUsername(),
                request.remoteAddress());

        // TODO
        //  notifyIpAuthFailureListeners(request.remoteAddress() != null ? request.remoteAddress().address().getAddress() : null, creds, request);

        return null;
    }

    private User impersonate(User origPKIuser, String impersonatedUser, AuthenticationDomain<TransportAuthenticationFrontend> authDomain)
            throws OpenSearchSecurityException {

        if (origPKIuser == null) {
            throw new OpenSearchSecurityException("no original PKI user found");
        }

        if (adminDns.isAdminDN(impersonatedUser)) {
            throw new OpenSearchSecurityException(
                    "'" + origPKIuser.getName() + "' is not allowed to impersonate as an adminuser  '" + impersonatedUser + "'");
        }

        try {
            if (!adminDns.isTransportImpersonationAllowed(new LdapName(origPKIuser.getName()), impersonatedUser)) {
                throw new OpenSearchSecurityException(
                        "'" + origPKIuser.getName() + "' is not allowed to impersonate as transport user '" + impersonatedUser + "'");
            } else {
                User impersonatedUserObject = checkExistsAndAuthz(origPKIuser,
                        AuthCredentials.forUser(impersonatedUser).authenticatorType("impersonation+tls_cert").build(), authDomain);

                if (impersonatedUserObject != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Impersonate transport user from '{}' to '{}'", origPKIuser.getName(), impersonatedUser);
                    }
                    return impersonatedUserObject;
                } else {
                    log.debug(
                            "Unable to impersonate transport user from '{}' to '{}' because the impersonated user does not exists in {}, try next ...",
                            origPKIuser.getName(), impersonatedUser, authDomain);
                    return null;
                }
            }
        } catch (final InvalidNameException e1) {
            throw new OpenSearchSecurityException("PKI does not have a valid name ('" + origPKIuser.getName() + "'), should never happen", e1);
        }
    }

    private User authcz(AuthCredentials ac, AuthenticationDomain<TransportAuthenticationFrontend> authDomain) {
        if (ac == null) {
            return null;
        }

        try {
            if (!authDomain.cacheUser() || authenticatedUserCache == null) {
                return authDomain.authenticate(ac, AuthenticationDebugLogger.DISABLED).get();
            }

            return authenticatedUserCache.get(ac, () -> {
                if (log.isTraceEnabled()) {
                    log.trace("Credentials for user " + ac.getUsername() + " not cached, return from " + authDomain + " backend directly");
                }
                final User authenticatedUser = authDomain.authenticate(ac, AuthenticationDebugLogger.DISABLED).get();

                if (authenticatedUser == null) {
                    throw new CredentialsException("Could not authenticate " + ac);
                }

                return authenticatedUser;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Can not authenticate " + ac.getUsername() + " due to " + e.toString(), e);
            }
            return null;
        } finally {
            ac.clearSecrets();
        }
    }

    private User checkExistsAndAuthz(User origPKIuser, AuthCredentials authCredentials,
            AuthenticationDomain<TransportAuthenticationFrontend> authDomain) {
        if (authCredentials == null) {
            return null;
        }

        try {
            if (transportImpersonationCache == null) {
                return authDomain.impersonate(origPKIuser, authCredentials).get();
            }

            return transportImpersonationCache.get(authCredentials.getName(), () -> {
                if (log.isTraceEnabled()) {
                    log.trace("Credentials for user " + authCredentials.getName() + " not cached, return from " + authDomain + " backend directly");
                }

                User impersonatedUser = authDomain.impersonate(origPKIuser, authCredentials).get();

                if (impersonatedUser != null) {
                    return impersonatedUser;
                }

                if (log.isDebugEnabled()) {
                    log.debug("User " + authCredentials.getName() + " does not exist in " + authDomain);
                }
                throw new CredentialsException("User " + authCredentials.getName() + " does not exist in " + authDomain);
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Can not check and authorize " + authCredentials.getName() + " due to " + e.toString(), e);
            }
            return null;
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
