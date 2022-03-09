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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.NoSuchComponentException;
import com.floragunn.searchguard.TypedComponentRegistry;
import com.floragunn.searchguard.authc.AuthenticationDebugLogger;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.AuthenticationBackend.UserMapper;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.authc.transport.TransportAuthenticationDomain.TransportAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.support.IPAddressCollection;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

import inet.ipaddr.IPAddress;

public class LegacyAuthenticationDomain<AuthenticatorType extends AuthenticationFrontend> implements AuthenticationDomain<AuthenticatorType> {
    private static final Logger log = LogManager.getLogger(LegacyAuthenticationDomain.class);

    private final String id;
    private final LegacyAuthenticationBackend backend;
    private final AuthenticatorType authenticator;
    private final ImmutableList<LegacyAuthorizationBackend> authorizationBackends;
    private final int order;
    private final boolean challenge;
    private final Pattern skippedUsers;
    private final IPAddressCollection enabledOnlyForIps;
    private final String infoString;

    public LegacyAuthenticationDomain(String id, LegacyAuthenticationBackend backend, AuthenticatorType authenticator, boolean challenge, int order,
            Pattern skippedUsers, IPAddressCollection enabledOnlyForIps, ImmutableList<LegacyAuthorizationBackend> authorizationBackends) {
        super();
        this.id = id;
        this.backend = backend;
        this.authenticator = authenticator;
        this.order = order;
        this.challenge = challenge;
        this.skippedUsers = skippedUsers;
        this.enabledOnlyForIps = enabledOnlyForIps;
        this.authorizationBackends = authorizationBackends;
        this.infoString = buildInfoString();
    }

    public boolean isChallenge() {
        return challenge;
    }

    @Override
    public AuthenticatorType getFrontend() {
        return authenticator;
    }

    @Override
    public boolean accept(RequestMetaData<?> request) {
        IPAddress ipAddress = request.getOriginatingIpAddress();

        return enabledOnlyForIps == null || enabledOnlyForIps.contains(ipAddress);
    }

    @Override
    public boolean accept(AuthCredentials authCredentials) {
        return skippedUsers == null || !skippedUsers.matches(authCredentials.getName());
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    public int getOrder() {
        return order;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return infoString;
    }

    private String buildInfoString() {
        StringBuilder result = new StringBuilder();

        if (authenticator != null) {
            result.append(authenticator.getType());
        }

        if (backend != null) {
            result.append("/").append(backend.getType());
        }

        if (id != null) {
            result.append("[").append(id).append("]");
        }

        return result.toString();
    }

    public static Optional<AuthenticationDomain<HTTPAuthenticator>> parseHttpDomain(String id, DocNode docNode,
            ConfigurationRepository.Context context, ImmutableList<LegacyAuthorizationBackend> authorizationBackends)
            throws ConfigValidationException {
        TypedComponentRegistry typedComponentRegistry = context.modulesRegistry().getTypedComponentRegistry();

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        boolean enabled = vNode.get("http_enabled").withDefault(false).asBoolean();

        if (!enabled) {
            return Optional.empty();
        }

        IPAddressCollection enabledOnlyForIps = vNode.get("enabled_only_for_ips").by(IPAddressCollection::parse);
        Pattern skipUsers = vNode.get("skip_users").by(Pattern::parse);
        String authenticatorType = vNode.get("http_authenticator.type").asString();
        int order = vNode.get("order").withDefault(0).asInt();
        boolean challenge = vNode.get("challenge").withDefault(false).asBoolean();

        if (authenticatorType == null) {
            // This is likely an auth domain that only works for the transport layer
            return Optional.empty();
        }

        String backendType = vNode.get("authentication_backend.type").withDefault("internal").asString();

        HTTPAuthenticator httpAuthenticator = null;
        LegacyAuthenticationBackend authenticationBackend = null;

        try {
            httpAuthenticator = typedComponentRegistry.create(LegacyHTTPAuthenticator.class, authenticatorType,
                    docNode.getAsNode("http_authenticator", "config"), context);
            
            if (httpAuthenticator == null) {
                throw new NoSuchComponentException(authenticatorType);
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("http_authenticator.config", e);
        } catch (NoSuchComponentException e) {
            validationErrors.add(
                    new InvalidAttributeValue("http_authenticator.type", authenticatorType, e.getAvailableTypesAsInfoString())
                            .message("Unknown HTTP authenticator").cause(e));
        } catch (Exception e) {
            log.error("Unexpected exception while creating authenticator " + authenticatorType, e);
            validationErrors.add(new ValidationError("http_authenticator", e.getMessage()).cause(e));
        }

        try {
            // Resolve the alias of the internal auth domain. This is done hard-coded because we don't want aliases any more in non-legacy code.
            if ("intern".equals(backendType)) {
                backendType = "internal";
            }
             
            authenticationBackend = typedComponentRegistry.create(LegacyAuthenticationBackend.class, backendType, docNode.getAsNode("authentication_backend", "config"), context);

            if (authenticationBackend == null) {
                throw new NoSuchComponentException(backendType);
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("authentication_backend.config", e);
        } catch (NoSuchComponentException e) {
            validationErrors.add(new InvalidAttributeValue("type", backendType, e.getAvailableTypesAsInfoString())
                    .message("Unknown authentication backend").cause(e));
        } catch (Exception e) {
            log.error("Unexpected exception while creating authentication backend " + backendType, e);
            validationErrors.add(new ValidationError("authentication_backend", e.getMessage()).cause(e));
        }

        validationErrors.throwExceptionForPresentErrors();

        return Optional.of(new LegacyAuthenticationDomain<HTTPAuthenticator>(id, authenticationBackend, httpAuthenticator, challenge, order,
                skipUsers, enabledOnlyForIps, authorizationBackends));

    }

    public static Optional<AuthenticationDomain<TransportAuthenticationFrontend>> parseTransportDomain(String id, DocNode docNode,
            ConfigurationRepository.Context context, ImmutableList<LegacyAuthorizationBackend> authorizationBackends)
            throws ConfigValidationException {
        TypedComponentRegistry typedComponentRegistry = context.modulesRegistry().getTypedComponentRegistry();

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        boolean enabled = vNode.get("transport_enabled").withDefault(false).asBoolean();

        if (!enabled) {
            return Optional.empty();
        }

        IPAddressCollection enabledOnlyForIps = vNode.get("enabled_only_for_ips").by(IPAddressCollection::parse);
        Pattern skipUsers = vNode.get("skip_users").by(Pattern::parse);
        int order = vNode.get("order").withDefault(0).asInt();

        String backendType = vNode.get("authentication_backend.type").withDefault("internal").asString();

        LegacyAuthenticationBackend authenticationBackend = null;

        try {
            // Resolve the alias of the internal auth domain. This is done hard-coded because we don't want aliases any more in non-legacy code.
            if ("intern".equals(backendType)) {
                backendType = "internal";
            }
            
            authenticationBackend = typedComponentRegistry.create(LegacyAuthenticationBackend.class, backendType, docNode.getAsNode("authentication_backend", "config"), context);

            if (authenticationBackend == null) {
                throw new NoSuchComponentException(backendType);
            }

        } catch (ConfigValidationException e) {
            validationErrors.add(backendType, e);
        } catch (NoSuchComponentException e) {
            validationErrors.add(new InvalidAttributeValue("authentication_backend.type", backendType,
                    e.getAvailableTypesAsInfoString()).message("Unknown authentication backend").cause(e));
        } catch (Exception e) {
            log.error("Unexpected exception while creating authentication backend " + backendType, e);
            validationErrors.add(new ValidationError("authentication_backend", e.getMessage()).cause(e));
        }

        validationErrors.throwExceptionForPresentErrors();

        return Optional.of(new LegacyAuthenticationDomain<TransportAuthenticationFrontend>(id, authenticationBackend, null, false, order, skipUsers,
                enabledOnlyForIps, authorizationBackends));

    }

    @Override
    public CompletableFuture<User> authenticate(AuthCredentials authCredentials, AuthenticationDebugLogger debug) throws AuthenticatorUnavailableException, CredentialsException {
        User user = backend.authenticate(authCredentials);

        if (user == null) {
            return CompletableFuture.completedFuture(null);
        }

        for (LegacyAuthorizationBackend authorizationBackend : authorizationBackends) {
            try {
                authorizationBackend.fillRoles(user, authCredentials);
            } catch (ElasticsearchSecurityException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Authz backend " + authorizationBackend + " did not find roles for " + authCredentials, e);
                }
            } catch (Exception e) {
                log.warn("Error while retrieving roles for " + authCredentials + " from " + authorizationBackend, e);
            }
        }

        return CompletableFuture.completedFuture(user);
    }

    @Override
    public CompletableFuture<User> impersonate(User originalUser, AuthCredentials authCredentials)
            throws AuthenticatorUnavailableException, CredentialsException {

        User user = UserMapper.DIRECT.map(authCredentials);

        if (!backend.exists(user)) {
            return CompletableFuture.completedFuture(null);
        }

        for (LegacyAuthorizationBackend authorizationBackend : authorizationBackends) {
            try {
                authorizationBackend.fillRoles(user, authCredentials);
            } catch (ElasticsearchSecurityException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Authz backend " + authorizationBackend + " did not find roles for " + authCredentials, e);
                }
            } catch (Exception e) {
                log.warn("Error while retrieving roles for " + authCredentials + " from " + authorizationBackend, e);
            }
        }

        return CompletableFuture.completedFuture(user);
    }

    @Override
    public String getType() {
        return toString();
    }

    @Override
    public boolean cacheUser() {
        switch (backend.userCachingPolicy()) {
        case ALWAYS:
            return true;
        case ONLY_IF_AUTHZ_SEPARATE:
            return !authorizationBackends.isEmpty();
        case NEVER:
        default:
            return false;
        }
    }
}
