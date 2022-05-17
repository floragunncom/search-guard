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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.net.CacheConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.NoSuchComponentException;
import com.floragunn.searchguard.TypedComponentRegistry;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.UserInformationBackend;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.authc.rest.RestAuthcConfig;
import com.floragunn.searchguard.authc.rest.RestAuthcConfig.Network;
import com.floragunn.searchguard.authc.transport.TransportAuthcConfig;
import com.floragunn.searchguard.authc.transport.TransportAuthenticationDomain;
import com.floragunn.searchguard.authc.transport.TransportAuthenticationDomain.TransportAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.license.SearchGuardLicenseKey;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class LegacySgConfig implements Document<LegacySgConfig> {

    private static final Logger log = LogManager.getLogger(LegacySgConfig.class);

    private final DocNode source;
    private final RestAuthcConfig restAuthcConfig;
    private final TransportAuthcConfig transportAuthcConfig;
    private final SearchGuardLicenseKey license;

    LegacySgConfig(DocNode source, RestAuthcConfig restAuthcConfig, TransportAuthcConfig transportAuthcConfig, SearchGuardLicenseKey license) {
        this.source = source;
        this.restAuthcConfig = restAuthcConfig;
        this.transportAuthcConfig = transportAuthcConfig;
        this.license = license;
    }

    public static ValidationResult<LegacySgConfig> parse(DocNode docNode, ConfigurationRepository.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode;
        try {
            vNode = new ValidatingDocNode(docNode.splitDottedAttributeNamesToTree(), validationErrors);
        } catch (UnexpectedDocumentStructureException e) {
            return new ValidationResult<LegacySgConfig>(e.getValidationErrors());
        }

        if (!vNode.get("dynamic.multi_rolespan_enabled").withDefault(false).asBoolean()) {
            log.error(
                    "The option multi_rolespan_enabled is no longer supported; from now on the privilege evaluation will always work like multi_rolespan_enabled was set to true");
        }

        if (vNode.get("dynamic.disable_rest_auth").withDefault(false).asBoolean()) {
            log.error("The option disable_rest_auth is no longer supported. Rest layer authentication cannot be disabled.");
        }

        SearchGuardLicenseKey license = vNode.get("dynamic.license").by(SearchGuardLicenseKey::parse);
        ValidationResult<ImmutableList<LegacyAuthorizationBackend>> authorizationBackends = parseAuthorizationDomains(docNode, context);
        ValidationResult<RestAuthcConfig> restAuthcConfig = parseRestConfig(docNode, context, authorizationBackends.peek());
        ValidationResult<TransportAuthcConfig> transportAuthcConfig = parseTransportConfig(docNode, context, authorizationBackends.peek());

        validationErrors.add(null, authorizationBackends);
        validationErrors.add(null, restAuthcConfig);
        validationErrors.add(null, transportAuthcConfig);

        if (restAuthcConfig.hasResult() && transportAuthcConfig.hasResult()) {
            return new ValidationResult<LegacySgConfig>(new LegacySgConfig(docNode, restAuthcConfig.peek(), transportAuthcConfig.peek(), license),
                    validationErrors);
        } else {
            return new ValidationResult<LegacySgConfig>(validationErrors);
        }
    }

    static ValidationResult<RestAuthcConfig> parseRestConfig(DocNode docNode, ConfigurationRepository.Context context,
            ImmutableList<LegacyAuthorizationBackend> authorizationBackends) {

        DocNode authcNode = docNode.getAsNode("dynamic", "authc");

        if (authcNode.isNull()) {
            return new ValidationResult<RestAuthcConfig>(RestAuthcConfig.empty(docNode), new MissingAttribute("dynamic.authc"));
        }

        if (!authcNode.isMap()) {
            return new ValidationResult<RestAuthcConfig>(RestAuthcConfig.empty(docNode),
                    new InvalidAttributeValue("dynamic.authc", null, "A mapping from auth domain names to definitions"));
        }

        ValidationErrors validationErrors = new ValidationErrors();
        boolean criticalErrors = false;

        ValidationResult<ImmutableList<AuthenticationDomain<HttpAuthenticationFrontend>>> authenticationDomains = parseAuthenticationDomains(docNode, context,
                authorizationBackends);
        validationErrors.add(null, authenticationDomains.getValidationErrors());

        Network network = null;

        try {
            DocNode xffNode = docNode.getAsNode("dynamic", "http", "xff");

            if (!xffNode.isNull() && xffNode.getBoolean("enabled") != null && xffNode.getBoolean("enabled") == true) {
                network = Network.parseLegacy(xffNode);
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("dynamic.http.xff", e);
            criticalErrors = true;
        }

        if (!criticalErrors && authenticationDomains.hasResult()) {
            return new ValidationResult<RestAuthcConfig>(
                    new RestAuthcConfig(docNode, authenticationDomains.peek(), network, CacheConfig.DEFAULT, false), validationErrors);
        } else {
            return new ValidationResult<RestAuthcConfig>(RestAuthcConfig.empty(docNode), validationErrors);
        }
    }

    static ValidationResult<TransportAuthcConfig> parseTransportConfig(DocNode docNode, ConfigurationRepository.Context context,
            ImmutableList<LegacyAuthorizationBackend> authorizationBackends) {

        DocNode authcNode = docNode.getAsNode("dynamic", "authc");

        if (authcNode.isNull()) {
            return new ValidationResult<TransportAuthcConfig>(new MissingAttribute("dynamic.authc"));
        }

        if (!authcNode.isMap()) {
            return new ValidationResult<TransportAuthcConfig>(
                    new InvalidAttributeValue("dynamic.authc", null, "A mapping from auth domain names to definitions"));
        }

        ValidationErrors validationErrors = new ValidationErrors();

        ValidationResult<ImmutableList<AuthenticationDomain<TransportAuthenticationDomain.TransportAuthenticationFrontend>>> authenticationDomains = parseTransportAuthenticationDomains(
                docNode, context, authorizationBackends);
        validationErrors.add(null, authenticationDomains.getValidationErrors());

        if (authenticationDomains.hasResult()) {
            return new ValidationResult<TransportAuthcConfig>(
                    new TransportAuthcConfig(docNode, authenticationDomains.peek(), null, CacheConfig.DEFAULT, false), validationErrors);
        } else {
            return new ValidationResult<TransportAuthcConfig>(validationErrors);
        }
    }

    static ValidationResult<ImmutableList<AuthenticationDomain<HttpAuthenticationFrontend>>> parseAuthenticationDomains(DocNode docNode,
            ConfigurationRepository.Context context, ImmutableList<LegacyAuthorizationBackend> authorizationBackends) {

        DocNode authcNode = docNode.getAsNode("dynamic", "authc");

        if (authcNode.isNull()) {
            return new ValidationResult<ImmutableList<AuthenticationDomain<HttpAuthenticationFrontend>>>(new MissingAttribute("dynamic.authc"));
        }

        if (!authcNode.isMap()) {
            return new ValidationResult<ImmutableList<AuthenticationDomain<HttpAuthenticationFrontend>>>(
                    new InvalidAttributeValue("dynamic.authc", null, "A mapping from auth domain names to definitions"));
        }

        ImmutableList.Builder<AuthenticationDomain<HttpAuthenticationFrontend>> domains = new ImmutableList.Builder<>();
        ValidationErrors validationErrors = new ValidationErrors();

        for (Map.Entry<String, DocNode> entry : authcNode.toMapOfNodes().entrySet()) {
            String authDomainId = entry.getKey();

            try {
                domains.with(LegacyAuthenticationDomain.parseHttpDomain(authDomainId, entry.getValue(), context, authorizationBackends));
            } catch (ConfigValidationException e) {
                validationErrors.add("dynamic.authc." + authDomainId, e);
            }
        }

        ImmutableList<AuthenticationDomain<HttpAuthenticationFrontend>> result = domains
                .build((a, b) -> Integer.compare(((LegacyAuthenticationDomain<?>) a).getOrder(), ((LegacyAuthenticationDomain<?>) b).getOrder()));

        if (Boolean.TRUE.equals(docNode.get("dynamic", "http", "anonymous_auth_enabled"))) {
            result = result.with(new LegacyAnonAuthenticationDomain());
        }

        return new ValidationResult<ImmutableList<AuthenticationDomain<HttpAuthenticationFrontend>>>(result, validationErrors);
    }

    static ValidationResult<ImmutableList<LegacyAuthorizationBackend>> parseAuthorizationDomains(DocNode docNode,
            ConfigurationRepository.Context context) {
        DocNode authzNode = docNode.getAsNode("dynamic", "authz");

        if (authzNode.isNull()) {
            return new ValidationResult<ImmutableList<LegacyAuthorizationBackend>>(ImmutableList.empty());
        }

        if (!authzNode.isMap()) {
            return new ValidationResult<ImmutableList<LegacyAuthorizationBackend>>(ImmutableList.empty(),
                    new InvalidAttributeValue("dynamic.authc", null, "A mapping from auth domain names to definitions"));
        }

        ValidationErrors validationErrors = new ValidationErrors();
        ImmutableList.Builder<LegacyAuthorizationBackend> result = new ImmutableList.Builder<>();

        for (Map.Entry<String, DocNode> entry : authzNode.toMapOfNodes().entrySet()) {
            try {
                result.with(parseAuthorizationDomain(entry.getValue(), context));
            } catch (ConfigValidationException e) {
                validationErrors.add("dynamic.authz", e);
            }
        }

        return new ValidationResult<ImmutableList<LegacyAuthorizationBackend>>(result.build(), validationErrors);
    }

    static Optional<LegacyAuthorizationBackend> parseAuthorizationDomain(DocNode docNode, ConfigurationRepository.Context context)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);
        TypedComponentRegistry typedComponentRegistry = context.modulesRegistry().getTypedComponentRegistry();

        boolean enabled = vNode.get("http_enabled").withDefault(true).asBoolean();

        if (!enabled) {
            return Optional.empty();
        }

        String type = vNode.get("authorization_backend.type").required().asString();
        List<String> skipUsers = vNode.get("skipped_users").asListOfStrings();

        LegacyAuthorizationBackend authorizationBackend = null;

        try {
            authorizationBackend = typedComponentRegistry.create(LegacyAuthorizationBackend.class, type,
                    docNode.getAsNode("authorization_backend", "config"), context);

        } catch (ConfigValidationException e) {
            validationErrors.add(type, e);
        } catch (NoSuchComponentException e) {
            validationErrors.add(new InvalidAttributeValue("authorization_backend.type", type, e.getAvailableTypesAsInfoString()).cause(e));
        } catch (Exception e) {
            log.error("Unexpected exception while creating authorization backend " + type, e);
            validationErrors.add(new ValidationError(type, e.getMessage()).cause(e));
        }

        if (skipUsers != null && skipUsers.size() > 0) {
            final UserInformationBackend original = authorizationBackend;

            authorizationBackend = new LegacyAuthorizationBackend() {
                @Override
                public String getType() {
                    return original.getType();
                }

                @Override
                public void fillRoles(User user, AuthCredentials credentials) throws AuthenticatorUnavailableException {
                    if (!WildcardMatcher.matchAny(skipUsers, credentials.getName())) {
                        fillRoles(user, credentials);
                    }
                }
            };
        }

        validationErrors.throwExceptionForPresentErrors();

        return Optional.of(authorizationBackend);
    }

    static ValidationResult<ImmutableList<AuthenticationDomain<TransportAuthenticationDomain.TransportAuthenticationFrontend>>> parseTransportAuthenticationDomains(
            DocNode docNode, ConfigurationRepository.Context context, ImmutableList<LegacyAuthorizationBackend> authorizationBackends) {

        DocNode authcNode = docNode.getAsNode("dynamic", "authc");

        if (authcNode.isNull()) {
            return new ValidationResult<ImmutableList<AuthenticationDomain<TransportAuthenticationDomain.TransportAuthenticationFrontend>>>(
                    ImmutableList.empty());
        }

        if (!authcNode.isMap()) {
            return new ValidationResult<ImmutableList<AuthenticationDomain<TransportAuthenticationDomain.TransportAuthenticationFrontend>>>(
                    ImmutableList.empty(), new InvalidAttributeValue("dynamic.authc", null, "A mapping from auth domain names to definitions"));
        }

        ImmutableList.Builder<AuthenticationDomain<TransportAuthenticationDomain.TransportAuthenticationFrontend>> domains = new ImmutableList.Builder<>();

        ValidationErrors validationErrors = new ValidationErrors();

        for (Map.Entry<String, DocNode> entry : authcNode.toMapOfNodes().entrySet()) {
            String authDomainId = entry.getKey();

            try {
                domains.with(LegacyAuthenticationDomain.parseTransportDomain(authDomainId, entry.getValue(), context, authorizationBackends));
            } catch (ConfigValidationException e) {
                validationErrors.add("dynamic.authc." + authDomainId, e);
            }
        }

        return new ValidationResult<ImmutableList<AuthenticationDomain<TransportAuthenticationFrontend>>>(
                domains.build(
                        (a, b) -> Integer.compare(((LegacyAuthenticationDomain<?>) a).getOrder(), ((LegacyAuthenticationDomain<?>) b).getOrder())),
                validationErrors);
    }

    public RestAuthcConfig getRestAuthcConfig() {
        return restAuthcConfig;
    }

    public TransportAuthcConfig getTransportAuthcConfig() {
        return transportAuthcConfig;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public DocNode getSource() {
        return source;
    }

    public SearchGuardLicenseKey getLicense() {
        return license;
    }

}
