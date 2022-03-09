/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.transport;

import java.util.List;

import com.floragunn.codova.config.net.CacheConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.base.IPAddressAcceptanceRules;
import com.floragunn.searchguard.authc.transport.TransportAuthenticationDomain.TransportAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;

public class TransportAuthcConfig implements Document<TransportAuthcConfig> {

    private final DocNode source;
    private final ImmutableList<AuthenticationDomain<TransportAuthenticationFrontend>> authenticators;
    private final boolean debugEnabled;
    private final CacheConfig userCacheConfig;
    private final Network network;

    public TransportAuthcConfig(DocNode source, ImmutableList<AuthenticationDomain<TransportAuthenticationFrontend>> authenticators, Network network,
            CacheConfig userCacheConfig, boolean debugEnabled) {
        super();
        this.source = source;
        this.authenticators = authenticators;
        this.userCacheConfig = userCacheConfig;
        this.debugEnabled = debugEnabled;
        this.network = network;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public static ValidationResult<TransportAuthcConfig> parse(DocNode docNode, ConfigurationRepository.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        List<AuthenticationDomain<TransportAuthenticationFrontend>> authenticators = vNode.get("authenticators")
                .asList((n) -> TransportAuthenticationDomain.parse(n, context));

        boolean debugEnabled = vNode.get("debug").withDefault(false).asBoolean();
        CacheConfig userCacheConfig = vNode.get("user_cache").withDefault(CacheConfig.DEFAULT).by(CacheConfig::new);
        Network network = vNode.get("network").by(Network::parse);

        vNode.checkForUnusedAttributes();

        return new ValidationResult<>(new TransportAuthcConfig(docNode, ImmutableList.of(authenticators), network, userCacheConfig, debugEnabled));
    }

    public ImmutableList<AuthenticationDomain<TransportAuthenticationFrontend>> getAuthenticators() {
        return authenticators;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public CacheConfig getUserCacheConfig() {
        return userCacheConfig;
    }

    public static class Network {
        private final IPAddressAcceptanceRules ipAddressAcceptanceRules;

        public Network(IPAddressAcceptanceRules ipAddressAcceptanceRules) {
            this.ipAddressAcceptanceRules = ipAddressAcceptanceRules;
        }

        static Network parse(DocNode docNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

            IPAddressAcceptanceRules acceptanceRules = new IPAddressAcceptanceRules(vNode.get("accept").by(IPAddressAcceptanceRules.Criteria::parse),
                    vNode.get("deny").by(IPAddressAcceptanceRules.Criteria::parse));

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return new Network(acceptanceRules);
        }

        public IPAddressAcceptanceRules getIpAddressAcceptanceRules() {
            return ipAddressAcceptanceRules;
        }

    }

    public Network getNetwork() {
        return network;
    }
}
