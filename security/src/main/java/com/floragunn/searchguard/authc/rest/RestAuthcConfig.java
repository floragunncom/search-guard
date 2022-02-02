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

package com.floragunn.searchguard.authc.rest;

import java.util.List;
import java.util.regex.Pattern;

import com.floragunn.codova.config.net.CacheConfig;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.base.IPAddressAcceptanceRules;
import com.floragunn.searchguard.authc.base.StandardAuthenticationDomain;
import com.floragunn.searchguard.authc.rest.authenticators.HTTPAuthenticator;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.support.IPAddressCollection;
import com.floragunn.searchsupport.util.ImmutableList;

public class RestAuthcConfig implements PatchableDocument<RestAuthcConfig> {

    private final DocNode source;
    private final ImmutableList<AuthenticationDomain<HTTPAuthenticator>> authenticators;
    private final Network network;
    private final boolean debugEnabled;
    private final CacheConfig userCacheConfig;

    public RestAuthcConfig(DocNode source, ImmutableList<AuthenticationDomain<HTTPAuthenticator>> authenticators, Network network,
            CacheConfig userCacheConfig, boolean debugEnabled) {
        super();
        this.source = source;
        this.authenticators = authenticators;
        this.network = network;
        this.debugEnabled = debugEnabled;
        this.userCacheConfig = userCacheConfig;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public static ValidationResult<RestAuthcConfig> parse(DocNode docNode, ConfigurationRepository.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        List<AuthenticationDomain<HTTPAuthenticator>> authDomain = vNode.get("auth_domains")
                .asList((n) -> StandardAuthenticationDomain.parse(n, HTTPAuthenticator.class, context));

        Network network = vNode.get("network").by(Network::parse);

        boolean debugEnabled = vNode.get("debug").withDefault(false).asBoolean();

        CacheConfig userCacheConfig = vNode.get("user_cache").withDefault(CacheConfig.DEFAULT).by(CacheConfig::new);

        vNode.checkForUnusedAttributes();

        return new ValidationResult<>(new RestAuthcConfig(docNode, ImmutableList.of(authDomain), network, userCacheConfig, debugEnabled),
                validationErrors);
    }

    public static RestAuthcConfig empty(DocNode docNode) {
        return new RestAuthcConfig(docNode, ImmutableList.empty(), null, CacheConfig.DEFAULT, false);
    }

    public static class Network {
        private final IPAddressCollection trustedProxies;

        /**
         * @deprecated just for supporting legacy configuration
         */
        @Deprecated
        private final Pattern trustedProxiesPattern;
        private final String remoteIpHttpHeader;
        private final IPAddressAcceptanceRules ipAddressAcceptanceRules;

        public Network(IPAddressCollection trustedProxies, IPAddressAcceptanceRules ipAddressAcceptanceRules, Pattern trustedProxiesPattern,
                String remoteIpHttpHeader) {
            this.trustedProxies = trustedProxies;
            this.trustedProxiesPattern = trustedProxiesPattern;
            this.remoteIpHttpHeader = remoteIpHttpHeader;
            this.ipAddressAcceptanceRules = ipAddressAcceptanceRules;
        }

        static Network parse(DocNode docNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

            IPAddressCollection trustedProxies = vNode.get("trusted_proxies").by(IPAddressCollection::parse);
            Pattern trustedProxiesPattern = vNode.get("trusted_proxies_regex").asPattern();
            String remoteIpHttpHeader = vNode.get("http.remote_ip_header").withDefault("X-Forwarded-For").asString();
            IPAddressAcceptanceRules acceptanceRules = new IPAddressAcceptanceRules(vNode.get("accept").by(IPAddressAcceptanceRules.Criteria::parse),
                    vNode.get("deny").by(IPAddressAcceptanceRules.Criteria::parse));

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return new Network(trustedProxies, acceptanceRules, trustedProxiesPattern, remoteIpHttpHeader);
        }

        public static Network parseLegacy(DocNode docNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

            Pattern trustedProxiesPattern = vNode.get("internalProxies").asPattern();
            String remoteIpHttpHeader = vNode.get("remoteIpHeader").withDefault("X-Forwarded-For").asString();

            validationErrors.throwExceptionForPresentErrors();

            return new Network(null, IPAddressAcceptanceRules.ANY, trustedProxiesPattern, remoteIpHttpHeader);
        }

        public IPAddressCollection getTrustedProxies() {
            return trustedProxies;
        }

        /**
         * @deprecated just for supporting legacy configuration
         */
        @Deprecated
        public Pattern getTrustedProxiesPattern() {
            return trustedProxiesPattern;
        }

        public boolean hasTrustedProxies() {
            return trustedProxies != null || trustedProxiesPattern != null;
        }

        public String getRemoteIpHttpHeader() {
            return remoteIpHttpHeader;
        }

        public IPAddressAcceptanceRules getIpAddressAcceptanceRules() {
            return ipAddressAcceptanceRules;
        }

    }

    public Network getNetwork() {
        return network;
    }

    public ImmutableList<AuthenticationDomain<HTTPAuthenticator>> getAuthenticators() {
        return authenticators;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public CacheConfig getUserCacheConfig() {
        return userCacheConfig;
    }

    @Override
    public String toString() {
        return "RestAuthcConfig [authenticators=" + authenticators + ", network=" + network + ", debugEnabled=" + debugEnabled + ", userCacheConfig="
                + userCacheConfig + "]";
    }

    @Override
    public RestAuthcConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

}
