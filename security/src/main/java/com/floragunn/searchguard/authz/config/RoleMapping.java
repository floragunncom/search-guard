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

package com.floragunn.searchguard.authz.config;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.transport.TransportAddress;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.IPAddressCollection;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.support.PatternMap;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressNetwork.IPAddressGenerator;

public class RoleMapping implements Document<RoleMapping>, Hideable {

    public static ValidationResult<RoleMapping> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        boolean reserved = vNode.get("reserved").withDefault(false).asBoolean();
        boolean hidden = vNode.get("hidden").withDefault(false).asBoolean();
        Pattern backendRoles = vNode.get("backend_roles").by(Pattern::parse);
        Pattern hosts = vNode.get("hosts").by(Pattern::parse);
        Pattern users = vNode.get("users").by(Pattern::parse);
        ImmutableSet<Pattern> andBackendRoles = vNode.hasNonNull("and_backend_roles")
                ? ImmutableSet.of(vNode.get("and_backend_roles").asList().ofObjectsParsedBy(Pattern::parse))
                : null;
        IPAddressCollection ips = vNode.get("ips").by(IPAddressCollection::parse);
        String description = vNode.get("description").asString();

        vNode.checkForUnusedAttributes();

        return new ValidationResult<RoleMapping>(
                new RoleMapping(docNode, reserved, hidden, backendRoles, users, hosts, ips, andBackendRoles, description), validationErrors);
    }

    private static final Logger log = LogManager.getLogger(RoleMapping.class);

    private final DocNode source;
    private final boolean reserved;
    private final boolean hidden;
    private final Pattern backendRoles;
    private final Pattern users;
    private final Pattern hosts;
    private final IPAddressCollection ips;
    private final ImmutableSet<Pattern> andBackendRoles;

    private final String description;

    public RoleMapping(DocNode docNode, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        this.source = docNode;
        this.reserved = vNode.get("reserved").withDefault(false).asBoolean();
        this.hidden = vNode.get("hidden").withDefault(false).asBoolean();
        this.backendRoles = vNode.get("backend_roles").by(Pattern::parse);
        this.hosts = vNode.get("hosts").by(Pattern::parse);
        this.users = vNode.get("users").by(Pattern::parse);
        this.andBackendRoles = vNode.hasNonNull("and_backend_roles")
                ? ImmutableSet.of(vNode.get("and_backend_roles").asList().ofObjectsParsedBy(Pattern::parse))
                : null;
        this.ips = vNode.get("ips").by(IPAddressCollection::parse);
        this.description = vNode.get("description").asString();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();
    }

    public RoleMapping(DocNode source, boolean reserved, boolean hidden, Pattern backendRoles, Pattern users, Pattern hosts, IPAddressCollection ips,
            ImmutableSet<Pattern> andBackendRoles, String description) {
        super();
        this.source = source;
        this.reserved = reserved;
        this.hidden = hidden;
        this.backendRoles = backendRoles;
        this.users = users;
        this.hosts = hosts;
        this.ips = ips;
        this.andBackendRoles = andBackendRoles;
        this.description = description;
    }

    public boolean isReserved() {
        return reserved;
    }

    public boolean isHidden() {
        return hidden;
    }

    public Pattern getBackendRoles() {
        return backendRoles;
    }

    public Pattern getHosts() {
        return hosts;
    }

    public IPAddressCollection getIps() {
        return ips;
    }

    public Pattern getUsers() {
        return users;
    }

    public ImmutableSet<Pattern> getAndBackendRoles() {
        return andBackendRoles;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public static class InvertedIndex {
        private final PatternMap<String> byUsers;
        private final PatternMap<String> byBackendRoles;
        private final PatternMap<String> byHostNames;
        private final ImmutableMap<IPAddressCollection, ImmutableSet<String>> byIps;

        /**
         * @deprecated Undocumented: Backend roles which must existed "and'ed-together"
         */
        private final ImmutableMap<ImmutableSet<Pattern>, ImmutableSet<String>> byBackendRolesAnded;
        private static final IPAddressGenerator ipAddressGenerator = new IPAddressGenerator();

        public InvertedIndex(SgDynamicConfiguration<RoleMapping> roleMappings) {

            PatternMap.Builder<String> users = new PatternMap.Builder<>();
            PatternMap.Builder<String> backendRoles = new PatternMap.Builder<>();
            PatternMap.Builder<String> hosts = new PatternMap.Builder<>();

            ListMultimap<String, String> ips = ArrayListMultimap.create();
            ListMultimap<ImmutableSet<Pattern>, String> andBackendRoles = ArrayListMultimap.create();

            for (Entry<String, RoleMapping> entry : roleMappings.getCEntries().entrySet()) {
                String role = entry.getKey();
                RoleMapping mapping = entry.getValue();

                users.add(mapping.getUsers(), role);
                backendRoles.add(mapping.getBackendRoles(), role);
                hosts.add(mapping.getHosts(), role);

                if (mapping.getIps() != null && !mapping.getIps().getSource().isEmpty()) {
                    for (String ip : mapping.getIps().getSource()) {
                        ips.put(ip, role);
                    }
                }

                if (mapping.getAndBackendRoles() != null && !mapping.getAndBackendRoles().isEmpty()) {
                    andBackendRoles.put(ImmutableSet.of(mapping.getAndBackendRoles()), role);
                }
            }

            this.byUsers = users.build();
            this.byBackendRoles = backendRoles.build();
            this.byHostNames = hosts.build();
            this.byIps = ImmutableMap.map(ips.asMap(), (k) -> ip(k), (v) -> ImmutableSet.of(v));
            this.byBackendRolesAnded = ImmutableMap.map(andBackendRoles.asMap(), (k) -> k, (v) -> ImmutableSet.of(v));
        }

        public ImmutableSet<String> evaluate(User user, TransportAddress transportAddress,
                ConfigConstants.RolesMappingResolution rolesMappingResolution) {

            if (user == null) {
                return ImmutableSet.empty();
            }

            ImmutableSet.Builder<String> result = new ImmutableSet.Builder<String>(user.getSearchGuardRoles());

            if (rolesMappingResolution == ConfigConstants.RolesMappingResolution.BOTH
                    || rolesMappingResolution == ConfigConstants.RolesMappingResolution.BACKENDROLES_ONLY) {
                result.addAll(user.getRoles());
            }

            if (((rolesMappingResolution == ConfigConstants.RolesMappingResolution.BOTH
                    || rolesMappingResolution == ConfigConstants.RolesMappingResolution.MAPPING_ONLY))) {

                result.addAll(byUsers.get(user.getName()));
                result.addAll(byBackendRoles.get(user.getRoles()));

                if (transportAddress != null) {
                    if (!byHostNames.isEmpty()) {
                        // The following may trigger a reverse DNS lookup
                        result.addAll(byHostNames.get(transportAddress.address().getHostName()));
                        // Backwards compatibility:
                        result.addAll(byHostNames.get(transportAddress.getAddress()));
                    }

                    if (!byIps.isEmpty()) {
                        IPAddress ipAddress = ipAddressGenerator.from(transportAddress.address().getAddress());

                        for (Map.Entry<IPAddressCollection, ImmutableSet<String>> entry : byIps.entrySet()) {
                            if (entry.getKey().contains(ipAddress)) {
                                result.addAll(entry.getValue());
                            }
                        }
                    }
                }

                if (!byBackendRolesAnded.isEmpty()) {
                    for (ImmutableSet<Pattern> patternSet : byBackendRolesAnded.keySet()) {
                        if (patternSet.forAllApplies((p) -> p.matches(user.getRoles()))) {
                            result.addAll(byBackendRolesAnded.get(patternSet));
                        }
                    }
                }
            }

            return result.build();
        }

        private static IPAddressCollection ip(String source) {
            try {
                return IPAddressCollection.parse(Collections.singletonList(source));
            } catch (Exception e) {
                // This should not happen, as the pattern has been complied before 
                log.error("Error while compiling IP address " + source, e);
                return null;
            }
        }
    }

}
