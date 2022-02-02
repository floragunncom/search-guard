/*
 * Copyright 2015-2018 floragunn GmbH
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

package com.floragunn.searchguard.sgconf;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexAbstraction.Type;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authc.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.authc.blocking.IpRangeVerdictBasedBlockRegistry;
import com.floragunn.searchguard.authc.blocking.VerdictBasedBlockRegistry;
import com.floragunn.searchguard.authc.blocking.WildcardVerdictBasedBlockRegistry;
import com.floragunn.searchguard.authz.RoleMapping;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationException;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationResult;
import com.floragunn.searchguard.sgconf.SgRoles.TenantPermissions;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7.ExcludeIndex;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7.Index;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.StringInterpolationException;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.CheckTable;
import com.floragunn.searchsupport.util.ImmutableSet;
import com.google.common.collect.Sets;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;

public class ConfigModelV7 extends ConfigModel {

    private static final Logger log = LogManager.getLogger(ConfigModelV7.class);
    private static final String KIBANA_ALL_SAVED_OBJECTS_WRITE = "kibana:saved_objects/*/write";
    private static final Set<String> KIBANA_ALL_SAVED_OBJECTS_WRITE_SET = ImmutableSet.of(KIBANA_ALL_SAVED_OBJECTS_WRITE);

    private static boolean dfmEmptyOverridesAll;

    private ConfigConstants.RolesMappingResolution rolesMappingResolution;
    private ActionGroups actionGroups;
    private SgRoles sgRoles;
    private RoleMapping.InvertedIndex invertedRoleMappings;
    private SgDynamicConfiguration<TenantV7> tenants;
    private ClientBlockRegistry<InetAddress> blockedIpAddresses;
    private ClientBlockRegistry<String> blockedUsers;
    private ClientBlockRegistry<IPAddress> blockeNetmasks;

    public ConfigModelV7(SgDynamicConfiguration<RoleV7> roles, SgDynamicConfiguration<RoleMapping> rolemappings,
            SgDynamicConfiguration<ActionGroupsV7> actiongroups, SgDynamicConfiguration<TenantV7> tenants, SgDynamicConfiguration<BlocksV7> blocks,
            Settings esSettings) {

        this.tenants = tenants;

        try {
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.valueOf(esSettings
                    .get(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, ConfigConstants.RolesMappingResolution.MAPPING_ONLY.toString())
                    .toUpperCase());
        } catch (Exception e) {
            log.error("Cannot apply roles mapping resolution", e);
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.MAPPING_ONLY;
        }

        actionGroups = new ActionGroups(actiongroups);
        sgRoles = reload(roles);
        invertedRoleMappings = new RoleMapping.InvertedIndex(rolemappings);
        blockedIpAddresses = reloadBlockedIpAddresses(blocks);
        blockedUsers = reloadBlockedUsers(blocks);
        blockeNetmasks = reloadBlockedNetmasks(blocks);
        ConfigModelV7.dfmEmptyOverridesAll = esSettings.getAsBoolean(ConfigConstants.SEARCHGUARD_DFM_EMPTY_OVERRIDES_ALL, false);

    }

    private ClientBlockRegistry<IPAddress> reloadBlockedNetmasks(SgDynamicConfiguration<BlocksV7> blocks) {
        Function<String, Optional<IPAddress>> parsedIp = s -> {
            IPAddressString ipAddressString = new IPAddressString(s);
            try {
                ipAddressString.validate();
                return Optional.of(ipAddressString.toAddress());
            } catch (AddressStringException e) {
                log.error("Reloading blocked IP addresses failed ", e);
                return Optional.empty();
            }
        };

        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.net_mask);
        Set<IPAddress> allows = b.v1().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());
        Set<IPAddress> disallows = b.v2().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());

        return new IpRangeVerdictBasedBlockRegistry(allows, disallows);
    }

    private ClientBlockRegistry<String> reloadBlockedUsers(SgDynamicConfiguration<BlocksV7> blocks) {
        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.name);
        return new WildcardVerdictBasedBlockRegistry(b.v1(), b.v2());
    }

    private ClientBlockRegistry<InetAddress> reloadBlockedIpAddresses(SgDynamicConfiguration<BlocksV7> blocks) {
        Function<String, Optional<InetAddress>> parsedIp = s -> {
            try {
                return Optional.of(InetAddress.getByName(s));
            } catch (UnknownHostException e) {
                log.error("Reloading blocked IP addresses failed", e);
                return Optional.empty();
            }
        };

        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.ip);
        Set<InetAddress> allows = b.v1().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());
        Set<InetAddress> disallows = b.v2().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toSet());

        return new VerdictBasedBlockRegistry<>(InetAddress.class, allows, disallows);
    }

    private Tuple<Set<String>, Set<String>> readBlocks(SgDynamicConfiguration<BlocksV7> blocks, BlocksV7.Type type) {
        Set<String> allows = new HashSet<>();
        Set<String> disallows = new HashSet<>();

        List<BlocksV7> blocksV7s = blocks.getCEntries().values().stream().filter(b -> b.getType() == type).collect(Collectors.toList());

        for (BlocksV7 blocksV7 : blocksV7s) {
            if (blocksV7.getVerdict() == null) {
                log.error("No verdict type found in blocks");
                continue;
            }
            if (blocksV7.getVerdict() == BlocksV7.Verdict.disallow) {
                disallows.addAll(blocksV7.getValue());
            } else if (blocksV7.getVerdict() == BlocksV7.Verdict.allow) {
                allows.addAll(blocksV7.getValue());
            } else {
                log.error("Found unknown verdict type: " + blocksV7.getVerdict());
            }
        }
        return new Tuple<>(allows, disallows);
    }

    public Set<String> getAllConfiguredTenantNames() {
        return Collections.unmodifiableSet(tenants.getCEntries().keySet());
    }

    public SgRoles getSgRoles() {
        return sgRoles;
    }

    public ActionGroups getActionGroups() {
        return actionGroups;
    }

    @Override
    public List<ClientBlockRegistry<InetAddress>> getBlockIpAddresses() {
        return Collections.singletonList(blockedIpAddresses);
    }

    @Override
    public List<ClientBlockRegistry<String>> getBlockedUsers() {
        return Collections.singletonList(blockedUsers);
    }

    @Override
    public List<ClientBlockRegistry<IPAddress>> getBlockedNetmasks() {
        return Collections.singletonList(blockeNetmasks);
    }

    @Override
    public boolean isTenantValid(String requestedTenant) {

        if ("SGS_GLOBAL_TENANT".equals(requestedTenant) || ConfigModel.USER_TENANT.equals(requestedTenant)) {
            return true;
        }

        return getAllConfiguredTenantNames().contains(requestedTenant);
    }

    private SgRoles reload(SgDynamicConfiguration<RoleV7> settings) {

        final Set<Future<SgRole>> futures = new HashSet<>(5000);
        final ExecutorService execs = Executors.newFixedThreadPool(10);

        for (Entry<String, RoleV7> sgRole : settings.getCEntries().entrySet()) {

            Future<SgRole> future = execs.submit(() -> {
                if (sgRole.getValue() == null) {
                    return null;
                }

                return SgRole.create(sgRole.getKey(), sgRole.getValue(), actionGroups);
            });

            futures.add(future);
        }

        execs.shutdown();
        try {
            execs.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted (1) while loading roles");
            return null;
        }

        try {
            SgRoles _sgRoles = new SgRoles(futures.size());
            for (Future<SgRole> future : futures) {
                _sgRoles.addSgRole(future.get());
            }

            return _sgRoles;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted (2) while loading roles");
            return null;
        } catch (ExecutionException e) {
            log.error("Error while updating roles: {}", e.getCause(), e.getCause());
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    //beans
    public static class SgRoles extends com.floragunn.searchguard.sgconf.SgRoles implements ToXContentObject {

        public static SgRoles create(SgDynamicConfiguration<RoleV7> settings, ActionGroups actionGroups) throws ConfigValidationException {

            SgRoles result = new SgRoles(settings.getCEntries().size());

            for (Entry<String, RoleV7> entry : settings.getCEntries().entrySet()) {

                if (entry.getValue() == null) {
                    continue;
                }

                result.addSgRole(SgRole.create(entry.getKey(), entry.getValue(), actionGroups));
            }

            return result;
        }

        protected final Logger log = LogManager.getLogger(this.getClass());

        final Map<String, SgRole> roles;

        private SgRoles(int roleCount) {
            roles = new HashMap<>(roleCount);
        }

        private SgRoles addSgRole(SgRole sgRole) {
            if (sgRole != null) {
                this.roles.put(sgRole.getName(), sgRole);
            }
            return this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((roles == null) ? 0 : roles.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SgRoles other = (SgRoles) obj;
            if (roles == null) {
                if (other.roles != null)
                    return false;
            } else if (!roles.equals(other.roles))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "roles=" + roles;
        }

        public Collection<SgRole> getRoles() {
            return Collections.unmodifiableCollection(roles.values());
        }

        public Set<String> getRoleNames() {
            return Collections.unmodifiableSet(roles.keySet());
        }

        public SgRoles filter(Set<String> keep) {
            SgRoles result = new SgRoles(keep.size());

            for (String roleName : keep) {
                SgRole role = roles.get(roleName);

                if (role != null) {
                    result.addSgRole(role);
                }
            }

            return result;
        }

        @Override
        public EvaluatedDlsFlsConfig getDlsFls(User user, IndexNameExpressionResolver resolver, ClusterService cs,
                NamedXContentRegistry namedXContentRegistry) {

            if (!containsDlsFlsConfig()) {
                if (log.isDebugEnabled()) {
                    log.debug("No fls or dls found for {} in {} sg roles", user, roles.size());
                }

                return EvaluatedDlsFlsConfig.EMPTY;
            }

            Map<String, Set<String>> dlsQueriesByIndex = new HashMap<String, Set<String>>();
            Map<String, Set<String>> flsFields = new HashMap<String, Set<String>>();
            Map<String, Set<String>> maskedFieldsMap = new HashMap<String, Set<String>>();

            Set<String> noDlsConcreteIndices = new HashSet<>();
            Set<String> noFlsConcreteIndices = new HashSet<>();
            Set<String> noMaskedFieldConcreteIndices = new HashSet<>();

            for (SgRole sgr : roles.values()) {
                for (IndexPattern ip : sgr.getIpatterns()) {
                    String[] concreteIndices;

                    try {
                        concreteIndices = ip.getResolvedIndexPatterns(user, resolver, cs, false);
                    } catch (StringInterpolationException e) {
                        throw new ElasticsearchSecurityException("Invalid index pattern in role " + sgr.getName() + ": " + ip.indexPattern, e);
                    }

                    try {
                        String dls = ip.getDlsQuery(user);

                        if (dls != null && dls.length() > 0) {

                            for (int i = 0; i < concreteIndices.length; i++) {
                                dlsQueriesByIndex.computeIfAbsent(concreteIndices[i], (key) -> new HashSet<String>()).add(dls);
                            }
                        } else if (dfmEmptyOverridesAll) {
                            noDlsConcreteIndices.addAll(Arrays.asList(concreteIndices));
                        }

                    } catch (StringInterpolationException e) {
                        throw new ElasticsearchSecurityException("Invalid DLS query in role " + sgr.getName() + ": " + ip.dlsQuery, e);
                    }

                    Set<String> fls = ip.getFls();

                    if (fls != null && fls.size() > 0) {

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (flsFields.containsKey(ci)) {
                                flsFields.get(ci).addAll(Sets.newHashSet(fls));
                            } else {
                                flsFields.put(ci, new HashSet<String>());
                                flsFields.get(ci).addAll(Sets.newHashSet(fls));
                            }
                        }
                    } else {
                        noFlsConcreteIndices.addAll(Arrays.asList(concreteIndices));
                    }

                    Set<String> maskedFields = ip.getMaskedFields();

                    if (maskedFields != null && maskedFields.size() > 0) {

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (maskedFieldsMap.containsKey(ci)) {
                                maskedFieldsMap.get(ci).addAll(Sets.newHashSet(maskedFields));
                            } else {
                                maskedFieldsMap.put(ci, new HashSet<String>());
                                maskedFieldsMap.get(ci).addAll(Sets.newHashSet(maskedFields));
                            }
                        }
                    } else if (dfmEmptyOverridesAll) {
                        noMaskedFieldConcreteIndices.addAll(Arrays.asList(concreteIndices));
                    }
                }
            }

            if (dfmEmptyOverridesAll) {
                if (log.isDebugEnabled()) {
                    log.debug("Index patterns with no dls queries attached: {} - They will be removed from {}", noDlsConcreteIndices,
                            dlsQueriesByIndex.keySet());
                    log.debug("Index patterns with no fls fields attached: {} - They will be removed from {}", noFlsConcreteIndices,
                            flsFields.keySet());
                    log.debug("Index patterns with no masked fields attached: {} - They will be removed from {}", noMaskedFieldConcreteIndices,
                            maskedFieldsMap.keySet());
                }

                WildcardMatcher.wildcardRemoveFromSet(dlsQueriesByIndex.keySet(), noDlsConcreteIndices);
                WildcardMatcher.wildcardRemoveFromSet(flsFields.keySet(), noFlsConcreteIndices);
                WildcardMatcher.wildcardRemoveFromSet(maskedFieldsMap.keySet(), noMaskedFieldConcreteIndices);
            }

            return new EvaluatedDlsFlsConfig(dlsQueriesByIndex, flsFields, maskedFieldsMap);
        }

        //kibana special only, terms eval
        public ImmutableSet<String> getAllPermittedIndicesForKibana(ActionRequestInfo requestInfo, User user, Set<String> actions,
                IndexNameExpressionResolver resolver, ClusterService cs, ActionRequestIntrospector actionRequestIntrospector) {
            ImmutableSet.Builder<String> resultBuilder = new ImmutableSet.Builder<String>();

            for (SgRole sgr : roles.values()) {
                sgr.getAllResolvedPermittedIndices(resultBuilder,
                        actionRequestIntrospector.create("*", IndicesOptions.LENIENT_EXPAND_OPEN).getResolvedIndices(), user, actions, resolver, cs);
            }

            ImmutableSet<String> result = resultBuilder.build();

            for (SgRole sgRole : roles.values()) {
                result = sgRole.removeAllResolvedExcludedIndices(result, user, actions, resolver, cs);
            }

            result = result.with(requestInfo.getResolvedIndices().getRemoteIndices());

            return result;
        }

        public boolean impliesClusterPermissionPermission(String action) {
            for (SgRole sgRole : roles.values()) {
                if (sgRole.excludesClusterPermission(action)) {
                    return false;
                }
            }

            for (SgRole sgRole : roles.values()) {
                if (sgRole.impliesClusterPermission(action)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public PrivilegesEvaluationResult impliesIndexPrivilege(PrivilegesEvaluationContext context, ResolvedIndices resolved,
                ImmutableSet<String> actions) throws PrivilegesEvaluationException {

            ImmutableSet<PrivilegesEvaluationResult.Error> errors = ImmutableSet.empty();

            if (resolved.isLocalAll()) {
                // If we have a query on all indices, first check for roles which give privileges for *. Thus, we avoid costly index resolution

                CheckTable<String, String> checkTable = CheckTable.create("*", actions);

                top: for (SgRole role : roles.values()) {
                    for (IndexPattern indexPattern : role.ipatterns) {
                        try {
                            if ("*".equals(indexPattern.getUnresolvedIndexPattern(context.getUser()))) {
                                if (checkTable.checkIf("*", (action) -> WildcardMatcher.matchAny(indexPattern.perms, action))) {
                                    break top;
                                }
                            }
                        } catch (StringInterpolationException e) {
                            if (log.isDebugEnabled()) {
                                log.debug("Invalid index pattern " + indexPattern.indexPattern, e);
                            }
                            errors = errors.with(new PrivilegesEvaluationResult.Error("Invalid index pattern " + indexPattern.indexPattern, e));
                        }
                    }
                }

                if (checkTable.isComplete() && !exclusionsPresent(context, resolved, checkTable)) {
                    return PrivilegesEvaluationResult.OK;
                }

                if (!context.isResolveLocalAll()) {
                    if (!checkTable.isComplete()) {
                        return PrivilegesEvaluationResult.INSUFFICIENT.reason("Insufficient privileges").with(checkTable);
                    } else {
                        return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privileges excluded").with(checkTable);
                    }
                }
            }

            if (resolved.getLocalIndices().isEmpty()) {
                log.debug("No local indices; grant the request");
                return PrivilegesEvaluationResult.OK;
            }

            CheckTable<String, String> checkTable = CheckTable.create(resolved.getLocalIndices(), actions);

            top: for (SgRole role : roles.values()) {
                for (IndexPattern indexPattern : role.ipatterns) {
                    try {
                        String[] resolvedIndexPatterns = indexPattern.getResolvedIndexPatterns(context.getUser(), context.getResolver(),
                                context.getClusterService(), true);
                        Set<String> matchingIndices = resolved.getLocalIndices()
                                .matching((index) -> WildcardMatcher.matchAny(resolvedIndexPatterns, index));

                        if (checkTable.checkIf(matchingIndices, (action) -> WildcardMatcher.matchAny(indexPattern.perms, action))) {
                            break top;
                        }
                    } catch (StringInterpolationException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Invalid index pattern " + indexPattern.indexPattern, e);
                        }
                        errors = errors.with(new PrivilegesEvaluationResult.Error("Invalid index pattern " + indexPattern.indexPattern, e));
                    }
                }
            }

            uncheckExclusions(context, resolved, checkTable);

            if (checkTable.isComplete()) {
                return PrivilegesEvaluationResult.OK;
            }

            ImmutableSet<String> availableIndices = checkTable.getCompleteRows();

            if (!availableIndices.isEmpty()) {
                return PrivilegesEvaluationResult.PARTIALLY_OK.availableIndices(availableIndices, checkTable);
            }

            return PrivilegesEvaluationResult.INSUFFICIENT.with(checkTable, errors);
        }

        private void uncheckExclusions(PrivilegesEvaluationContext context, ResolvedIndices resolved, CheckTable<String, String> checkTable)
                throws PrivilegesEvaluationException {

            for (SgRole sgRole : roles.values()) {
                for (ExcludedIndexPermissions indexPattern : sgRole.indexPermissionExclusions) {
                    CheckTable<String, String> subTable = checkTable.getViewForMatchingColumns((action) -> indexPattern.perms.matches(action));

                    if (!subTable.isEmpty()) {

                        try {
                            indexPattern.uncheckMatches(context, subTable);
                        } catch (StringInterpolationException e) {
                            // For interpolation errors we go here for the strict path and also exclude the action. Otherwise, exclusions might break unexpectedly.

                            String message = "Invalid index pattern " + indexPattern.indexPattern + " in permission exclusion.\n"
                                    + "In order to fail safely, the requested actions will be denied for all indices.";

                            if (log.isDebugEnabled()) {
                                log.debug(message, e);
                            }

                            throw new PrivilegesEvaluationException(message, e);
                        }
                    }

                }
            }
        }

        private boolean exclusionsPresent(PrivilegesEvaluationContext privilegesEvaluationContext, ResolvedIndices resolved,
                CheckTable<String, String> checkTable) {

            for (SgRole sgRole : roles.values()) {
                for (ExcludedIndexPermissions indexPattern : sgRole.indexPermissionExclusions) {
                    if (indexPattern.perms.matches(checkTable.getColumns())) {
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public Set<String> getClusterPermissions(User user) {
            Set<String> result = new HashSet<>();

            for (SgRole role : roles.values()) {
                result.addAll(role.getClusterPerms());
            }

            return result;
        }

        @Override
        public TenantPermissions getTenantPermissions(User user, String requestedTenant) {
            if (user == null) {
                return EMPTY_TENANT_PERMISSIONS;
            }

            if (USER_TENANT.equals(requestedTenant)) {
                return FULL_TENANT_PERMISSIONS;
            }

            Set<String> permissions = new HashSet<>();

            for (SgRole role : roles.values()) {
                for (Tenant tenant : role.getTenants()) {
                    try {
                        if (WildcardMatcher.match(tenant.getEvaluatedTenantPattern(user), requestedTenant)) {
                            permissions.addAll(tenant.getPermissions());
                        }
                    } catch (StringInterpolationException e) {
                        log.error("Error while evaluating tenant pattern '" + tenant.getTenantPattern() + "' of role '" + role.getName()
                                + "' for user " + user.getName() + "\nSkipping tenant pattern.", e);
                    }
                }
            }

            // TODO SG8: Remove this

            if ("SGS_GLOBAL_TENANT".equals(requestedTenant) && permissions.isEmpty() && (roles.containsKey("sg_kibana_user")
                    || roles.containsKey("SGS_KIBANA_USER") || roles.containsKey("sg_all_access") || roles.containsKey("SGS_ALL_ACCESS"))) {
                return RW_TENANT_PERMISSIONS;
            }

            return new TenantPermissionsImpl(permissions);
        }

        @Override
        public boolean hasTenantPermission(User user, String requestedTenant, String action) {

            TenantPermissions permissions = getTenantPermissions(user, requestedTenant);

            if (permissions == null || permissions.getPermissions().isEmpty()) {
                return false;
            }

            return WildcardMatcher.matchAny(permissions.getPermissions(), action);
        }

        @Override
        public Map<String, Boolean> mapTenants(User user, Set<String> allTenantNames) {

            if (user == null) {
                return Collections.emptyMap();
            }

            Map<String, Boolean> result = new HashMap<>(roles.size());
            result.put(user.getName(), true);

            for (SgRole role : roles.values()) {
                for (Tenant tenant : role.getTenants()) {
                    try {
                        String tenantPattern = tenant.getEvaluatedTenantPattern(user);
                        boolean rw = tenant.isReadWrite();

                        for (String tenantName : allTenantNames) {

                            if (WildcardMatcher.match(tenantPattern, tenantName)) {

                                if (rw || !result.containsKey(tenantName)) { //RW outperforms RO
                                    result.put(tenantName, rw);
                                }
                            }
                        }
                    } catch (StringInterpolationException e) {
                        log.error("Error while evaluating tenant pattern '" + tenant.getTenantPattern() + "' of role '" + role.getName()
                                + "' for user " + user.getName() + "\nSkipping tenant pattern.", e);
                    }
                }
            }

            // TODO SG8: Remove this

            if (!result.containsKey("SGS_GLOBAL_TENANT") && (roles.containsKey("sg_kibana_user") || roles.containsKey("SGS_KIBANA_USER")
                    || roles.containsKey("sg_all_access") || roles.containsKey("SGS_ALL_ACCESS"))) {
                result.put("SGS_GLOBAL_TENANT", true);
            }

            return Collections.unmodifiableMap(result);
        }

        private boolean containsDlsFlsConfig() {
            for (SgRole sgr : roles.values()) {
                for (IndexPattern ip : sgr.getIpatterns()) {
                    if (ip.hasDlsQuery() || ip.hasFlsFields() || ip.hasMaskedFields()) {
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startObject("_sg_meta");
            builder.field("type", "roles");
            builder.field("config_version", 2);
            builder.endObject();

            for (SgRole role : roles.values()) {
                builder.field(role.getName(), role);
            }

            builder.endObject();
            return builder;
        }

    }

    public static class SgRole implements ToXContentObject {

        static SgRole create(String roleName, RoleV7 roleConfig, ActionGroups actionGroups) throws ConfigValidationException {
            SgRole result = new SgRole(roleName);

            final Set<String> permittedClusterActions = actionGroups.resolve(roleConfig.getCluster_permissions());
            result.addClusterPerms(permittedClusterActions);

            for (final Index permittedAliasesIndex : roleConfig.getIndex_permissions()) {

                final String dls = permittedAliasesIndex.getDls();
                final List<String> fls = permittedAliasesIndex.getFls();
                final List<String> maskedFields = permittedAliasesIndex.getMasked_fields();

                for (String pat : permittedAliasesIndex.getIndex_patterns()) {
                    IndexPattern _indexPattern = new IndexPattern(pat);
                    _indexPattern.setDlsQuery(dls);
                    _indexPattern.addFlsFields(fls);
                    _indexPattern.addMaskedFields(maskedFields);
                    _indexPattern.addPerm(actionGroups.resolve(permittedAliasesIndex.getAllowed_actions()));

                    result.addIndexPattern(_indexPattern);

                    if ("*".equals(pat)) {
                        // Map legacy config for actions which should have been marked as cluster permissions but were treated as index permissions
                        // These will be always configured for a * index pattern; other index patterns would not have worked for these actions in previous SG versions.

                        if (_indexPattern.getPerms().contains("indices:data/read/search/template")) {
                            result.clusterPerms.add("indices:data/read/search/template");
                        }

                        if (_indexPattern.getPerms().contains("indices:data/read/msearch/template")) {
                            result.clusterPerms.add("indices:data/read/msearch/template");
                        }
                    }

                }
            }

            if (roleConfig.getTenant_permissions() != null) {
                for (RoleV7.Tenant tenant : roleConfig.getTenant_permissions()) {
                    Set<String> resolvedTenantPermissions = actionGroups.resolve(tenant.getAllowed_actions());

                    for (String tenantPattern : tenant.getTenant_patterns()) {
                        // Important: Key can contain patterns
                        result.addTenant(new Tenant(tenantPattern, resolvedTenantPermissions));
                    }
                }
            }

            if (roleConfig.getExclude_cluster_permissions() != null) {
                result.clusterPermissionExclusions.addAll(actionGroups.resolve(roleConfig.getExclude_cluster_permissions()));
            }

            if (roleConfig.getExclude_index_permissions() != null) {
                for (ExcludeIndex excludedIndexPermissions : roleConfig.getExclude_index_permissions()) {
                    for (String pattern : excludedIndexPermissions.getIndex_patterns()) {
                        ExcludedIndexPermissions excludedIndexPattern = new ExcludedIndexPermissions(pattern,
                                actionGroups.resolve(excludedIndexPermissions.getActions()));
                        result.indexPermissionExclusions.add(excludedIndexPattern);
                    }
                }
            }

            return result;
        }

        private final String name;
        private final Set<Tenant> tenants = new HashSet<>();
        private final Set<IndexPattern> ipatterns = new HashSet<>();
        private final Set<String> clusterPerms = new HashSet<>();
        private final Set<ExcludedIndexPermissions> indexPermissionExclusions = new HashSet<>();
        private final Set<String> clusterPermissionExclusions = new HashSet<>();

        private SgRole(String name) {
            super();
            this.name = Objects.requireNonNull(name);
        }

        private boolean impliesClusterPermission(String action) {
            return WildcardMatcher.matchAny(clusterPerms, action);
        }

        private boolean excludesClusterPermission(String action) {
            return WildcardMatcher.matchAny(clusterPermissionExclusions, action);
        }

        //get indices which are permitted for the given types and actions

        //dnfof + kibana special only

        private void getAllResolvedPermittedIndices(ImmutableSet.Builder<String> result, ResolvedIndices resolved, User user, Set<String> actions,
                IndexNameExpressionResolver resolver, ClusterService cs) {

            for (IndexPattern p : ipatterns) {
                //what if we cannot resolve one (for create purposes)
                final boolean patternMatch = WildcardMatcher.matchAll(p.getPerms().toArray(new String[0]), actions);

                if (patternMatch) {
                    //resolved but can contain patterns for nonexistent indices
                    try {
                        String[] permitted = p.getResolvedIndexPatterns(user, resolver, cs, true);

                        result.with(resolved.getLocalIndices().matching((i) -> WildcardMatcher.matchAny(permitted, i)));

                        if (result.size() == resolved.getLocalIndices().size()) {
                            // We have already reached the maximum set. No need for further tests
                            break;
                        }
                    } catch (StringInterpolationException e) {
                        log.warn("Invalid index pattern " + p.indexPattern, e);
                    }
                }
            }

            //all that we want and all thats permitted of them
        }

        private ImmutableSet<String> removeAllResolvedExcludedIndices(ImmutableSet<String> indices, User user, Set<String> actions,
                IndexNameExpressionResolver resolver, ClusterService cs) {

            for (ExcludedIndexPermissions excludedIndexPattern : indexPermissionExclusions) {
                if (indices.isEmpty()) {
                    break;
                }

                if (excludedIndexPattern.perms.matches(actions)) {
                    if (excludedIndexPattern.indexPattern.equals("*")) {
                        return ImmutableSet.empty();
                    }

                    try {
                        indices = excludedIndexPattern.removeMatches(indices, user, resolver, cs);
                    } catch (StringInterpolationException e) {
                        log.warn("Invalid index pattern " + excludedIndexPattern.indexPattern + " in permission exclusion.\n"
                                + "In order to fail safely, the requested actions will be denied for all indices.", e);
                        return ImmutableSet.empty();
                    }
                }
            }

            return indices;
        }

        private SgRole addTenant(Tenant tenant) {
            if (tenant != null) {
                this.tenants.add(tenant);
            }
            return this;
        }

        private SgRole addIndexPattern(IndexPattern indexPattern) {
            if (indexPattern != null) {
                this.ipatterns.add(indexPattern);
            }
            return this;
        }

        private SgRole addClusterPerms(Collection<String> clusterPerms) {
            if (clusterPerms != null) {
                this.clusterPerms.addAll(clusterPerms);
            }
            return this;
        }

        public Set<IndexPattern> getIpatterns() {
            return Collections.unmodifiableSet(ipatterns);
        }

        public Set<String> getClusterPerms() {
            return Collections.unmodifiableSet(clusterPerms);
        }

        public Set<Tenant> getTenants() {
            return tenants;
        }

        public String getName() {
            return name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (this.clusterPerms != null && this.clusterPerms.size() > 0) {
                builder.field("cluster_permissions", this.clusterPerms);
            }

            if (ipatterns != null && ipatterns.size() > 0) {
                builder.field("index_permissions", ipatterns);
            }

            if (clusterPermissionExclusions != null && clusterPermissionExclusions.size() > 0) {
                builder.field("excluded_cluster_permissions", clusterPermissionExclusions);
            }

            if (indexPermissionExclusions != null && indexPermissionExclusions.size() > 0) {
                builder.field("excluded_index_permissions", indexPermissionExclusions);
            }

            if (tenants != null && tenants.size() > 0) {
                builder.field("tenant_permissions", tenants);
            }

            builder.endObject();

            return builder;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((clusterPermissionExclusions == null) ? 0 : clusterPermissionExclusions.hashCode());
            result = prime * result + ((clusterPerms == null) ? 0 : clusterPerms.hashCode());
            result = prime * result + ((indexPermissionExclusions == null) ? 0 : indexPermissionExclusions.hashCode());
            result = prime * result + ((ipatterns == null) ? 0 : ipatterns.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((tenants == null) ? 0 : tenants.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            SgRole other = (SgRole) obj;
            if (clusterPermissionExclusions == null) {
                if (other.clusterPermissionExclusions != null) {
                    return false;
                }
            } else if (!clusterPermissionExclusions.equals(other.clusterPermissionExclusions)) {
                return false;
            }
            if (clusterPerms == null) {
                if (other.clusterPerms != null) {
                    return false;
                }
            } else if (!clusterPerms.equals(other.clusterPerms)) {
                return false;
            }
            if (indexPermissionExclusions == null) {
                if (other.indexPermissionExclusions != null) {
                    return false;
                }
            } else if (!indexPermissionExclusions.equals(other.indexPermissionExclusions)) {
                return false;
            }
            if (ipatterns == null) {
                if (other.ipatterns != null) {
                    return false;
                }
            } else if (!ipatterns.equals(other.ipatterns)) {
                return false;
            }
            if (name == null) {
                if (other.name != null) {
                    return false;
                }
            } else if (!name.equals(other.name)) {
                return false;
            }
            if (tenants == null) {
                if (other.tenants != null) {
                    return false;
                }
            } else if (!tenants.equals(other.tenants)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "SgRole [name=" + name + ", tenants=" + tenants + ", ipatterns=" + ipatterns + ", clusterPerms=" + clusterPerms
                    + ", indexPermissionExclusions=" + indexPermissionExclusions + ", clusterPermissionExclusions=" + clusterPermissionExclusions
                    + "]";
        }

    }

    //sg roles
    public static class IndexPattern implements ToXContentObject {

        private final String indexPattern;
        private String dlsQuery;
        private final Set<String> fls = new HashSet<>();
        private final Set<String> maskedFields = new HashSet<>();
        private final Set<String> perms = new HashSet<>();

        public IndexPattern(String indexPattern) {
            super();
            this.indexPattern = Objects.requireNonNull(indexPattern);
        }

        public IndexPattern addFlsFields(List<String> flsFields) {
            if (flsFields != null) {
                this.fls.addAll(flsFields);
            }
            return this;
        }

        public IndexPattern addMaskedFields(List<String> maskedFields) {
            if (maskedFields != null) {
                this.maskedFields.addAll(maskedFields);
            }
            return this;
        }

        public IndexPattern addPerm(Set<String> perms) {
            if (perms != null) {
                this.perms.addAll(perms);
            }
            return this;
        }

        public IndexPattern setDlsQuery(String dlsQuery) {
            if (dlsQuery != null) {
                this.dlsQuery = dlsQuery;
            }
            return this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((dlsQuery == null) ? 0 : dlsQuery.hashCode());
            result = prime * result + ((fls == null) ? 0 : fls.hashCode());
            result = prime * result + ((maskedFields == null) ? 0 : maskedFields.hashCode());
            result = prime * result + ((indexPattern == null) ? 0 : indexPattern.hashCode());
            result = prime * result + ((perms == null) ? 0 : perms.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            IndexPattern other = (IndexPattern) obj;
            if (dlsQuery == null) {
                if (other.dlsQuery != null)
                    return false;
            } else if (!dlsQuery.equals(other.dlsQuery))
                return false;
            if (fls == null) {
                if (other.fls != null)
                    return false;
            } else if (!fls.equals(other.fls))
                return false;
            if (maskedFields == null) {
                if (other.maskedFields != null)
                    return false;
            } else if (!maskedFields.equals(other.maskedFields))
                return false;
            if (indexPattern == null) {
                if (other.indexPattern != null)
                    return false;
            } else if (!indexPattern.equals(other.indexPattern))
                return false;
            if (perms == null) {
                if (other.perms != null)
                    return false;
            } else if (!perms.equals(other.perms))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return System.lineSeparator() + "        indexPattern=" + indexPattern + System.lineSeparator() + "          dlsQuery=" + dlsQuery
                    + System.lineSeparator() + "          fls=" + fls + System.lineSeparator() + "          perms=" + perms;
        }

        public String getUnresolvedIndexPattern(User user) throws StringInterpolationException {
            return replaceProperties(indexPattern, user);
        }

        private String[] getResolvedIndexPatterns(User user, IndexNameExpressionResolver resolver, ClusterService cs, boolean appendUnresolved)
                throws StringInterpolationException {
            String unresolved = getUnresolvedIndexPattern(user);
            String[] resolved = null;
            if (WildcardMatcher.containsWildcard(unresolved)) {
                final String[] aliasesForPermittedPattern = cs.state().getMetadata().getIndicesLookup().entrySet().stream()
                        .filter(e -> e.getValue().getType().equals(Type.ALIAS)).filter(e -> WildcardMatcher.match(unresolved, e.getKey()))
                        .map(e -> e.getKey()).toArray(String[]::new);

                if (aliasesForPermittedPattern != null && aliasesForPermittedPattern.length > 0) {
                    resolved = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), aliasesForPermittedPattern);
                }
            }

            if (resolved == null && !unresolved.isEmpty()) {
                resolved = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), unresolved);
            }
            if (resolved == null || resolved.length == 0) {
                return new String[] { unresolved };
            } else {
                if (appendUnresolved) {
                    //append unresolved value for pattern matching
                    String[] retval = Arrays.copyOf(resolved, resolved.length + 1);
                    retval[retval.length - 1] = unresolved;
                    return retval;
                } else {
                    return resolved;
                }
            }
        }

        public String getDlsQuery(User user) throws StringInterpolationException {
            return replaceProperties(dlsQuery, user);
        }

        public boolean hasDlsQuery() {
            return dlsQuery != null && !dlsQuery.isEmpty();
        }

        public Set<String> getFls() {
            return Collections.unmodifiableSet(fls);
        }

        public boolean hasFlsFields() {
            return fls != null && !fls.isEmpty();
        }

        public Set<String> getMaskedFields() {
            return Collections.unmodifiableSet(maskedFields);
        }

        public boolean hasMaskedFields() {
            return maskedFields != null && !maskedFields.isEmpty();
        }

        public Set<String> getPerms() {
            return Collections.unmodifiableSet(perms);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            // TODO XXX
            builder.field("index_patterns", Collections.singletonList(indexPattern));

            if (dlsQuery != null) {
                builder.field("dls", dlsQuery);
            }

            if (fls != null && fls.size() > 0) {
                builder.field("fls", fls);
            }

            if (maskedFields != null && maskedFields.size() > 0) {
                builder.field("masked_fields", maskedFields);
            }

            if (perms != null && perms.size() > 0) {
                builder.field("allowed_actions", perms);
            }

            builder.endObject();
            return builder;
        }

    }

    public static class ExcludedIndexPermissions implements ToXContentObject {

        private final String indexPattern;

        private final Pattern perms;
        private final Set<String> permsAsString;

        public ExcludedIndexPermissions(String indexPattern, Set<String> perms) throws ConfigValidationException {
            super();
            this.indexPattern = Objects.requireNonNull(indexPattern);
            this.perms = Pattern.create(perms);
            this.permsAsString = perms;
        }

        public boolean matches(Set<String> indices, User user, IndexNameExpressionResolver resolver, ClusterService cs)
                throws StringInterpolationException {
            // Note: This does not process the obsolete user attributes any more
            String indexPattern = Attributes.replaceAttributes(this.indexPattern, user);

            if (log.isTraceEnabled()) {
                log.trace("matches(" + indices + ") on " + this.indexPattern + " => " + indexPattern);
            }

            if (indexPattern.equals("*")) {
                return true;
            }

            if (WildcardMatcher.containsWildcard(indexPattern)) {
                if (WildcardMatcher.matchAny(indexPattern, indices)) {
                    if (log.isTraceEnabled()) {
                        log.trace("Direct pattern match");
                    }
                    return true;
                }

                String[] aliasesForPermittedPattern = cs.state().getMetadata().getIndicesLookup().entrySet().stream()
                        .filter(e -> e.getValue().getType().equals(Type.ALIAS)).filter(e -> WildcardMatcher.match(indexPattern, e.getKey()))
                        .map(e -> e.getKey()).toArray(String[]::new);

                if (aliasesForPermittedPattern.length > 0) {
                    String[] resolvedAliases = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(),
                            aliasesForPermittedPattern);

                    for (String resolvedAlias : resolvedAliases) {
                        if (indices.contains(resolvedAlias)) {
                            if (log.isTraceEnabled()) {
                                log.trace("Match on alias: " + resolvedAlias + "; all resolved: " + Arrays.asList(resolvedAliases));
                            }

                            return true;
                        }
                    }
                }

                return false;
            } else {
                // No wildcard

                if (indices.contains(indexPattern)) {
                    if (log.isTraceEnabled()) {
                        log.trace("Direct match");
                    }

                    return true;
                }

                String[] resolvedAliases = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), indexPattern);

                for (String resolvedAlias : resolvedAliases) {
                    if (indices.contains(resolvedAlias)) {
                        if (log.isTraceEnabled()) {
                            log.trace("Match on alias: " + resolvedAlias + "; all resolved: " + Arrays.asList(resolvedAliases));
                        }

                        return true;
                    }
                }

                if (log.isTraceEnabled()) {
                    log.trace("No match on resolved aliases: " + Arrays.asList(resolvedAliases));
                }

                return false;
            }
        }

        public ImmutableSet<String> removeMatches(ImmutableSet<String> indices, User user, IndexNameExpressionResolver resolver, ClusterService cs)
                throws StringInterpolationException {
            // Note: This does not process the obsolete user attributes any more
            String indexPattern = Attributes.replaceAttributes(this.indexPattern, user);

            if (log.isTraceEnabled()) {
                log.trace("removeMatches(" + indices + ") on " + this.indexPattern + " => " + indexPattern);
            }

            if (indexPattern.equals("*")) {
                return ImmutableSet.empty();
            }

            if (WildcardMatcher.containsWildcard(indexPattern)) {
                indices = indices.withoutMatching((index) -> WildcardMatcher.match(indexPattern, index));

                if (log.isTraceEnabled()) {
                    log.trace("remaining indices after removing matches: " + indices);
                }

                if (indices.isEmpty()) {
                    return ImmutableSet.empty();
                }

                String[] aliasesForPermittedPattern = cs.state().getMetadata().getIndicesLookup().entrySet().stream()
                        .filter(e -> e.getValue().getType().equals(Type.ALIAS)).filter(e -> WildcardMatcher.match(indexPattern, e.getKey()))
                        .map(e -> e.getKey()).toArray(String[]::new);

                if (aliasesForPermittedPattern.length > 0) {

                    String[] resolvedAliases = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(),
                            aliasesForPermittedPattern);

                    indices = indices.without(Arrays.asList(resolvedAliases));

                    if (log.isTraceEnabled()) {
                        log.trace("remaining indices after removing matching aliases (" + Arrays.asList(resolvedAliases) + "): " + indices);
                    }
                }

            } else {
                // No wildcard

                indices = indices.without(indexPattern);

                if (log.isTraceEnabled()) {
                    log.trace("remaining indices after removing matches: " + indices);
                }

                if (indices.isEmpty()) {
                    return ImmutableSet.empty();
                }

                String[] resolvedAliases = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), indexPattern);

                indices = indices.without(Arrays.asList(resolvedAliases));

                if (log.isTraceEnabled()) {
                    log.trace("remaining indices after removing matching aliases (" + Arrays.asList(resolvedAliases) + "): " + indices);
                }

            }

            return indices;
        }

        public void uncheckMatches(PrivilegesEvaluationContext context, CheckTable<String, String> checkTable) throws StringInterpolationException {
            // Note: This does not process the obsolete user attributes any more
            String indexPattern = Attributes.replaceAttributes(this.indexPattern, context.getUser());

            if (log.isTraceEnabled()) {
                log.trace("removeMatches(" + checkTable.getRows() + ") on " + this.indexPattern + " => " + indexPattern);
            }

            if (indexPattern.equals("*")) {
                checkTable.uncheckAll();
                return;
            }

            if (WildcardMatcher.containsWildcard(indexPattern)) {
                checkTable.uncheckRowIf((index) -> WildcardMatcher.match(indexPattern, index));

                if (log.isTraceEnabled()) {
                    log.trace("remaining indices after removing matches: " + checkTable);
                }

                if (checkTable.isBlank()) {
                    return;
                }

                String[] aliasesForPermittedPattern = context.getClusterService().state().getMetadata().getIndicesLookup().entrySet().stream()
                        .filter(e -> e.getValue().getType().equals(Type.ALIAS)).filter(e -> WildcardMatcher.match(indexPattern, e.getKey()))
                        .map(e -> e.getKey()).toArray(String[]::new);

                if (aliasesForPermittedPattern.length > 0) {

                    ImmutableSet<String> resolvedAliases = ImmutableSet.ofArray(context.getResolver()
                            .concreteIndexNames(context.getClusterService().state(), IndicesOptions.lenientExpandOpen(), aliasesForPermittedPattern));

                    checkTable.uncheckRowIf((index) -> resolvedAliases.contains(index));

                    if (log.isTraceEnabled()) {
                        log.trace("remaining indices after removing matching aliases (" + resolvedAliases + "): " + checkTable);
                    }
                }

            } else {
                // No wildcard

                checkTable.uncheckRowIfPresent(indexPattern);

                if (log.isTraceEnabled()) {
                    log.trace("remaining indices after removing matches: " + checkTable);
                }

                if (checkTable.isBlank()) {
                    return;
                }

                ImmutableSet<String> resolvedAliases = ImmutableSet.ofArray(context.getResolver()
                        .concreteIndexNames(context.getClusterService().state(), IndicesOptions.lenientExpandOpen(), indexPattern));

                checkTable.uncheckRowIf((index) -> resolvedAliases.contains(index));

                if (log.isTraceEnabled()) {
                    log.trace("remaining indices after removing matching aliases (" + Arrays.asList(resolvedAliases) + "): " + checkTable);
                }

            }

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index_patterns", Collections.singletonList(indexPattern));

            if (permsAsString != null && permsAsString.size() > 0) {
                builder.field("actions", perms);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((indexPattern == null) ? 0 : indexPattern.hashCode());
            result = prime * result + ((perms == null) ? 0 : perms.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ExcludedIndexPermissions other = (ExcludedIndexPermissions) obj;
            if (indexPattern == null) {
                if (other.indexPattern != null) {
                    return false;
                }
            } else if (!indexPattern.equals(other.indexPattern)) {
                return false;
            }
            if (perms == null) {
                if (other.perms != null) {
                    return false;
                }
            } else if (!perms.equals(other.perms)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "ExcludedIndexPermissions [indexPattern=" + indexPattern + ", perms=" + perms + "]";
        }

    }

    public static class Tenant implements ToXContentObject {
        private final String tenantPattern;
        private final boolean tenantPatternNeedsAttributeReplacement;
        private final boolean readWrite;
        private final Set<String> permissions;

        private Tenant(String tenant, Set<String> permissions) {
            super();
            this.tenantPattern = tenant;
            this.tenantPatternNeedsAttributeReplacement = Attributes.needsAttributeReplacement(tenant);
            this.permissions = Collections.unmodifiableSet(permissions);
            this.readWrite = containsKibanaWritePermission(permissions);
        }

        public String getTenantPattern() {
            return tenantPattern;
        }

        public String getEvaluatedTenantPattern(User user) throws StringInterpolationException {
            if (tenantPatternNeedsAttributeReplacement) {
                return Attributes.replaceAttributes(tenantPattern, user);
            } else {
                return tenantPattern;
            }
        }

        public boolean isReadWrite() {
            return readWrite;
        }

        private boolean containsKibanaWritePermission(Set<String> permissionsToBeSearched) {
            if (permissionsToBeSearched.contains(KIBANA_ALL_SAVED_OBJECTS_WRITE)) {
                return true;
            }

            if (permissionsToBeSearched.contains("*")) {
                return true;
            }

            return WildcardMatcher.matchAny(permissionsToBeSearched, KIBANA_ALL_SAVED_OBJECTS_WRITE);
        }

        public Set<String> getPermissions() {
            return permissions;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((permissions == null) ? 0 : permissions.hashCode());
            result = prime * result + ((tenantPattern == null) ? 0 : tenantPattern.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Tenant other = (Tenant) obj;
            if (permissions == null) {
                if (other.permissions != null)
                    return false;
            } else if (!permissions.equals(other.permissions))
                return false;
            if (tenantPattern == null) {
                if (other.tenantPattern != null)
                    return false;
            } else if (!tenantPattern.equals(other.tenantPattern))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "Tenant [tenantPattern=" + tenantPattern + ", readWrite=" + readWrite + ", permissions=" + permissions + "]";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("tenant_patterns", Collections.singletonList(tenantPattern));

            if (permissions != null && permissions.size() > 0) {
                builder.field("allowed_actions", permissions);
            }

            builder.endObject();

            return builder;
        }
    }

    private static String replaceProperties(String orig, User user) throws StringInterpolationException {

        if (user == null || orig == null) {
            return orig;
        }

        String result = replaceObsoleteProperties(orig, user);

        result = Attributes.replaceAttributes(result, user);

        return result;
    }

    @Deprecated
    private static String replaceObsoleteProperties(String orig, User user) {

        if (user == null || orig == null) {
            return orig;
        }

        if (log.isTraceEnabled()) {
            log.trace("replaceObsoleteProperties()\nstring: " + orig + "\nattrs: " + user.getCustomAttributesMap().keySet());
        }

        for (Entry<String, String> entry : user.getCustomAttributesMap().entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            orig = orig.replace("${" + entry.getKey() + "}", entry.getValue());
            orig = orig.replace("${" + entry.getKey().replace('.', '_') + "}", entry.getValue());
        }
        return orig;
    }

    @Override
    public Set<String> mapSgRoles(User user, TransportAddress caller) {
        return invertedRoleMappings.evaluate(user, caller, rolesMappingResolution);
    }

    public static class TenantPermissionsImpl implements TenantPermissions {

        private final Set<String> permissions;

        public TenantPermissionsImpl(Set<String> permissions) {
            this.permissions = Collections.unmodifiableSet(permissions);
        }

        public boolean isReadPermitted() {
            return permissions.size() > 0;
        }

        public boolean isWritePermitted() {
            return permissions.contains(KIBANA_ALL_SAVED_OBJECTS_WRITE) || permissions.contains("*");
        }

        public Set<String> getPermissions() {
            return permissions;
        }
    }

    private final static Set<String> SET_OF_EVERYTHING = ImmutableSet.of("*");

    private static final TenantPermissions RW_TENANT_PERMISSIONS = new TenantPermissions() {

        @Override
        public boolean isWritePermitted() {
            return true;
        }

        @Override
        public boolean isReadPermitted() {
            return true;
        }

        @Override
        public Set<String> getPermissions() {
            return KIBANA_ALL_SAVED_OBJECTS_WRITE_SET;
        }
    };

    private static final TenantPermissions FULL_TENANT_PERMISSIONS = new TenantPermissions() {

        @Override
        public boolean isWritePermitted() {
            return true;
        }

        @Override
        public boolean isReadPermitted() {
            return true;
        }

        @Override
        public Set<String> getPermissions() {
            return SET_OF_EVERYTHING;
        }
    };

    private static final TenantPermissions EMPTY_TENANT_PERMISSIONS = new TenantPermissions() {
        @Override
        public boolean isWritePermitted() {
            return false;
        }

        @Override
        public boolean isReadPermitted() {
            return false;
        }

        @Override
        public Set<String> getPermissions() {
            return Collections.emptySet();
        }
    };
}
