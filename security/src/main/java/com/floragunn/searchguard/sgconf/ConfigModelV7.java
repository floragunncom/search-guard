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

import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.auth.blocking.IpRangeVerdictBasedBlockRegistry;
import com.floragunn.searchguard.auth.blocking.VerdictBasedBlockRegistry;
import com.floragunn.searchguard.auth.blocking.WildcardVerdictBasedBlockRegistry;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.*;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7.Index;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.google.common.collect.MultimapBuilder.SetMultimapBuilder;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexAbstraction.Type;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigModelV7 extends ConfigModel {

	private static boolean dfmEmptyOverridesAll;

    protected final Logger log = LogManager.getLogger(this.getClass());
    private ConfigConstants.RolesMappingResolution rolesMappingResolution;
    private ActionGroupResolver agr;
    private SgRoles sgRoles;
    private TenantHolder tenantHolder;
    private RoleMappingHolder roleMappingHolder;
    private SgDynamicConfiguration<RoleV7> roles;
    private SgDynamicConfiguration<TenantV7> tenants;
    private ClientBlockRegistry<InetAddress> blockedIpAddresses;
    private ClientBlockRegistry<String> blockedUsers;
    private ClientBlockRegistry<IPAddressString> blockeNetmasks;    

    public ConfigModelV7(SgDynamicConfiguration<RoleV7> roles, SgDynamicConfiguration<RoleMappingsV7> rolemappings,
                         SgDynamicConfiguration<ActionGroupsV7> actiongroups, SgDynamicConfiguration<TenantV7> tenants,
                         SgDynamicConfiguration<BlocksV7> blocks, DynamicConfigModel dcm, Settings esSettings) {

        this.roles = roles;
        this.tenants = tenants;

        try {
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.valueOf(esSettings
                    .get(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, ConfigConstants.RolesMappingResolution.MAPPING_ONLY.toString())
                    .toUpperCase());
        } catch (Exception e) {
            log.error("Cannot apply roles mapping resolution", e);
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.MAPPING_ONLY;
        }

        agr = reloadActionGroups(actiongroups);
        sgRoles = reload(roles);
        tenantHolder = new TenantHolder(roles, tenants);
        roleMappingHolder = new RoleMappingHolder(rolemappings, dcm.getHostsResolverMode());
        blockedIpAddresses = reloadBlockedIpAddresses(blocks);
        blockedUsers = reloadBlockedUsers(blocks);
        blockeNetmasks = reloadBlockedNetmasks(blocks);
        ConfigModelV7.dfmEmptyOverridesAll = esSettings.getAsBoolean(ConfigConstants.SEARCHGUARD_DFM_EMPTY_OVERRIDES_ALL, false);

    }

    private ClientBlockRegistry<IPAddressString> reloadBlockedNetmasks(SgDynamicConfiguration<BlocksV7> blocks) {
        Function<String, Optional<IPAddressString>> parsedIp = s -> {
            IPAddressString ipAddressString = new IPAddressString(s);
            try {
                ipAddressString.validate();
                return Optional.of(ipAddressString);
            } catch (AddressStringException e) {
                log.error("Reloading blocked IP addresses failed ", e);
                return Optional.empty();
            }
        };

        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.net_mask);
        Set<IPAddressString> allows = b.v1().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());
        Set<IPAddressString> disallows = b.v2().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());

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
        Set<InetAddress> disallows = b.v2().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());

        return new VerdictBasedBlockRegistry<>(InetAddress.class, allows, disallows);
    }

    private Tuple<Set<String>, Set<String>> readBlocks(SgDynamicConfiguration<BlocksV7> blocks, BlocksV7.Type type) {
        Set<String> allows = new HashSet<>();
        Set<String> disallows = new HashSet<>();

        List<BlocksV7> blocksV7s = blocks.getCEntries().values()
                .stream()
                .filter(b -> b.getType() == type)
                .collect(Collectors.toList());

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

    public ActionGroupResolver getActionGroupResolver() {
        return agr;
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
    public List<ClientBlockRegistry<IPAddressString>> getBlockedNetmasks() {
        return Collections.singletonList(blockeNetmasks);
    }

    private ActionGroupResolver reloadActionGroups(SgDynamicConfiguration<ActionGroupsV7> actionGroups) {
        return new ActionGroupResolver() {

            private Set<String> getGroupMembers(final String groupname) {

                if (actionGroups == null) {
                    return Collections.emptySet();
                }

                return Collections.unmodifiableSet(resolve(actionGroups, groupname));
            }

            private Set<String> resolve(final SgDynamicConfiguration<?> actionGroups, final String entry) {

                // SG5 format, plain array
                //List<String> en = actionGroups.getAsList(DotPath.of(entry));
                //if (en.isEmpty()) {
                // try SG6 format including readonly and permissions key
                //  en = actionGroups.getAsList(DotPath.of(entry + "." + ConfigConstants.CONFIGKEY_ACTION_GROUPS_PERMISSIONS));
                //}

                if (!actionGroups.getCEntries().containsKey(entry)) {
                    return Collections.emptySet();
                }

                final Set<String> ret = new HashSet<String>();

                final Object actionGroupAsObject = actionGroups.getCEntries().get(entry);

                if (actionGroupAsObject != null && actionGroupAsObject instanceof List) {

                    for (final String perm : ((List<String>) actionGroupAsObject)) {
                        if (actionGroups.getCEntries().keySet().contains(perm)) {
                            ret.addAll(resolve(actionGroups, perm));
                        } else {
                            ret.add(perm);
                        }
                    }

                } else if (actionGroupAsObject != null && actionGroupAsObject instanceof ActionGroupsV7) {
                    for (final String perm : ((ActionGroupsV7) actionGroupAsObject).getAllowed_actions()) {
                        if (actionGroups.getCEntries().keySet().contains(perm)) {
                            ret.addAll(resolve(actionGroups, perm));
                        } else {
                            ret.add(perm);
                        }
                    }
                } else {
                    throw new RuntimeException("Unable to handle " + actionGroupAsObject);
                }

                return Collections.unmodifiableSet(ret);
            }

            @Override
            public Set<String> resolvedActions(final List<String> actions) {
                final Set<String> resolvedActions = new HashSet<String>();
                for (String string : actions) {
                    final Set<String> groups = getGroupMembers(string);
                    if (groups.isEmpty()) {
                        resolvedActions.add(string);
                    } else {
                        resolvedActions.addAll(groups);
                    }
                }

                return Collections.unmodifiableSet(resolvedActions);
            }
        };
    }

    private SgRoles reload(SgDynamicConfiguration<RoleV7> settings) {

        final Set<Future<SgRole>> futures = new HashSet<>(5000);
        final ExecutorService execs = Executors.newFixedThreadPool(10);

        for (Entry<String, RoleV7> sgRole : settings.getCEntries().entrySet()) {

            Future<SgRole> future = execs.submit(() -> {
                if (sgRole.getValue() == null) {
                    return null;
                }

                return SgRole.create(sgRole.getKey(), sgRole.getValue(), agr);
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

        public static SgRoles create(SgDynamicConfiguration<RoleV7> settings, ActionGroupResolver actionGroupResolver) {

            SgRoles result = new SgRoles(settings.getCEntries().size());

            for (Entry<String, RoleV7> entry : settings.getCEntries().entrySet()) {

                if (entry.getValue() == null) {
                    continue;
                }

                result.addSgRole(SgRole.create(entry.getKey(), entry.getValue(), actionGroupResolver));
            }

            return result;
        }


        protected final Logger log = LogManager.getLogger(this.getClass());

        final Set<SgRole> roles;

        private SgRoles(int roleCount) {
            roles = new HashSet<>(roleCount);
        }

        private SgRoles addSgRole(SgRole sgRole) {
            if (sgRole != null) {
                this.roles.add(sgRole);
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

        public Set<SgRole> getRoles() {
            return Collections.unmodifiableSet(roles);
        }

        public Set<String> getRoleNames() {
            return getRoles().stream().map(r -> r.getName()).collect(Collectors.toSet());
        }

        public SgRoles filter(Set<String> keep) {
            final SgRoles retVal = new SgRoles(roles.size());
            for (SgRole sgr : roles) {
                if (keep.contains(sgr.getName())) {
                    retVal.addSgRole(sgr);
                }
            }
            return retVal;
        }

        public Map<String, Set<String>> getMaskedFields(User user, IndexNameExpressionResolver resolver, ClusterService cs) {

            boolean maskedFieldsPresent = false;
            
            outer:
            for (SgRole sgr : roles) {
                for (IndexPattern ip : sgr.getIpatterns()) {
                    if(ip.hasMaskedFields()) {
                        maskedFieldsPresent = true;
                        break outer;
                    }
                }
            }
            
            if(!maskedFieldsPresent) {
                if(log.isDebugEnabled()) {
                    log.debug("No masked fields found for {} in {} sg roles", user, roles.size());
                }
                return Collections.emptyMap();
            }

        	final Map<String, Set<String>> maskedFieldsMap = new HashMap<String, Set<String>>();
            final Set<String> noMaskedFieldConcreteIndices = new HashSet<>();

            for (SgRole sgr : roles) {
                for (IndexPattern ip : sgr.getIpatterns()) {
                    final Set<String> maskedFields = ip.getMaskedFields();

                    final String[] concreteIndices = ip.getResolvedIndexPatterns(user, resolver, cs, false);

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
                    } else if (dfmEmptyOverridesAll){
                        noMaskedFieldConcreteIndices.addAll(Arrays.asList(concreteIndices));
                    } 
                }
            }
            
            if(dfmEmptyOverridesAll) {
                
                
                if(log.isDebugEnabled()) {
                    log.debug("Index patterns with no masked fields attached: {} - They will be removed from {}", noMaskedFieldConcreteIndices, maskedFieldsMap.keySet());
                }
                
                WildcardMatcher.wildcardRemoveFromSet(maskedFieldsMap.keySet(), noMaskedFieldConcreteIndices);
    
                //maskedFieldsMap.keySet().removeAll(noMaskedFieldConcreteIndices);
            }
                         
            return maskedFieldsMap;
        }

        public Tuple<Map<String, Set<String>>, Map<String, Set<String>>> getDlsFls(User user, IndexNameExpressionResolver resolver,
                                                                                   ClusterService cs) {

            boolean flsOrDlsPresent = false;
            
            outer:
            for (SgRole sgr : roles) {
                for (IndexPattern ip : sgr.getIpatterns()) {
                    if(ip.hasDlsQuery() || ip.hasFlsFields()) {
                        flsOrDlsPresent = true;
                        break outer;
                    }
                }
            }
            
            if(!flsOrDlsPresent) {
                if(log.isDebugEnabled()) {
                    log.debug("No fls or dls found for {} in {} sg roles", user, roles.size());
                }
                return new Tuple<Map<String, Set<String>>, Map<String, Set<String>>>(Collections.emptyMap(), Collections.emptyMap());
            }

        	
        	final Map<String, Set<String>> dlsQueries = new HashMap<String, Set<String>>();
            final Map<String, Set<String>> flsFields = new HashMap<String, Set<String>>();

            final Set<String> noDlsConcreteIndices = new HashSet<>();
            final Set<String> noFlsConcreteIndices = new HashSet<>();
            
            for (SgRole sgr : roles) {
                for (IndexPattern ip : sgr.getIpatterns()) {
                    final Set<String> fls = ip.getFls();
                    final String dls = ip.getDlsQuery(user);

                    final String[] concreteIndices = ip.getResolvedIndexPatterns(user, resolver, cs, false);
                    

                    if (dls != null && dls.length() > 0) {

                        for (int i = 0; i < concreteIndices.length; i++) {
                            final String ci = concreteIndices[i];
                            if (dlsQueries.containsKey(ci)) {
                                dlsQueries.get(ci).add(dls);
                            } else {
                                dlsQueries.put(ci, new HashSet<String>());
                                dlsQueries.get(ci).add(dls);
                            }
                        }
                    } else if (dfmEmptyOverridesAll){
                        noDlsConcreteIndices.addAll(Arrays.asList(concreteIndices));
                    }

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
                }
            }

            
            if(dfmEmptyOverridesAll) {
                if(log.isDebugEnabled()) {
                    log.debug("Index patterns with no dls queries attached: {} - They will be removed from {}", noDlsConcreteIndices, dlsQueries.keySet());
                    log.debug("Index patterns with no fls fields attached: {} - They will be removed from {}", noFlsConcreteIndices, flsFields.keySet());
    
                }
    
                WildcardMatcher.wildcardRemoveFromSet(dlsQueries.keySet(), noDlsConcreteIndices);
                WildcardMatcher.wildcardRemoveFromSet(flsFields.keySet(), noFlsConcreteIndices);
                
                //dlsQueries.keySet().removeAll(noDlsConcreteIndices);
                //flsFields.keySet().removeAll(noFlsConcreteIndices);
            }
            
            
            
            return new Tuple<Map<String, Set<String>>, Map<String, Set<String>>>(dlsQueries, flsFields);

        }

        //kibana special only, terms eval
        public Set<String> getAllPermittedIndicesForKibana(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver,
                                                           ClusterService cs) {
            Set<String> retVal = new HashSet<>();
            for (SgRole sgr : roles) {
                retVal.addAll(sgr.getAllResolvedPermittedIndices(Resolved._LOCAL_ALL, user, actions, resolver, cs));
                retVal.addAll(resolved.getRemoteIndices());
            }
            return Collections.unmodifiableSet(retVal);
        }

        //dnfof only
        public Set<String> reduce(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver, ClusterService cs) {
            Set<String> retVal = new HashSet<>();
            for (SgRole sgr : roles) {
                retVal.addAll(sgr.getAllResolvedPermittedIndices(resolved, user, actions, resolver, cs));
            }
            if (log.isDebugEnabled()) {
                log.debug("Reduced requested resolved indices {} to permitted indices {}.", resolved, retVal.toString());
            }
            return Collections.unmodifiableSet(retVal);
        }

        //return true on success
        public boolean get(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver, ClusterService cs) {
            for (SgRole sgr : roles) {
                if (ConfigModelV7.impliesTypePerm(sgr.getIpatterns(), resolved, user, actions, resolver, cs)) {
                    return true;
                }
            }
            return false;
        }

        public boolean impliesClusterPermissionPermission(String action) {
            return roles.stream().filter(r -> r.impliesClusterPermission(action)).count() > 0;
        }

        //rolespan
        public boolean impliesTypePermGlobal(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver,
                                             ClusterService cs) {
            Set<IndexPattern> ipatterns = new HashSet<ConfigModelV7.IndexPattern>();
            roles.stream().forEach(p -> ipatterns.addAll(p.getIpatterns()));
            return ConfigModelV7.impliesTypePerm(ipatterns, resolved, user, actions, resolver, cs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startObject("_sg_meta");
            builder.field("type", "roles");
            builder.field("config_version", 2);
            builder.endObject();

            for (SgRole role : roles) {
                builder.field(role.getName(), role);
            }

            builder.endObject();
            return builder;
        }
    }

    public static class SgRole implements ToXContentObject {


        static SgRole create(String roleName, RoleV7 roleConfig, ActionGroupResolver actionGroupResolver) {
            SgRole result = new SgRole(roleName);

            final Set<String> permittedClusterActions = actionGroupResolver.resolvedActions(roleConfig.getCluster_permissions());
            result.addClusterPerms(permittedClusterActions);

            // XXX
            /*for(RoleV7.Tenant tenant: sgRole.getValue().getTenant_permissions()) {

                //if(tenant.equals(user.getName())) {
                //    continue;
                //}

                if(isTenantsRw(tenant)) {
                    _sgRole.addTenant(new Tenant(tenant.getKey(), true));
                } else {
                    _sgRole.addTenant(new Tenant(tenant.getKey(), false));
                }
            }*/

            for (final Index permittedAliasesIndex : roleConfig.getIndex_permissions()) {

                final String dls = permittedAliasesIndex.getDls();
                final List<String> fls = permittedAliasesIndex.getFls();
                final List<String> maskedFields = permittedAliasesIndex.getMasked_fields();

                for (String pat : permittedAliasesIndex.getIndex_patterns()) {
                    IndexPattern _indexPattern = new IndexPattern(pat);
                    _indexPattern.setDlsQuery(dls);
                    _indexPattern.addFlsFields(fls);
                    _indexPattern.addMaskedFields(maskedFields);
                    _indexPattern.addPerm(actionGroupResolver.resolvedActions(permittedAliasesIndex.getAllowed_actions()));

                    /*for(Entry<String, List<String>> type: permittedAliasesIndex.getValue().getTypes(-).entrySet()) {
                        TypePerm typePerm = new TypePerm(type.getKey());
                        final List<String> perms = type.getValue();
                        typePerm.addPerms(agr.resolvedActions(perms));
                        _indexPattern.addTypePerms(typePerm);
                    }*/

                    result.addIndexPattern(_indexPattern);

                }

            }

            return result;
        }

        private final String name;
        //private final Set<Tenant> tenants = new HashSet<>();
        private final Set<IndexPattern> ipatterns = new HashSet<>();
        private final Set<String> clusterPerms = new HashSet<>();

        private SgRole(String name) {
            super();
            this.name = Objects.requireNonNull(name);
        }

        private boolean impliesClusterPermission(String action) {
            return WildcardMatcher.matchAny(clusterPerms, action);
        }
        //get indices which are permitted for the given types and actions

        //dnfof + kibana special only

        private Set<String> getAllResolvedPermittedIndices(Resolved resolved, User user, String[] actions, IndexNameExpressionResolver resolver,
                                                           ClusterService cs) {

            final Set<String> retVal = new HashSet<>();
            for (IndexPattern p : ipatterns) {
                //what if we cannot resolve one (for create purposes)
                final boolean patternMatch = WildcardMatcher.matchAll(p.getPerms().toArray(new String[0]), actions);

                //                final Set<TypePerm> tperms = p.getTypePerms();
                //                for (TypePerm tp : tperms) {
                //                    if (WildcardMatcher.matchAny(tp.typePattern, resolved.getTypes(-).toArray(new String[0]))) {
                //                        patternMatch = WildcardMatcher.matchAll(tp.perms.toArray(new String[0]), actions);
                //                    }
                //                }
                if (patternMatch) {
                    //resolved but can contain patterns for nonexistent indices
                    final String[] permitted = p.getResolvedIndexPatterns(user, resolver, cs, true); //maybe they do not exist
                    final Set<String> res = new HashSet<>();
                    if (!resolved.isLocalAll() && !resolved.getAllIndices().contains("*") && !resolved.getAllIndices().contains("_all")) {
                        final Set<String> wanted = new HashSet<>(resolved.getAllIndices());
                        //resolved but can contain patterns for nonexistent indices
                        WildcardMatcher.wildcardRetainInSet(wanted, permitted);
                        res.addAll(wanted);
                    } else {
                        //we want all indices so just return what's permitted

                        //#557
                        //final String[] allIndices = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), "*");
                        final String[] allIndices = cs.state().getMetadata().getConcreteAllOpenIndices();
                        final Set<String> wanted = new HashSet<>(Arrays.asList(allIndices));
                        WildcardMatcher.wildcardRetainInSet(wanted, permitted);
                        res.addAll(wanted);
                    }
                    retVal.addAll(res);
                }
            }

            //all that we want and all thats permitted of them
            return Collections.unmodifiableSet(retVal);
        }
        /*private SgRole addTenant(Tenant tenant) {
            if (tenant != null) {
                this.tenants.add(tenant);
            }
            return this;
        }*/

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

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((clusterPerms == null) ? 0 : clusterPerms.hashCode());
            result = prime * result + ((ipatterns == null) ? 0 : ipatterns.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            //result = prime * result + ((tenants == null) ? 0 : tenants.hashCode());
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
            SgRole other = (SgRole) obj;
            if (clusterPerms == null) {
                if (other.clusterPerms != null)
                    return false;
            } else if (!clusterPerms.equals(other.clusterPerms))
                return false;
            if (ipatterns == null) {
                if (other.ipatterns != null)
                    return false;
            } else if (!ipatterns.equals(other.ipatterns))
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            //            if (tenants == null) {
            //                if (other.tenants != null)
            //                    return false;
            //            } else if (!tenants.equals(other.tenants))
            //                return false;
            return true;
        }

        @Override
        public String toString() {
            return System.lineSeparator() + "  " + name + System.lineSeparator() + "    ipatterns=" + ipatterns + System.lineSeparator()
                    + "    clusterPerms=" + clusterPerms;
        }

        //public Set<Tenant> getTenants(User user) {
        //    //TODO filter out user tenants
        //    return Collections.unmodifiableSet(tenants);
        //}

        public Set<IndexPattern> getIpatterns() {
            return Collections.unmodifiableSet(ipatterns);
        }

        public Set<String> getClusterPerms() {
            return Collections.unmodifiableSet(clusterPerms);
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

            /* TODO ????
             *
            if (tenants != null && tenants.size() > 0) {
                builder.array("tenant_permissions", tenants);
            }
            */
            builder.endObject();

            return builder;
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

        public String getUnresolvedIndexPattern(User user) {
            return replaceProperties(indexPattern, user);
        }
        
        private String[] getResolvedIndexPatterns(User user, IndexNameExpressionResolver resolver, ClusterService cs, boolean appendUnresolved) {
            String unresolved = getUnresolvedIndexPattern(user);
            String[] resolved = null;
            if (WildcardMatcher.containsWildcard(unresolved)) {
                final String[] aliasesForPermittedPattern = cs.state().getMetadata().getIndicesLookup().entrySet().stream()
                        .filter(e -> e.getValue().getType().equals(Type.ALIAS)).filter(e -> WildcardMatcher.match(unresolved, e.getKey())).map(e -> e.getKey())
                        .toArray(String[]::new);

                if (aliasesForPermittedPattern != null && aliasesForPermittedPattern.length > 0) {
                    resolved = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), aliasesForPermittedPattern);
                }
            }

            if (resolved == null && !unresolved.isEmpty()) {
                resolved = resolver.concreteIndexNames(cs.state(), IndicesOptions.lenientExpandOpen(), unresolved);
            }
            if (resolved == null || resolved.length == 0) {
                return new String[]{unresolved};
            } else {
            	if(appendUnresolved) {
                    //append unresolved value for pattern matching
                    String[] retval = Arrays.copyOf(resolved, resolved.length + 1);
                    retval[retval.length - 1] = unresolved;
                    return retval;            		
            	}
            	else {
            		return resolved;
            	}
            }
        }

        public String getDlsQuery(User user) {
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

    /*public static class TypePerm {
        private final String typePattern;
        private final Set<String> perms = new HashSet<>();
    
        private TypePerm(String typePattern) {
            super();
            this.typePattern = Objects.requireNonNull(typePattern);
            /*if(IGNORED_TYPES.contains(typePattern)) {
                throw new RuntimeException("typepattern '"+typePattern+"' not allowed");
            }
        }
    
        private TypePerm addPerms(Collection<String> perms) {
            if (perms != null) {
                this.perms.addAll(perms);
            }
            return this;
        }
    
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((perms == null) ? 0 : perms.hashCode());
            result = prime * result + ((typePattern == null) ? 0 : typePattern.hashCode());
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
            TypePerm other = (TypePerm) obj;
            if (perms == null) {
                if (other.perms != null)
                    return false;
            } else if (!perms.equals(other.perms))
                return false;
            if (typePattern == null) {
                if (other.typePattern != null)
                    return false;
            } else if (!typePattern.equals(other.typePattern))
                return false;
            return true;
        }
    
        @Override
        public String toString() {
            return System.lineSeparator() + "             typePattern=" + typePattern + System.lineSeparator() + "             perms=" + perms;
        }
    
        public String getTypePattern() {
            return typePattern;
        }
    
        public Set<String> getPerms() {
            return Collections.unmodifiableSet(perms);
        }
    
    }*/

    public static class Tenant {
        private final String tenant;
        private final boolean readWrite;

        private Tenant(String tenant, boolean readWrite) {
            super();
            this.tenant = tenant;
            this.readWrite = readWrite;
        }

        public String getTenant() {
            return tenant;
        }

        public boolean isReadWrite() {
            return readWrite;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (readWrite ? 1231 : 1237);
            result = prime * result + ((tenant == null) ? 0 : tenant.hashCode());
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
            if (readWrite != other.readWrite)
                return false;
            if (tenant == null) {
                if (other.tenant != null)
                    return false;
            } else if (!tenant.equals(other.tenant))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return System.lineSeparator() + "                tenant=" + tenant + System.lineSeparator() + "                readWrite=" + readWrite;
        }
    }

    private static String replaceProperties(String orig, User user) {

        if (user == null || orig == null) {
            return orig;
        }

        orig = orig.replace("${user.name}", user.getName()).replace("${user_name}", user.getName());
        orig = replaceRoles(orig, user);
        for (Entry<String, String> entry : user.getCustomAttributesMap().entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            orig = orig.replace("${" + entry.getKey() + "}", entry.getValue());
            orig = orig.replace("${" + entry.getKey().replace('.', '_') + "}", entry.getValue());
        }
        return orig;
    }

    private static String replaceRoles(final String orig, final User user) {
        String retVal = orig;
        if (orig.contains("${user.roles}") || orig.contains("${user_roles}")) {
            final String commaSeparatedRoles = toQuotedCommaSeparatedString(user.getRoles());
            retVal = orig.replace("${user.roles}", commaSeparatedRoles).replace("${user_roles}", commaSeparatedRoles);
        }
        return retVal;
    }

    private static String toQuotedCommaSeparatedString(final Set<String> roles) {
        return Joiner.on(',').join(Iterables.transform(roles, s -> {
            return new StringBuilder(s.length() + 2).append('"').append(s).append('"').toString();
        }));
    }

    private static boolean impliesTypePerm(Set<IndexPattern> ipatterns, Resolved resolved, User user, String[] actions,
                                           IndexNameExpressionResolver resolver, ClusterService cs) {
        Set<String> matchingIndex = new HashSet<>(resolved.getAllIndices());

        for (String in : resolved.getAllIndices()) {
            //find index patterns who are matching
            Set<String> matchingActions = new HashSet<>(Arrays.asList(actions));
            //Set<String> matchingTypes = new HashSet<>(resolved.getTypes(-));
            for (IndexPattern p : ipatterns) {
                if (WildcardMatcher.matchAny(p.getResolvedIndexPatterns(user, resolver, cs, true), in)) {
                    //per resolved index per pattern
                    //for (String t : resolved.getTypes(-)) {
                    //for (TypePerm tp : p.typePerms) {
                    //if (WildcardMatcher.match(tp.typePattern, t)) {
                    //matchingTypes.remove(t);
                    for (String a : Arrays.asList(actions)) {
                        if (WildcardMatcher.matchAny(p.perms, a)) {
                            matchingActions.remove(a);
                        }
                    }
                    //}
                    //}
                    //}
                }
            }

            if (matchingActions.isEmpty() /*&& matchingTypes.isEmpty()*/) {
                matchingIndex.remove(in);
            }
        }

        return matchingIndex.isEmpty();
    }

    //#######

    private class TenantHolder {

        private static final String KIBANA_ALL_SAVED_OBJECTS_WRITE = "kibana:saved_objects/*/write";
        private final Set<String> KIBANA_ALL_SAVED_OBJECTS_WRITE_SET = ImmutableSet.of(KIBANA_ALL_SAVED_OBJECTS_WRITE);

        // TODO make this data structure more readable
        private SetMultimap<String, Tuple<String, Set<String>>> tenantsMM = null;

        public TenantHolder(SgDynamicConfiguration<RoleV7> roles, SgDynamicConfiguration<TenantV7> definedTenants) {
            final Set<Future<Tuple<String, Set<Tuple<String, Set<String>>>>>> futures = new HashSet<>(roles.getCEntries().size());

            final ExecutorService execs = Executors.newFixedThreadPool(10);

            for (Entry<String, RoleV7> sgRole : roles.getCEntries().entrySet()) {

                if (sgRole.getValue() == null) {
                    continue;
                }

                Future<Tuple<String, Set<Tuple<String, Set<String>>>>> future = execs.submit(new Callable<Tuple<String, Set<Tuple<String, Set<String>>>>>() {
                    @Override
                    public Tuple<String, Set<Tuple<String, Set<String>>>> call() throws Exception {
                        final Set<Tuple<String, Set<String>>> tuples = new HashSet<>();
                        final List<RoleV7.Tenant> tenants = sgRole.getValue().getTenant_permissions();

                        if (tenants != null) {

                            for (RoleV7.Tenant tenant : tenants) {
                                for (String matchingTenant : WildcardMatcher.getMatchAny(tenant.getTenant_patterns(), definedTenants.getCEntries().keySet())) {
                                    tuples.add(new Tuple<String, Set<String>>(matchingTenant, agr.resolvedActions(tenant.getAllowed_actions())));
                                }
                            }
                        }

                        return new Tuple<String, Set<Tuple<String, Set<String>>>>(sgRole.getKey(), tuples);
                    }
                });

                futures.add(future);

            }

            execs.shutdown();
            try {
                execs.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted (1) while loading roles");
                return;
            }

            try {
                final SetMultimap<String, Tuple<String, Set<String>>> tenantsMM_ = SetMultimapBuilder.hashKeys(futures.size()).hashSetValues(16).build();

                for (Future<Tuple<String, Set<Tuple<String, Set<String>>>>> future : futures) {
                    Tuple<String, Set<Tuple<String, Set<String>>>> result = future.get();
                    tenantsMM_.putAll(result.v1(), result.v2());
                }

                tenantsMM = tenantsMM_;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted (2) while loading tenants");
                return;
            } catch (ExecutionException e) {
                log.error("Error while updating tenants: {}", e.getCause(), e.getCause());
                throw ExceptionsHelper.convertToElastic(e);
            }

        }

        public Map<String, Boolean> mapTenants(final User user, Set<String> roles) {

            if (user == null || tenantsMM == null) {
                return Collections.emptyMap();
            }

            final Map<String, Boolean> result = new HashMap<>(roles.size());
            result.put(user.getName(), true);

            tenantsMM.entries().stream().filter(e -> roles.contains(e.getKey())).filter(e -> !user.getName().equals(e.getValue().v1())).forEach(e -> {
                String tenant = e.getValue().v1();
                Set<String> permissions = e.getValue().v2();

                boolean rw = containsKibanaWritePermission(permissions);

                if (rw || !result.containsKey(tenant)) { //RW outperforms RO
                    result.put(tenant, rw);
                }
            });

            if (!result.containsKey("SGS_GLOBAL_TENANT") && (roles.contains("sg_kibana_user") || roles.contains("SGS_KIBANA_USER")
                    || roles.contains("sg_all_access") || roles.contains("SGS_ALL_ACCESS"))) {
                result.put("SGS_GLOBAL_TENANT", true);
            }

            return Collections.unmodifiableMap(result);
        }

        public Map<String, Set<String>> mapTenantPermissions(final User user, Set<String> roles) {

            if (user == null || tenantsMM == null) {
                return Collections.emptyMap();
            }

            final Map<String, Set<String>> result = new HashMap<>(roles.size());
            result.put(user.getName(), ImmutableSet.of("*"));

            tenantsMM.entries().stream().filter(e -> roles.contains(e.getKey())).filter(e -> !user.getName().equals(e.getValue().v1())).forEach(e -> {

                if (result.get(e.getValue().v1()) != null) {
                    result.get(e.getValue().v1()).addAll(e.getValue().v2());
                } else {
                    result.put(e.getValue().v1(), new HashSet<String>((e.getValue().v2())));
                }
            });

            if (!result.containsKey("SGS_GLOBAL_TENANT") && (
                    roles.contains("sg_kibana_user")
                            || roles.contains("SGS_KIBANA_USER")
                            || roles.contains("sg_all_access")
                            || roles.contains("SGS_ALL_ACCESS")
            )) {
                result.put("SGS_GLOBAL_TENANT", KIBANA_ALL_SAVED_OBJECTS_WRITE_SET);
            }

            return Collections.unmodifiableMap(result);
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
    }

    private class RoleMappingHolder {

        private ListMultimap<String, String> users;
        private ListMultimap<Set<String>, String> abars;
        private ListMultimap<String, String> bars;
        private ListMultimap<String, String> hosts;
        private final String hostResolverMode;

        private RoleMappingHolder(final SgDynamicConfiguration<RoleMappingsV7> rolemappings, final String hostResolverMode) {

            this.hostResolverMode = hostResolverMode;

            if (roles != null) {

                final ListMultimap<String, String> users_ = ArrayListMultimap.create();
                final ListMultimap<Set<String>, String> abars_ = ArrayListMultimap.create();
                final ListMultimap<String, String> bars_ = ArrayListMultimap.create();
                final ListMultimap<String, String> hosts_ = ArrayListMultimap.create();

                for (final Entry<String, RoleMappingsV7> roleMap : rolemappings.getCEntries().entrySet()) {

                    for (String u : roleMap.getValue().getUsers()) {
                        users_.put(u, roleMap.getKey());
                    }

                    final Set<String> abar = new HashSet<String>(roleMap.getValue().getAnd_backend_roles());

                    if (!abar.isEmpty()) {
                        abars_.put(abar, roleMap.getKey());
                    }

                    for (String bar : roleMap.getValue().getBackend_roles()) {
                        bars_.put(bar, roleMap.getKey());
                    }

                    for (String host : roleMap.getValue().getHosts()) {
                        hosts_.put(host, roleMap.getKey());
                    }
                }

                users = users_;
                abars = abars_;
                bars = bars_;
                hosts = hosts_;
            }
        }

        private Set<String> map(final User user, final TransportAddress caller) {

            if (user == null || users == null || abars == null || bars == null || hosts == null) {
                return Collections.emptySet();
            }

            final Set<String> sgRoles = new TreeSet<String>(user.getSearchGuardRoles());

            if (rolesMappingResolution == ConfigConstants.RolesMappingResolution.BOTH
                    || rolesMappingResolution == ConfigConstants.RolesMappingResolution.BACKENDROLES_ONLY) {
                if (log.isDebugEnabled()) {
                    log.debug("Pass backendroles from {}", user);
                }
                sgRoles.addAll(user.getRoles());
            }

            if (((rolesMappingResolution == ConfigConstants.RolesMappingResolution.BOTH
                    || rolesMappingResolution == ConfigConstants.RolesMappingResolution.MAPPING_ONLY))) {

                for (String p : WildcardMatcher.getAllMatchingPatterns(users.keySet(), user.getName())) {
                    sgRoles.addAll(users.get(p));
                }

                for (String p : WildcardMatcher.getAllMatchingPatterns(bars.keySet(), user.getRoles())) {
                    sgRoles.addAll(bars.get(p));
                }

                for (Set<String> p : abars.keySet()) {
                    if (WildcardMatcher.allPatternsMatched(p, user.getRoles())) {
                        sgRoles.addAll(abars.get(p));
                    }
                }

                if (caller != null) {
                    //IPV4 or IPv6 (compressed and without scope identifiers)
                    final String ipAddress = caller.getAddress();

                    for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), ipAddress)) {
                        sgRoles.addAll(hosts.get(p));
                    }

                    if (caller.address() != null
                            && (hostResolverMode.equalsIgnoreCase("ip-hostname") || hostResolverMode.equalsIgnoreCase("ip-hostname-lookup"))) {
                        final String hostName = caller.address().getHostString();

                        for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), hostName)) {
                            sgRoles.addAll(hosts.get(p));
                        }
                    }

                    if (caller.address() != null && hostResolverMode.equalsIgnoreCase("ip-hostname-lookup")) {

                        final String resolvedHostName = caller.address().getHostName();

                        for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), resolvedHostName)) {
                            sgRoles.addAll(hosts.get(p));
                        }
                    }
                }
            }

            return Collections.unmodifiableSet(sgRoles);

        }
    }

    @Override
    public Map<String, Set<String>> mapTenantPermissions(User user, Set<String> roles) {
        return tenantHolder.mapTenantPermissions(user, roles);
    }

    @Override
    public Map<String, Boolean> mapTenants(User user, Set<String> roles) {
        return tenantHolder.mapTenants(user, roles);
    }

    @Override
    public Set<String> mapSgRoles(User user, TransportAddress caller) {
        return roleMappingHolder.map(user, caller);
    }
}
