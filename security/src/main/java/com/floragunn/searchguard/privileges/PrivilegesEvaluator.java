/*
 * Copyright 2015-2021 floragunn GmbH
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

package com.floragunn.searchguard.privileges;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.privileges.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.privileges.PrivilegesEvaluationResult.Status;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfigRegistry;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.ReflectionHelper;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableSet;
import com.google.common.base.Strings;

public class PrivilegesEvaluator implements DCFListener {

    public static final Setting<Boolean> INDEX_REDUCTION_FAST_PATH = Setting.boolSetting("searchguard.privilege_evaluation.index_reduction_fast_path",
            true, Property.NodeScope, Property.Filtered);
    
    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final Logger actionTrace = LogManager.getLogger("sg_action_trace");
    private final ClusterService clusterService;

    private final IndexNameExpressionResolver resolver;

    private final AuditLog auditLog;
    private ThreadContext threadContext;

    private PrivilegesInterceptor privilegesInterceptor;

    private final boolean checkSnapshotRestoreWritePrivileges;

    private final ClusterInfoHolder clusterInfoHolder;
    private ConfigModel configModel;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final SnapshotRestoreEvaluator snapshotRestoreEvaluator;
    private final SearchGuardIndexAccessEvaluator sgIndexAccessEvaluator;
    private final TermsAggregationEvaluator termsAggregationEvaluator;
    private final boolean enterpriseModulesEnabled;
    private DynamicConfigModel dcm;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;
    private final NamedXContentRegistry namedXContentRegistry;
    private final List<String> adminOnlyActions;
    private final List<String> adminOnlyActionExceptions;
    private final Client localClient;
    private final boolean indexReductionFastPath;

    // Hack for Kibana multitenancy index template issue: https://git.floragunn.com/search-guard/search-guard-kibana-plugin/-/issues/381
    private String kibanaServerUsername;
    private String kibanaIndexName;
    private volatile boolean kibanaIndexTemplateFixApplied = false;

    public PrivilegesEvaluator(Client localClient, final ClusterService clusterService, final ThreadPool threadPool,
            final ConfigurationRepository configurationRepository, final IndexNameExpressionResolver resolver, AuditLog auditLog,
            final Settings settings, final ClusterInfoHolder clusterInfoHolder, ActionRequestIntrospector actionRequestIntrospector,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry,
            GuiceDependencies guiceDependencies, NamedXContentRegistry namedXContentRegistry, boolean enterpriseModulesEnabled) {
        super();
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.auditLog = auditLog;
        this.localClient = localClient;

        this.threadContext = threadPool.getThreadContext();
        
        this.indexReductionFastPath = INDEX_REDUCTION_FAST_PATH.get(settings);
        this.checkSnapshotRestoreWritePrivileges = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES,
                ConfigConstants.SG_DEFAULT_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES);

        this.clusterInfoHolder = clusterInfoHolder;
        this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;

        this.actionRequestIntrospector = actionRequestIntrospector;
        snapshotRestoreEvaluator = new SnapshotRestoreEvaluator(settings, auditLog, guiceDependencies);
        sgIndexAccessEvaluator = new SearchGuardIndexAccessEvaluator(settings, auditLog, actionRequestIntrospector);
        termsAggregationEvaluator = new TermsAggregationEvaluator();
        this.enterpriseModulesEnabled = enterpriseModulesEnabled;
        this.namedXContentRegistry = namedXContentRegistry;
        this.adminOnlyActions = settings.getAsList(ConfigConstants.SEARCHGUARD_ACTIONS_ADMIN_ONLY,
                ConfigConstants.SEARCHGUARD_ACTIONS_ADMIN_ONLY_DEFAULT);
        this.adminOnlyActionExceptions = settings.getAsList(ConfigConstants.SEARCHGUARD_ACTIONS_ADMIN_ONLY_EXCEPTIONS, Collections.emptyList());
    }

    @Override
    public void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium) {
        this.dcm = dcm;
        this.configModel = cm;

        this.privilegesInterceptor = ReflectionHelper.instantiatePrivilegesInterceptorImpl(cm, dcm);

        this.kibanaServerUsername = dcm.getKibanaServerUsername();
        this.kibanaIndexName = dcm.getKibanaIndexname();
    }

    private SgRoles getSgRoles(Set<String> roles) {
        return configModel.getSgRoles().filter(roles);
    }

    public boolean isInitialized() {
        return configModel != null && configModel.getSgRoles() != null && dcm != null;
    }

    public PrivilegesEvaluatorResponse evaluate(User user, String action0, ActionRequest request, Task task,
            SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext) {

        if (!isInitialized()) {
            throw new ElasticsearchSecurityException("Search Guard is not initialized.");
        }

        if (action0.startsWith("internal:indices/admin/upgrade")) {
            action0 = "indices:admin/upgrade";
        }

        PrivilegesEvaluatorResponse presponse = new PrivilegesEvaluatorResponse();

        if (isAdminOnlyAction(action0)) {
            log.info("Action " + action0 + " is reserved for users authenticating with an admin certificate");
            presponse.missingPrivileges.add(action0);
            presponse.allowed = false;
            return presponse;
        }

        TransportAddress caller;
        Set<String> mappedRoles;
        SgRoles sgRoles;

        if (specialPrivilegesEvaluationContext == null) {
            caller = Objects.requireNonNull((TransportAddress) this.threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));
            mappedRoles = mapSgRoles(user, caller);
            sgRoles = getSgRoles(mappedRoles);
        } else {
            caller = specialPrivilegesEvaluationContext.getCaller() != null ? specialPrivilegesEvaluationContext.getCaller()
                    : (TransportAddress) this.threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            mappedRoles = specialPrivilegesEvaluationContext.getMappedRoles();
            sgRoles = specialPrivilegesEvaluationContext.getSgRoles();
        }

        if (request instanceof BulkRequest && (com.google.common.base.Strings.isNullOrEmpty(user.getRequestedTenant()))) {
            // Shortcut for bulk actions. The details are checked on the lower level of the BulkShardRequests (Action indices:data/write/bulk[s]).
            // This shortcut is only possible if the default tenant is selected, as we might need to rewrite the request for non-default tenants.
            // No further access check for the default tenant is necessary, as access will be also checked on the TransportShardBulkAction level.

            if (!sgRoles.impliesClusterPermissionPermission(action0)) {
                presponse.missingPrivileges.add(action0);
                presponse.allowed = false;
                log.info("No {}-level perm match for {} [Action [{}]] [RolesChecked {}]", "cluster", user, action0, sgRoles.getRoleNames());
                log.info("No permissions for {}", presponse.missingPrivileges);
                return presponse;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Taking shortcut for BulkRequest; user: " + user);
                }

                presponse.allowed = true;
                return presponse;
            }
        }

        ActionRequestInfo requestInfo = actionRequestIntrospector.getActionRequestInfo(request);
        presponse.actionRequestInfo = requestInfo;

        if (log.isDebugEnabled()) {
            if (requestInfo.isUnknown()) {
                log.debug("### evaluate UNKNOWN " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                        + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext);
            } else if (!requestInfo.isIndexRequest()) {
                log.debug("### evaluate " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                        + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext);
            } else {
                log.debug("### evaluate " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                        + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext + "\nResolved: "
                        + requestInfo.getResolvedIndices() + "\nUresolved: " + requestInfo.getUnresolved() + "\nDNFOF: " + dcm.isDnfofEnabled());
            }
        }

        // check snapshot/restore requests 
        if (snapshotRestoreEvaluator.evaluate(request, task, action0, clusterInfoHolder, presponse).isComplete()) {
            return presponse;
        }

        // SG index access
        if (sgIndexAccessEvaluator.evaluate(request, task, action0, requestInfo, presponse).isComplete()) {
            return presponse;
        }

        if (enterpriseModulesEnabled) {
            presponse.evaluatedDlsFlsConfig = sgRoles.getDlsFls(user, resolver, clusterService, namedXContentRegistry);
        }

        if (isClusterPerm(action0)) {
            if (!sgRoles.impliesClusterPermissionPermission(action0)) {
                presponse.missingPrivileges.add(action0);
                presponse.allowed = false;
                log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]", "cluster", user, requestInfo, action0,
                        sgRoles.getRoleNames());
                log.info("No permissions for {}", presponse.missingPrivileges);
                return presponse;
            }

            if (request instanceof RestoreSnapshotRequest && checkSnapshotRestoreWritePrivileges) {
                // Evaluate additional index privileges                
                return evaluateIndexPrivileges(user, action0, additionalPrivileges(request, action0), request, task, requestInfo, sgRoles,
                        specialPrivilegesEvaluationContext, presponse);
            }

            if (privilegesInterceptor != null) {
                PrivilegesInterceptor.InterceptionResult replaceResult = privilegesInterceptor.replaceKibanaIndex(request, action0, user,
                        requestInfo.getResolvedIndices(), sgRoles);

                if (log.isDebugEnabled()) {
                    log.debug("Result from privileges interceptor for cluster perm: {}", replaceResult);
                }

                if (replaceResult == PrivilegesInterceptor.InterceptionResult.DENY) {
                    auditLog.logMissingPrivileges(action0, request, task);
                    return presponse;
                } else if (replaceResult == PrivilegesInterceptor.InterceptionResult.ALLOW) {
                    presponse.allowed = true;
                    return presponse;
                }
            }

            if (dcm.isDnfofEnabled() && action0.startsWith("indices:data/read/") && requestInfo.containsWildcards()) {

                Set<String> reduced = sgRoles.reduce(requestInfo.getResolvedIndices(), user, ImmutableSet.of(action0), resolver, clusterService);

                if (reduced.isEmpty()) {
                    presponse.allowed = false;
                    return presponse;
                }

                if (!reduced.containsAll(requestInfo.getResolvedIndices().getLocalIndices())) {
                    if (actionRequestIntrospector.reduceIndices(request, reduced, requestInfo)) {
                        // TODO re-check privs?

                        presponse.missingPrivileges.clear();
                        presponse.allowed = true;
                        return presponse;
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Allowing as cluster privilege: " + action0);
            }
            presponse.allowed = true;
            return presponse;
        }

        if (isTenantPerm(action0)) {
            if (!hasTenantPermission(user, sgRoles, action0)) {
                presponse.missingPrivileges.add(action0);
                presponse.allowed = false;
                log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]", "tenant", user, requestInfo, action0, mappedRoles);
                log.info("No permissions for {}", presponse.missingPrivileges);
                return presponse;
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Allowing as tenant privilege: " + action0);
                }
                presponse.allowed = true;
                return presponse;
            }
        }

        if (checkDocWhitelistHeader(user, action0, request)) {
            if (log.isTraceEnabled()) {
                log.trace("Allowing due to doc whitelist: " + action0);
            }
            presponse.allowed = true;
            return presponse;
        }

        // term aggregations
        if (termsAggregationEvaluator.evaluate(requestInfo, request, clusterService, user, sgRoles, resolver, presponse, actionRequestIntrospector)
                .isComplete()) {
            return presponse;
        }

        ImmutableSet<String> allIndexPermsRequired = expandPrivileges(request, action0);

        if (log.isDebugEnabled()) {
            if (allIndexPermsRequired.size() > 1 || !allIndexPermsRequired.contains(action0)) {
                log.debug("Expanded index privileges: " + allIndexPermsRequired);
            }
        }

        return evaluateIndexPrivileges(user, action0, allIndexPermsRequired, request, task, requestInfo, sgRoles, specialPrivilegesEvaluationContext,
                presponse);
    }

    private PrivilegesEvaluatorResponse evaluateIndexPrivileges(User user, String action0, ImmutableSet<String> allIndexPermsRequired,
            ActionRequest request, Task task, ActionRequestInfo actionRequestInfo, SgRoles sgRoles,
            SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext, PrivilegesEvaluatorResponse presponse) {

        if (actionRequestInfo.getResolvedIndices().containsOnlyRemoteIndices()) {
            log.debug(
                    "Request contains only remote indices. We can skip all further checks and let requests be handled by remote cluster: " + action0);
            presponse.missingPrivileges.clear();
            presponse.allowed = true;
            return presponse;
        }

        presponse.missingPrivileges.clear();
        presponse.missingPrivileges.addAll(allIndexPermsRequired);

        if (log.isDebugEnabled()) {
            log.debug("requested resolved indextypes: {}", actionRequestInfo);
        }

        if (log.isDebugEnabled()) {
            log.debug("sgr: {}", sgRoles.getRoleNames());
        }

        // Hack for Kibana multitenancy index template issue: https://git.floragunn.com/search-guard/search-guard-kibana-plugin/-/issues/381
        if (!kibanaIndexTemplateFixApplied && user.getName().equals(kibanaServerUsername)) {
            if ((request instanceof ResizeRequest && ((ResizeRequest) request).getSourceIndex().startsWith(kibanaIndexName)
                    && ((ResizeRequest) request).getSourceIndex().endsWith("_reindex_temp"))
                    || (request instanceof CreateIndexRequest && ((CreateIndexRequest) request).index().startsWith(kibanaIndexName))) {

                kibanaIndexTemplateFixApplied = true;

                IndexTemplateMetadata template = clusterService.state().getMetadata().getTemplates().get("tenant_template");

                if (template != null && template.patterns().size() > 0 && template.patterns().get(0).startsWith(kibanaIndexName)) {
                    presponse.addAdditionalActionFilter(new ActionFilter() {

                        @Override
                        public int order() {
                            return 0;
                        }

                        @Override
                        public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request,
                                ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
                            localClient.admin().indices().deleteTemplate(new DeleteIndexTemplateRequest("tenant_template"),
                                    new ActionListener<AcknowledgedResponse>() {

                                        @Override
                                        public void onResponse(AcknowledgedResponse response) {
                                            log.info("Deleted obsolete tenant_template");
                                            chain.proceed(task, action, request, listener);
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            log.error("Error while deleting tenant_template. Ignoring.", e);
                                            chain.proceed(task, action, request, listener);
                                        }
                                    });
                        }
                    });
                }
            }
        }

        if (privilegesInterceptor != null) {

            PrivilegesInterceptor.InterceptionResult replaceResult = privilegesInterceptor.replaceKibanaIndex(request, action0, user,
                    actionRequestInfo.getResolvedIndices(), sgRoles);

            if (log.isDebugEnabled()) {
                log.debug("Result from privileges interceptor: {}", replaceResult);
            }

            if (replaceResult == PrivilegesInterceptor.InterceptionResult.DENY) {
                auditLog.logMissingPrivileges(action0, request, task);
                return presponse;
            } else if (replaceResult == PrivilegesInterceptor.InterceptionResult.ALLOW) {
                presponse.allowed = true;
                return presponse;
            }
        }

        if (dcm.isDnfofEnabled()
                && (action0.startsWith("indices:data/read/") || action0.startsWith("indices:admin/mappings/fields/get")
                        || action0.equals("indices:admin/shards/search_shards") || action0.equals(ResolveIndexAction.NAME))
                && actionRequestInfo.containsWildcards()) {

            ImmutableSet<String> reduced = sgRoles
                    .reduce(actionRequestInfo.getResolvedIndices(), user, allIndexPermsRequired, resolver, clusterService).with();
            
            if (log.isTraceEnabled()) {
                log.trace("DNFNF: reduced from: " + actionRequestInfo.getResolvedIndices().getLocalIndices() + "\n       to: " + reduced);
            }

            if (reduced.size() < actionRequestInfo.getResolvedIndices().getLocalIndices().size()) {
                ImmutableSet<String> reducedWithRemote = reduced.with(actionRequestInfo.getResolvedIndices().getRemoteIndices());
                if (reducedWithRemote.isEmpty()) {

                    if (dcm.isDnfofForEmptyResultsEnabled()) {

                        if (log.isTraceEnabled()) {
                            log.trace("Allowing empty result due to DNFOF: " + action0);
                        }

                        if (request instanceof SearchRequest) {
                            ((SearchRequest) request).indices(new String[0]);
                            ((SearchRequest) request).indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
                            presponse.missingPrivileges.clear();
                            presponse.allowed = true;
                            return presponse;
                        }

                        if (request instanceof ClusterSearchShardsRequest) {
                            ((ClusterSearchShardsRequest) request).indices(new String[0]);
                            ((ClusterSearchShardsRequest) request).indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
                            presponse.missingPrivileges.clear();
                            presponse.allowed = true;
                            return presponse;
                        }

                        if (request instanceof GetFieldMappingsRequest) {
                            ((GetFieldMappingsRequest) request).indices(new String[0]);
                            ((GetFieldMappingsRequest) request).indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
                            presponse.missingPrivileges.clear();
                            presponse.allowed = true;
                            return presponse;
                        }

                        if (request instanceof ResolveIndexAction.Request) {
                            ((ResolveIndexAction.Request) request).indices(new String[0]);
                            presponse.missingPrivileges.clear();
                            presponse.allowed = true;
                            return presponse;
                        }
                    }

                    if (log.isDebugEnabled()) {
                        log.trace("Denying because no allowed indices were found: " + action0);
                    }

                    presponse.allowed = false;
                    return presponse;
                }
                
                if (log.isTraceEnabled()) {
                    log.trace("DNFOF: Reducing indices to " + reduced);
                }

                if (actionRequestIntrospector.reduceIndices(request, reducedWithRemote, actionRequestInfo)) {

                    if (indexReductionFastPath) {                    
                        if (log.isDebugEnabled()) {
                            log.debug("Allowing reduced request due to DNFOF: " + action0);
                        }
                        
                        presponse.missingPrivileges.clear();
                        presponse.allowed = true;
                        return presponse;
                    }
                    
                    actionRequestInfo = actionRequestIntrospector.getActionRequestInfo(request);
                    presponse.actionRequestInfo = actionRequestInfo;
                }
            }
        }

        //not bulk, mget, etc request here

        if (log.isDebugEnabled()) {
            log.debug("sgr2: {}", sgRoles.getRoleNames());
        }

        PrivilegesEvaluationResult privilegesEvaluationResult = sgRoles.impliesTypePermGlobal(actionRequestInfo.getResolvedIndices(), user,
                allIndexPermsRequired, resolver, clusterService);

        if (privilegesEvaluationResult.getStatus() != Status.PASS) {
            Level logLevel = privilegesEvaluationResult.hasErrors() ? Level.WARN : Level.INFO;

            if (log.isEnabled(logLevel)) {
                log.log(logLevel,
                        "### No index privileges for " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user + "\nResolved Indices: "
                                + actionRequestInfo.getResolvedIndices() + "\nUnresolved: " + actionRequestInfo.getUnresolved() + "\nRoles: "
                                + sgRoles.getRoleNames() + "\nRequired Privileges: " + allIndexPermsRequired + "\n" + privilegesEvaluationResult);
            }

            presponse.allowed = false;
            return presponse;
        }

        if (request instanceof ResizeRequest) {
            if (log.isDebugEnabled()) {
                log.debug("Checking additional create index action for resize operation: " + request);
            }
            ResizeRequest resizeRequest = (ResizeRequest) request;
            CreateIndexRequest createIndexRequest = resizeRequest.getTargetIndexRequest();
            PrivilegesEvaluatorResponse subResponse = evaluate(user, CreateIndexAction.NAME, createIndexRequest, task,
                    specialPrivilegesEvaluationContext);

            if (!subResponse.allowed) {
                return subResponse;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Allowed because we have all indices permissions for " + allIndexPermsRequired);
        }

        presponse.allowed = true;
        return presponse;
    }

    public Set<String> mapSgRoles(final User user, final TransportAddress caller) {
        return this.configModel.mapSgRoles(user, caller);
    }

    public Set<String> getAllConfiguredTenantNames() {
        return configModel.getAllConfiguredTenantNames();
    }

    public boolean multitenancyEnabled() {
        return privilegesInterceptor != null && dcm.isKibanaMultitenancyEnabled();
    }

    public boolean notFailOnForbiddenEnabled() {
        return privilegesInterceptor != null && dcm.isDnfofEnabled();
    }

    public String kibanaIndex() {
        return dcm.getKibanaIndexname();
    }

    public String kibanaServerUsername() {
        return dcm.getKibanaServerUsername();
    }

    private boolean isAdminOnlyAction(String action) {
        if (adminOnlyActions.isEmpty()) {
            return false;
        }

        for (String adminOnlyAction : adminOnlyActions) {
            if (action.startsWith(adminOnlyAction)) {
                if (adminOnlyActionExceptions.isEmpty()) {
                    return true;
                } else {
                    for (String exception : adminOnlyActionExceptions) {
                        if (action.startsWith(exception)) {
                            return false;
                        }
                    }

                    return true;
                }

            }
        }

        return false;
    }

    private <Request extends ActionRequest> ImmutableSet<String> expandPrivileges(Request request, String originalAction) {
        ActionConfig<Request, ?, ?> actionConfig = ActionConfigRegistry.INSTANCE.get(originalAction, request);

        if (actionConfig == null) {
            return ImmutableSet.of(originalAction);
        }

        Set<String> additionalPrivileges = actionConfig.evaluateAdditionalPrivileges(request);

        return ImmutableSet.of(additionalPrivileges, originalAction);
    }

    private <Request extends ActionRequest> ImmutableSet<String> additionalPrivileges(Request request, String originalAction) {
        ActionConfig<Request, ?, ?> actionConfig = ActionConfigRegistry.INSTANCE.get(originalAction, request);

        if (actionConfig == null) {
            return ImmutableSet.empty();
        }

        return actionConfig.evaluateAdditionalPrivileges(request);
    }

    public static boolean isClusterPerm(String action0) {
        return !isTenantPerm(action0) && (action0.startsWith("searchguard:cluster:") || action0.startsWith("cluster:")
                || action0.startsWith("indices:admin/template/") || action0.startsWith("indices:admin/index_template/")
                || action0.startsWith(SearchScrollAction.NAME) || (action0.equals(BulkAction.NAME)) || (action0.equals(MultiGetAction.NAME))
                || (action0.equals(MultiSearchAction.NAME)) || (action0.equals(MultiTermVectorsAction.NAME)) || action0.equals(ReindexAction.NAME)
                || action0.equals("indices:data/read/search/template") || action0.equals("indices:data/read/msearch/template")
                || ActionConfigRegistry.INSTANCE.isClusterAction(action0));

    }

    public static boolean isTenantPerm(String action0) {
        return action0.startsWith("cluster:admin:searchguard:tenant:");
    }

    public static boolean isIndexPerm(String action) {
        return !isClusterPerm(action) && !isTenantPerm(action);
    }

    /**
     * Only used for authinfo REST API
     */
    public Map<String, Boolean> mapTenants(User user, Set<String> roles) {
        SgRoles sgRoles = this.getSgRoles(roles);
        return sgRoles.mapTenants(user, this.configModel.getAllConfiguredTenantNames());
    }

    public Map<String, Boolean> evaluateClusterAndTenantPrivileges(User user, TransportAddress caller, Collection<String> privilegesAskedFor) {
        if (privilegesAskedFor == null || privilegesAskedFor.isEmpty() || user == null) {
            log.debug("Privileges or user empty");
            return Collections.emptyMap();
        }

        // Note: This does not take authtokens into account yet. However, as this is only an API for Kibana and Kibana does not use authtokens, 
        // this does not really matter        
        Set<String> mappedRoles = mapSgRoles(user, caller);
        SgRoles sgRoles = getSgRoles(mappedRoles);
        String requestedTenant = getRequestedTenant(user);
        Set<String> privilegesGranted = new HashSet<>();

        if (configModel.isTenantValid(requestedTenant)) {
            privilegesGranted.addAll(sgRoles.getTenantPermissions(user, requestedTenant).getPermissions());
        } else {
            log.info("Invalid tenant: " + requestedTenant + "; user: " + user);
        }

        privilegesGranted.addAll(sgRoles.getClusterPermissions(user));

        return matchPrivileges(privilegesGranted, privilegesAskedFor);
    }

    private Map<String, Boolean> matchPrivileges(Set<String> privilegesGranted, Collection<String> privilegesAskedFor) {
        log.debug(() -> "Check " + privilegesGranted + " against " + privilegesAskedFor);
        final Map<String, Boolean> result = new HashMap<>();
        for (String privilegeAskedFor : privilegesAskedFor) {

            if (privilegesGranted == null || privilegesGranted.isEmpty()) {
                result.put(privilegeAskedFor, false);
            } else {
                result.put(privilegeAskedFor, WildcardMatcher.matchAny(privilegesGranted, privilegeAskedFor));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private boolean hasTenantPermission(User user, SgRoles sgRoles, String action) {
        String requestedTenant = !Strings.isNullOrEmpty(user.getRequestedTenant()) ? user.getRequestedTenant() : "SGS_GLOBAL_TENANT";

        if (!multitenancyEnabled() && !"SGS_GLOBAL_TENANT".equals(requestedTenant)) {
            return false;
        }

        if (!configModel.isTenantValid(requestedTenant)) {
            log.info("Invalid tenant: " + requestedTenant + "; user: " + user);
            return false;
        }

        return sgRoles.hasTenantPermission(user, requestedTenant, action);
    }

    private String getRequestedTenant(User user) {

        String requestedTenant = user.getRequestedTenant();

        if (Strings.isNullOrEmpty(requestedTenant) || !multitenancyEnabled()) {
            return "SGS_GLOBAL_TENANT";
        } else {
            return requestedTenant;
        }
    }

    public boolean hasClusterPermission(User user, String action) {
        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = specialPrivilegesEvaluationContextProviderRegistry.provide(user,
                threadContext);

        if (specialPrivilegesEvaluationContext != null) {
            user = specialPrivilegesEvaluationContext.getUser();
        }

        TransportAddress caller;
        Set<String> mappedRoles;
        SgRoles sgRoles;

        if (specialPrivilegesEvaluationContext == null) {
            caller = Objects.requireNonNull((TransportAddress) this.threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));
            mappedRoles = mapSgRoles(user, caller);
            sgRoles = getSgRoles(mappedRoles);
        } else {
            caller = specialPrivilegesEvaluationContext.getCaller() != null ? specialPrivilegesEvaluationContext.getCaller()
                    : (TransportAddress) this.threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
            mappedRoles = specialPrivilegesEvaluationContext.getMappedRoles();
            sgRoles = specialPrivilegesEvaluationContext.getSgRoles();
        }

        return sgRoles.impliesClusterPermissionPermission(action);
    }

    private boolean checkDocWhitelistHeader(User user, String action, ActionRequest request) {
        String docWhitelistHeader = threadContext.getHeader(ConfigConstants.SG_DOC_WHITELST_HEADER);

        if (docWhitelistHeader == null) {
            return false;
        }

        if (!(request instanceof GetRequest)) {
            return false;
        }

        try {
            DocumentWhitelist documentWhitelist = DocumentWhitelist.parse(docWhitelistHeader);
            GetRequest getRequest = (GetRequest) request;

            if (documentWhitelist.isWhitelisted(getRequest.index(), getRequest.id())) {
                if (log.isDebugEnabled()) {
                    log.debug("Request " + request + " is whitelisted by " + documentWhitelist);
                }

                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Error while handling document whitelist: " + docWhitelistHeader, e);
            return false;
        }
    }

    public boolean isKibanaRbacEnabled() {
        return dcm.isKibanaRbacEnabled();
    }

}
