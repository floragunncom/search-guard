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

package com.floragunn.searchguard.authz;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.AuthorizationConfig;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.RoleMapping;
import com.floragunn.searchguard.authz.config.Tenant;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authc.session.backend.SessionApi;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult.Status;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.meta.Meta;
import com.google.common.base.Strings;

public class PrivilegesEvaluator implements ComponentStateProvider {
    static final StaticSettings.Attribute<Pattern> ADMIN_ONLY_ACTIONS = //
            StaticSettings.Attribute.define("searchguard.admin_only_actions")
                    .withDefault(Pattern.createUnchecked("cluster:admin:searchguard:config/*", "cluster:admin:searchguard:internal/*")).asPattern();
    static final StaticSettings.Attribute<Pattern> ADMIN_ONLY_INDICES = //
            StaticSettings.Attribute.define("searchguard.admin_only_indices").withDefault(Pattern.createUnchecked("searchguard", ".searchguard",
                    ".searchguard_*", ".signals_watches*", ".signals_accounts", ".signals_settings", ".signals_truststores", ".signals_proxies"))
                    .asPattern();
    static final StaticSettings.Attribute<Boolean> CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES = //
            StaticSettings.Attribute.define("searchguard.check_snapshot_restore_write_privileges").withDefault(true).asBoolean();

    static final StaticSettings.Attribute<Boolean> UNSUPPORTED_RESTORE_SGINDEX_ENABLED = //
            StaticSettings.Attribute.define("searchguard.unsupported.restore.sgindex.enabled").withDefault(false).asBoolean();

    public static final StaticSettings.AttributeSet STATIC_SETTINGS = //
            StaticSettings.AttributeSet.of(ADMIN_ONLY_ACTIONS, ADMIN_ONLY_INDICES, CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES,
                    UNSUPPORTED_RESTORE_SGINDEX_ENABLED);

    private static final Logger log = LogManager.getLogger(PrivilegesEvaluator.class);
    private final ClusterService clusterService;
    private final AuthorizationService authorizationService;
    private final IndexNameExpressionResolver resolver;

    private final AuditLog auditLog;
    private ThreadContext threadContext;

    private MultiTenancyConfigurationProvider multiTenancyConfigurationProvider;

    private final boolean checkSnapshotRestoreWritePrivileges;

    private final ClusterInfoHolder clusterInfoHolder;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final SnapshotRestoreEvaluator snapshotRestoreEvaluator;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;
    private final Pattern adminOnlyActions;
    private final Pattern adminOnlyIndices;
    private final Actions actions;
    private final ComponentState componentState = new ComponentState(10, null, "privileges_evaluator");
    private final ByteSizeValue statefulIndexMaxHeapSize;

    private final AtomicReference<RoleBasedActionAuthorization> actionAuthorization = new AtomicReference<>();
    private volatile AuthorizationConfig authzConfig = AuthorizationConfig.DEFAULT;
    private volatile TenantManager tenantManager = null;

    public PrivilegesEvaluator(ClusterService clusterService, ThreadPool threadPool,
            ConfigurationRepository configurationRepository, AuthorizationService authorizationService, IndexNameExpressionResolver resolver,
            AuditLog auditLog, StaticSettings settings, ClusterInfoHolder clusterInfoHolder, Actions actions,
            ActionRequestIntrospector actionRequestIntrospector,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry,
            GuiceDependencies guiceDependencies, NamedXContentRegistry namedXContentRegistry, boolean enterpriseModulesEnabled) {
        super();
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.auditLog = auditLog;
        this.authorizationService = authorizationService;

        this.threadContext = threadPool.getThreadContext();

        this.checkSnapshotRestoreWritePrivileges = settings.get(CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES);

        this.clusterInfoHolder = clusterInfoHolder;
        this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;

        this.actions = actions;
        this.actionRequestIntrospector = actionRequestIntrospector;
        this.snapshotRestoreEvaluator = new SnapshotRestoreEvaluator(auditLog, guiceDependencies, settings.get(UNSUPPORTED_RESTORE_SGINDEX_ENABLED));
        this.adminOnlyActions = settings.get(ADMIN_ONLY_ACTIONS);
        this.adminOnlyIndices = settings.get(ADMIN_ONLY_INDICES);
        this.statefulIndexMaxHeapSize = settings.get(RoleBasedActionAuthorization.PRECOMPUTED_PRIVILEGES_MAX_HEAP_SIZE);

        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<AuthorizationConfig> config = configMap.get(CType.AUTHZ);
                SgDynamicConfiguration<LegacySgConfig> legacyConfig = configMap.get(CType.CONFIG);
                AuthorizationConfig authzConfig = AuthorizationConfig.DEFAULT;

                if (config != null && config.getCEntry("default") != null) {
                    PrivilegesEvaluator.this.authzConfig = authzConfig = config.getCEntry("default");

                    log.info("Updated authz config:\n" + config);
                    if (log.isDebugEnabled()) {
                        log.debug(authzConfig);
                    }
                } else if (legacyConfig != null && legacyConfig.getCEntry("sg_config") != null) {
                    try {
                        LegacySgConfig sgConfig = legacyConfig.getCEntry("sg_config");
                        PrivilegesEvaluator.this.authzConfig = authzConfig = AuthorizationConfig.parseLegacySgConfig(sgConfig.getSource(), null,
                                settings);

                        log.info("Updated authz config (legacy):\n" + legacyConfig);
                        if (log.isDebugEnabled()) {
                            log.debug(authzConfig);
                        }
                    } catch (ConfigValidationException e) {
                        log.error("Error while parsing sg_config:\n" + e);
                    }
                }

                SgDynamicConfiguration<Role> roles = configMap.get(CType.ROLES);
                SgDynamicConfiguration<Tenant> tenants = configMap.get(CType.TENANTS);

                ActionGroup.FlattenedIndex actionGroups = configMap.get(CType.ACTIONGROUPS) != null
                        ? new ActionGroup.FlattenedIndex(configMap.get(CType.ACTIONGROUPS))
                        : ActionGroup.FlattenedIndex.EMPTY;

                tenantManager = new TenantManager(tenants.getCEntries().keySet(), multiTenancyConfigurationProvider);                
                RoleBasedActionAuthorization newActionAuthorization = new RoleBasedActionAuthorization(roles, actionGroups, actions, Meta.from(clusterService.state().metadata()),
                        tenants.getCEntries().keySet(), statefulIndexMaxHeapSize, adminOnlyIndices, authzConfig.getMetricsLevel(), multiTenancyConfigurationProvider);
                
                RoleBasedActionAuthorization oldActionAuthorization = PrivilegesEvaluator.this.actionAuthorization.getAndSet(newActionAuthorization);
                
                componentState.setConfigVersion(configMap.getVersionsAsString());
                componentState.replacePart(newActionAuthorization.getComponentState());
                componentState.replacePart(actionGroups.getComponentState());
                componentState.updateStateFromParts();
                
                if (oldActionAuthorization != null) {
                    oldActionAuthorization.shutdown();
                }
            }
        });

        clusterService.addListener(new ClusterStateListener() {

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                RoleBasedActionAuthorization actionAuthorization = PrivilegesEvaluator.this.actionAuthorization.get();

                if (actionAuthorization != null) {
                    actionAuthorization.updateStatefulIndexPrivilegesAsync(clusterService, threadPool);
                }
            }
        });
    }

    public boolean isInitialized() {
        return actionAuthorization != null;
    }

    public PrivilegesEvaluationResult evaluate(User user, ImmutableSet<String> mappedRoles, String action0, ActionRequest request, Task task,
            PrivilegesEvaluationContext context, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext) {

        if (!isInitialized()) {
            throw new ElasticsearchSecurityException("Search Guard is not initialized.");
        }

        if (action0.startsWith("internal:indices/admin/upgrade")) {
            action0 = "indices:admin/upgrade";
        }

        Action action = actions.get(action0);

        if (adminOnlyActions.matches(action0)) {
            log.info("Action " + action0 + " is reserved for users authenticating with an admin certificate");
            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Action is reserved for users authenticating with an admin certificate")
                    .missingPrivileges(action);
        }

        if (SessionApi.DeleteAction.NAME.equals(action0)) {
            // Special case for deleting own session: This is always allowed
            return PrivilegesEvaluationResult.OK;
        }

        if (action.isOpen()) {
            return PrivilegesEvaluationResult.OK;
        }

        AuthorizationConfig authzConfig = this.authzConfig;
        ActionAuthorization actionAuthorization;

        if (specialPrivilegesEvaluationContext == null) {
            actionAuthorization = this.actionAuthorization.get();
        } else {
            actionAuthorization = specialPrivilegesEvaluationContext.getActionAuthorization();
        }

        try {
            if (request instanceof BulkRequest && (com.google.common.base.Strings.isNullOrEmpty(user.getRequestedTenant()))) {
                // Shortcut for bulk actions. The details are checked on the lower level of the BulkShardRequests (Action indices:data/write/bulk[s]).
                // This shortcut is only possible if the default tenant is selected, as we might need to rewrite the request for non-default tenants.
                // No further access check for the default tenant is necessary, as access will be also checked on the TransportShardBulkAction level.

                PrivilegesEvaluationResult result = actionAuthorization.hasClusterPermission(context, action);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("No cluster-level permission for {} [Action [{}]] [RolesChecked {}]\n{}", user, action0, mappedRoles, result);
                    return result;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Taking shortcut for BulkRequest; user: " + user);
                    }

                    return result;
                }
            }

            ActionRequestInfo requestInfo = actionRequestIntrospector.getActionRequestInfo(action, request);

            if (log.isDebugEnabled()) {
                if (requestInfo.isUnknown()) {
                    log.debug("### evaluate UNKNOWN " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                            + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext);
                } else if (!requestInfo.isIndexRequest()) {
                    log.debug("### evaluate " + action0 + " (" + request.getClass().getName() + ")\nUser: " + user
                            + "\nspecialPrivilegesEvaluationContext: " + specialPrivilegesEvaluationContext);
                } else {
                    log.debug("### evaluate {} ({})\nUser: {}\nspecialPrivilegesEvaluationContext: {}\nRequestInfo: {}", action0,
                            request.getClass().getName(), user, specialPrivilegesEvaluationContext, requestInfo);
                }
            }

            // check snapshot/restore requests 
            PrivilegesEvaluationResult result = snapshotRestoreEvaluator.evaluate(request, task, action, clusterInfoHolder);
            if (!result.isPending()) {
                return result;
            }

            if (action.isClusterPrivilege()) {
                result = actionAuthorization.hasClusterPermission(context, action);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("### No cluster privileges for {} ({})\nUser: {}\nRoles: {}\n{}", action, request.getClass().getName(), user,
                            mappedRoles, result);
                    return result;
                }

                if (request instanceof RestoreSnapshotRequest && checkSnapshotRestoreWritePrivileges) {
                    // Evaluate additional index privileges                
                    return evaluateIndexPrivileges(user, action, action.expandPrivileges(request), request, task, requestInfo, mappedRoles,
                            authzConfig, actionAuthorization, specialPrivilegesEvaluationContext, context);
                }

                ImmutableSet<Action> additionalPrivileges = action.getAdditionalPrivileges(request);

                if (additionalPrivileges.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Allowing as cluster privilege: " + action0);
                    }
                    return PrivilegesEvaluationResult.OK;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Additional privileges required: " + additionalPrivileges);
                    }

                    return evaluateAdditionalPrivileges(user, action, additionalPrivileges, request, task, requestInfo, mappedRoles, authzConfig,
                            actionAuthorization, specialPrivilegesEvaluationContext, context);
                }
            }

            if (action.isTenantPrivilege()) {
                result = hasTenantPermission(user, mappedRoles, action, actionAuthorization, context);

                if (!result.isOk()) {
                    log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]:\n{}", "tenant", user, requestInfo, action0,
                            mappedRoles, result);
                    return result;
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Allowing as tenant privilege: " + action0);
                    }
                    return result;
                }
            }

            if (checkDocWhitelistHeader(user, action0, request)) {
                if (log.isTraceEnabled()) {
                    log.trace("Allowing due to doc whitelist: " + action0);
                }
                return PrivilegesEvaluationResult.OK;
            }

            ImmutableSet<Action> allIndexPermsRequired = action.expandPrivileges(request);

            if (log.isDebugEnabled()) {
                if (allIndexPermsRequired.size() > 1 || !allIndexPermsRequired.contains(action)) {
                    log.debug("Expanded index privileges: " + allIndexPermsRequired);
                }
            }

            return evaluateIndexPrivileges(user, action, allIndexPermsRequired, request, task, requestInfo, mappedRoles, authzConfig,
                    actionAuthorization, specialPrivilegesEvaluationContext, context);
        } catch (Exception e) {
            log.error("Error while evaluating " + action0 + " (" + request.getClass().getName() + ")", e);
            return PrivilegesEvaluationResult.INSUFFICIENT.with(ImmutableList.of(new PrivilegesEvaluationResult.Error(e.getMessage(), e)));
        }
    }

    private PrivilegesEvaluationResult evaluateIndexPrivileges(User user, Action action, ImmutableSet<Action> requiredPermissions,
            ActionRequest request, Task task, ActionRequestInfo actionRequestInfo, ImmutableSet<String> mappedRoles, AuthorizationConfig authzConfig,
            ActionAuthorization actionAuthorization, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext,
            PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {

        if (actionRequestInfo.getResolvedIndices().containsOnlyRemoteIndices()) {
            log.debug("Request contains only remote indices. We can skip all further checks and let requests be handled by remote cluster: {}",
                    action);
            return PrivilegesEvaluationResult.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("requested resolved indextypes: {}", actionRequestInfo);
        }

        boolean dnfofPossible = authzConfig.isIgnoreUnauthorizedIndices() && authzConfig.getIgnoreUnauthorizedIndicesActions().matches(action.name())
                && (actionRequestInfo.ignoreUnavailable() || actionRequestInfo.containsWildcards());

        if (!dnfofPossible) {
            context.setResolveLocalAll(false);
        }

        ImmutableSet<Action> allIndexPermsRequired = requiredPermissions.matching(Action::isIndexLikePrivilege);
        ImmutableSet<Action> clusterPermissions = requiredPermissions.matching(Action::isClusterPrivilege);

        if (!clusterPermissions.isEmpty()) {
            for (Action clusterPermission : clusterPermissions) {
                PrivilegesEvaluationResult result = actionAuthorization.hasClusterPermission(context, clusterPermission);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    if (log.isEnabled(Level.INFO)) {
                        log.info("### No cluster privileges for " + clusterPermission + " (" + request.getClass().getName() + ")\nUser: " + user
                                + "\nResolved Indices: " + actionRequestInfo.getResolvedIndices() + "\nUnresolved: "
                                + actionRequestInfo.getUnresolved() + "\nRoles: " + mappedRoles + "\n" + result);
                    }

                    return result;
                }
            }
        }

        PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasIndexPermission(context, action, allIndexPermsRequired,
                actionRequestInfo.getMainResolvedIndices(), action.scope());

        if (!actionRequestInfo.getAdditionalResolvedIndices().isEmpty()) {
            for (Map.Entry<Action.AdditionalDimension, ResolvedIndices> entry : actionRequestInfo.getAdditionalResolvedIndices().entrySet()) {
                ImmutableSet<Action> additionalIndexPermissions = entry.getKey().getRequiredPrivileges(allIndexPermsRequired, actions);

                PrivilegesEvaluationResult subResult = actionAuthorization.hasIndexPermission(context, action, additionalIndexPermissions,
                        entry.getValue(), action.scope());

                if (log.isTraceEnabled()) {
                    log.trace("Sub result for {}/{}:\n{}", entry.getKey(), additionalIndexPermissions, subResult);
                }

                privilegesEvaluationResult = privilegesEvaluationResult.withAdditional(entry.getKey(), subResult);
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("Result from privileges evaluation: " + privilegesEvaluationResult.getStatus() + "\n" + privilegesEvaluationResult);
        }

        if (privilegesEvaluationResult.getStatus() == Status.PARTIALLY_OK || privilegesEvaluationResult.getStatus() == Status.OK_WHEN_RESOLVED) {
            if (dnfofPossible) {
                if (log.isDebugEnabled()) {
                    log.debug("Reducing indices to {}; {}\n{}", privilegesEvaluationResult.getAvailableIndices(),
                            privilegesEvaluationResult.getAdditionalAvailableIndices(), privilegesEvaluationResult);
                }

                privilegesEvaluationResult = actionRequestIntrospector.reduceIndices(action, request,
                        privilegesEvaluationResult.getAvailableIndices(), privilegesEvaluationResult.getAdditionalAvailableIndices(),
                        actionRequestInfo);
            } else if (actionRequestInfo.getResolvedIndices().getLocal().hasAliasesOnly()
                    && privilegesEvaluationResult.getStatus() == Status.OK_WHEN_RESOLVED) {

                if (authzConfig.isAllowAliasesIfAllIndicesAllowed() && authzConfig.isIgnoreUnauthorizedIndices()
                        && authzConfig.getIgnoreUnauthorizedIndicesActions().matches(action.name())
                        && actionRequestIntrospector.isReduceIndicesAvailable(action, request)) {
                    // We only come here if no wildcard was requested and ignore_unavailable=false. Thus, normally we won't apply dnfof logic. However, we make
                    // an exception if enabled in the config: aliases.allow_if_all_indices_are_allowed

                    privilegesEvaluationResult = actionRequestIntrospector.reduceIndices(action, request,
                            privilegesEvaluationResult.getAvailableIndices(), privilegesEvaluationResult.getAdditionalAvailableIndices(),
                            actionRequestInfo);
                } else if (actionRequestInfo.getResolvedIndices().getLocal().getAliases().size() == 1
                        && actionRequestInfo.getResolvedIndices().getLocal().getAliases().only().resolve(action.aliasResolutionMode()).size() == 1
                        && privilegesEvaluationResult.getAvailableIndices().size() == 1
                        && (request instanceof GetRequest || request instanceof IndexRequest || request instanceof BulkShardRequest)) {
                    // Special case for actions which can only operate on aliases which contain a single index. 
                    // Such actions are:
                    // - GetRequest (GET document by ID)
                    // - IndexRequest (PUT document; resolves to the write index of the alias)
                    // - BulkShardRequest (backs IndexRequest and bulk document API)
                    // In case we have an OK_WHEN_RESOLVED state for that single index, we let that pass

                    privilegesEvaluationResult = PrivilegesEvaluationResult.OK;
                } else {
                    String reasonForNoIndexReduction = "You have privileges for all members of an alias, but not for the whole alias. Access to the alias is denied, because ";

                    if (!authzConfig.isIgnoreUnauthorizedIndices()) {
                        reasonForNoIndexReduction += "ignore_unauthorized_indices is globally disabled in sg_authz.";
                    } else if (!authzConfig.getIgnoreUnauthorizedIndicesActions().matches(action.name())) {
                        reasonForNoIndexReduction += "the action " + action + " is not available for ignore_unauthorized_indices.";
                    } else {
                        reasonForNoIndexReduction += "ignore_unavailable is set to false. Use ignore_unavailable=true to get access to the indices you have privileges for. Alternatively, set aliases.allow_if_all_indices_are_allowed: true in sg_authz.yml";
                    }

                    privilegesEvaluationResult = privilegesEvaluationResult.status(Status.INSUFFICIENT).reason(reasonForNoIndexReduction);
                }
            } else {
                String reasonForNoIndexReduction = "You have privileges for some, but not all requested indices. However, access to the whole operation is denied, because ";

                if (!authzConfig.isIgnoreUnauthorizedIndices()) {
                    reasonForNoIndexReduction += "ignore_unauthorized_indices is globally disabled in sg_authz.";
                } else if (!authzConfig.getIgnoreUnauthorizedIndicesActions().matches(action.name())) {
                    reasonForNoIndexReduction += "the action " + action + " is not available for ignore_unauthorized_indices.";
                } else {
                    reasonForNoIndexReduction += "ignore_unavailable is set to false. Use ignore_unavailable=true to get access to the indices you have privileges for.";
                }

                privilegesEvaluationResult = privilegesEvaluationResult.status(Status.INSUFFICIENT).reason(reasonForNoIndexReduction);
            }

        } else if (privilegesEvaluationResult.getStatus() == Status.INSUFFICIENT) {
            if (dnfofPossible) {
                if (!actionRequestInfo.getResolvedIndices().getRemoteIndices().isEmpty()) {
                    privilegesEvaluationResult = actionRequestIntrospector.reduceIndices(action, request, ImmutableSet.empty(), ImmutableMap.empty(),
                            actionRequestInfo);
                } else if (authzConfig.getIgnoreUnauthorizedIndicesActionsAllowingEmptyResult().matches(action.name())) {
                    if (log.isTraceEnabled()) {
                        log.trace("Changing result from INSUFFICIENT to EMPTY");
                    }

                    privilegesEvaluationResult = privilegesEvaluationResult.status(Status.EMPTY);
                }
            }
        }

        if (privilegesEvaluationResult.getStatus() == Status.EMPTY) {
            if (actionRequestIntrospector.forceEmptyResult(action, request)) {
                if (log.isDebugEnabled()) {
                    log.debug("DNF: Reducing indices to yield an empty result\n" + privilegesEvaluationResult);
                }

                privilegesEvaluationResult = privilegesEvaluationResult.status(Status.OK);
            } else {
                log.warn("DNFOF for empty results is not available for {} ({})", action, request.getClass().getName());
            }
        }

        if (privilegesEvaluationResult.getStatus() != Status.OK) {
            Level logLevel = privilegesEvaluationResult.hasErrors() ? Level.WARN : Level.INFO;

            if (log.isEnabled(logLevel)) {
                log.log(logLevel,
                        "### No index privileges for {} ({})\nUser: {}\nResolved Indices: {}\n"
                                + "Unresolved: {}\nRoles: {}\nRequired Privileges: {}\n{}",
                        action, request.getClass().getName(), user, actionRequestInfo.getResolvedIndices(), actionRequestInfo.getUnresolved(),
                        mappedRoles, allIndexPermsRequired, privilegesEvaluationResult);
            }

            return privilegesEvaluationResult;
        }

        if (log.isDebugEnabled()) {
            log.debug("Allowed because we have all indices permissions for " + allIndexPermsRequired);
        }

        return PrivilegesEvaluationResult.OK;
    }

    private PrivilegesEvaluationResult evaluateAdditionalPrivileges(User user, Action action, ImmutableSet<Action> additionalPrivileges,
            ActionRequest request, Task task, ActionRequestInfo actionRequestInfo, ImmutableSet<String> mappedRoles, AuthorizationConfig authzConfig,
            ActionAuthorization actionAuthorization, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext,
            PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {

        if (additionalPrivileges.forAllApplies((a) -> a.isIndexLikePrivilege())) {
            return evaluateIndexPrivileges(user, action, additionalPrivileges, request, task, actionRequestInfo, mappedRoles, authzConfig,
                    actionAuthorization, specialPrivilegesEvaluationContext, context);
        }

        ImmutableSet<Action> indexPrivileges = ImmutableSet.empty();

        for (Action additionalPrivilege : additionalPrivileges) {
            if (additionalPrivilege.isClusterPrivilege()) {
                PrivilegesEvaluationResult result = actionAuthorization.hasClusterPermission(context, additionalPrivilege);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("Additional privilege missing: " + result);
                    return result;
                }
            } else if (additionalPrivilege.isTenantPrivilege()) {
                PrivilegesEvaluationResult result = hasTenantPermission(user, mappedRoles, additionalPrivilege, actionAuthorization, context);

                if (result.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                    log.info("Additional privilege missing: " + result);
                    return result;
                }
            } else if (additionalPrivilege.isIndexLikePrivilege()) {
                indexPrivileges = indexPrivileges.with(additionalPrivilege);
            }
        }

        if (!indexPrivileges.isEmpty()) {
            return evaluateIndexPrivileges(user, action, indexPrivileges, request, task, actionRequestInfo, mappedRoles, authzConfig,
                    actionAuthorization, specialPrivilegesEvaluationContext, context);
        } else {
            return PrivilegesEvaluationResult.OK;
        }

    }

    public Set<String> getAllConfiguredTenantNames() {
        return tenantManager.getConfiguredTenantNames();
    }

    public boolean notFailOnForbiddenEnabled() {
        return authzConfig.isIgnoreUnauthorizedIndices();
    }

    public static boolean isTenantPerm(String action0) {
        return action0.startsWith("cluster:admin:searchguard:tenant:");
    }

    public Map<String, Boolean> evaluateClusterAndTenantPrivileges(User user, TransportAddress caller, Collection<String> privilegesAskedFor) {
        if (privilegesAskedFor == null || privilegesAskedFor.isEmpty() || user == null) {
            log.debug("Privileges or user empty");
            return Collections.emptyMap();
        }

        // Note: This does not take authtokens into account yet. However, as this is only an API for Kibana and Kibana does not use authtokens, 
        // this does not really matter        
        ImmutableSet<String> mappedRoles = this.authorizationService.getMappedRoles(user, caller);
        String requestedTenant = getRequestedTenant(user);
        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, false, mappedRoles, null, null, authzConfig.isDebugEnabled(),
                actionRequestIntrospector, null);

        Map<String, Boolean> result = new HashMap<>();

        boolean tenantValid = tenantManager.isTenantHeaderValid(requestedTenant);

        if (!tenantValid) {
            log.info("Invalid tenant: " + requestedTenant + "; user: " + user);
        }
        
        ActionAuthorization actionAuthorization = this.actionAuthorization.get();

        for (String privilegeAskedFor : privilegesAskedFor) {
            Action action = actions.get(privilegeAskedFor);

            try {
                if (action.isTenantPrivilege()) {
                    if (tenantValid) {
                        PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasTenantPermission(context, action,
                                requestedTenant);

                        result.put(privilegeAskedFor, privilegesEvaluationResult.getStatus() == PrivilegesEvaluationResult.Status.OK);
                    } else {
                        result.put(privilegeAskedFor, false);
                    }
                } else {
                    PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasClusterPermission(context, action);

                    result.put(privilegeAskedFor, privilegesEvaluationResult.getStatus() == PrivilegesEvaluationResult.Status.OK);
                }
            } catch (PrivilegesEvaluationException e) {
                log.error("Error while evaluating " + privilegeAskedFor + " for " + user, e);
                result.put(privilegeAskedFor, false);
            }
        }

        return result;
    }

    private PrivilegesEvaluationResult hasTenantPermission(User user, ImmutableSet<String> mappedRoles, Action action,
            ActionAuthorization actionAuthorization, PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {
        String requestedTenant = !Strings.isNullOrEmpty(user.getRequestedTenant()) ? user.getRequestedTenant() : Tenant.GLOBAL_TENANT_ID;

        if (!multiTenancyConfigurationProvider.isMultiTenancyEnabled() && !tenantManager.isGlobalTenantHeader(requestedTenant)) {
            log.warn("Denying request to non-default tenant because MT is disabled: " + requestedTenant);
            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Multi-tenancy is disabled");
        }

        return actionAuthorization.hasTenantPermission(context, action, requestedTenant);
    }

    private String getRequestedTenant(User user) {

        String requestedTenant = user.getRequestedTenant();

        if (tenantManager.isTenantHeaderEmpty(requestedTenant) || !multiTenancyConfigurationProvider.isMultiTenancyEnabled()) {
            return Tenant.GLOBAL_TENANT_ID;
        } else {
            return requestedTenant;
        }
    }

    public boolean hasClusterPermission(User user, String actionName, TransportAddress callerTransportAddress) throws PrivilegesEvaluationException {
        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = specialPrivilegesEvaluationContextProviderRegistry.provide(user,
                threadContext);

        if (specialPrivilegesEvaluationContext != null) {
            user = specialPrivilegesEvaluationContext.getUser();
        }

        ImmutableSet<String> mappedRoles;
        ActionAuthorization actionAuthorization;

        if (specialPrivilegesEvaluationContext == null) {
            mappedRoles = this.authorizationService.getMappedRoles(user, callerTransportAddress);
            actionAuthorization = this.actionAuthorization.get();
        } else {
            mappedRoles = specialPrivilegesEvaluationContext.getMappedRoles();
            actionAuthorization = specialPrivilegesEvaluationContext.getActionAuthorization();
        }

        Action action = this.actions.get(actionName);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, false, mappedRoles, action, null, authzConfig.isDebugEnabled(),
                actionRequestIntrospector, specialPrivilegesEvaluationContext);

        PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasClusterPermission(context, action);

        return privilegesEvaluationResult.getStatus() == PrivilegesEvaluationResult.Status.OK;
    }

    public boolean hasClusterPermissions(User user, List<String> permissions, TransportAddress callerTransportAddress)
            throws PrivilegesEvaluationException {
        if (permissions.isEmpty()) {
            return true;
        }

        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = specialPrivilegesEvaluationContextProviderRegistry.provide(user,
                threadContext);

        if (specialPrivilegesEvaluationContext != null) {
            user = specialPrivilegesEvaluationContext.getUser();
        }

        ImmutableSet<String> mappedRoles;
        ActionAuthorization actionAuthorization;

        if (specialPrivilegesEvaluationContext == null) {
            mappedRoles = this.authorizationService.getMappedRoles(user, callerTransportAddress);
            actionAuthorization = this.actionAuthorization.get();
        } else {
            mappedRoles = specialPrivilegesEvaluationContext.getMappedRoles();
            actionAuthorization = specialPrivilegesEvaluationContext.getActionAuthorization();
        }

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, false, mappedRoles, null, null, authzConfig.isDebugEnabled(),
                actionRequestIntrospector, null);

        for (String permission : permissions) {
            PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasClusterPermission(context, actions.get(permission));

            if (privilegesEvaluationResult.getStatus() != PrivilegesEvaluationResult.Status.OK) {
                return false;
            }
        }

        return true;
    }

    public boolean hasClusterPermissions(String permission, PrivilegesEvaluationContext context) throws PrivilegesEvaluationException {
        ActionAuthorization actionAuthorization;

        if (context.getSpecialPrivilegesEvaluationContext() == null) {
            actionAuthorization = this.actionAuthorization.get();
        } else {
            actionAuthorization = context.getSpecialPrivilegesEvaluationContext().getActionAuthorization();
        }

        PrivilegesEvaluationResult privilegesEvaluationResult = actionAuthorization.hasClusterPermission(context, actions.get(permission));

        return privilegesEvaluationResult.getStatus() == PrivilegesEvaluationResult.Status.OK;

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

    public void setMultiTenancyConfigurationProvider(MultiTenancyConfigurationProvider multiTenancyConfigurationProvider) {
        this.multiTenancyConfigurationProvider = multiTenancyConfigurationProvider;
    }

    public RoleBasedActionAuthorization getActionAuthorization() {
        return actionAuthorization.get();
    }

    public RoleMapping.ResolutionMode getRolesMappingResolution() {
        return authzConfig.getRoleMappingResolution();
    }

    public ActionGroup.FlattenedIndex getActionGroups() {
        return actionAuthorization.get().getActionGroups();
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public IndexNameExpressionResolver getResolver() {
        return resolver;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public boolean isDebugEnabled() {
        return authzConfig.isDebugEnabled();
    }
}
