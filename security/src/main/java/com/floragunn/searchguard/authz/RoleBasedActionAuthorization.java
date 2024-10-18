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
/*
 * Includes code from https://github.com/opensearch-project/security/blob/780a6b95a70a91d2425731fff09b624f88ef2c1a/src/main/java/org/opensearch/security/privileges/ActionPrivileges.java
 * which is Copyright OpenSearch Contributors
 */

package com.floragunn.searchguard.authz;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.Count;
import com.floragunn.searchsupport.cstate.metrics.CountAggregation;
import com.floragunn.searchsupport.cstate.metrics.Measurement;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;
import com.floragunn.searchsupport.meta.Meta;
import com.selectivem.collections.CompactMapGroupBuilder;
import com.selectivem.collections.DeduplicatingCompactSubSetBuilder;
import com.selectivem.collections.ImmutableCompactSubSet;
import com.selectivem.collections.IndexedImmutableSet;

public class RoleBasedActionAuthorization implements ActionAuthorization, ComponentStateProvider {
    static final StaticSettings.Attribute<ByteSizeValue> PRECOMPUTED_PRIVILEGES_MAX_HEAP_SIZE = //
            StaticSettings.Attribute.define("searchguard.privileges_evaluation.precomputed_privileges.max_heap_size")
                    .withDefault(new ByteSizeValue(10, ByteSizeUnit.MB)).asByteSizeValue();

    private static final Logger log = LogManager.getLogger(RoleBasedActionAuthorization.class);

    private final SgDynamicConfiguration<Role> roles;
    private final ActionGroup.FlattenedIndex actionGroups;
    private final Actions actions;
    private final TenantManager tenantManager;

    private final ClusterPermissions cluster;
    private final ClusterPermissionExclusions clusterExclusions;
    private final IndexPermissions<Role.Index> index;
    private final IndexPermissions<Role.Alias> alias;
    private final IndexPermissions<Role.DataStream> dataStream;
    private final TenantPermissions tenant;
    private final ComponentState componentState;
    private final ByteSizeValue statefulIndexMaxHeapSize;

    private final Pattern universallyDeniedIndices;

    private final MetricsLevel metricsLevel;
    private final Measurement<?> indexActionChecks;
    private final CountAggregation indexActionCheckResults;
    private final CountAggregation indexActionCheckResults_ok;
    private final CountAggregation indexActionCheckResults_insufficient;
    private final CountAggregation indexActionCheckResults_partially;
    private final CountAggregation indexActionTypes;
    private final CountAggregation indexActionTypes_wellKnown;
    private final CountAggregation indexActionTypes_nonWellKnown;
    private final Measurement<?> tenantActionChecks;
    private final CountAggregation tenantActionCheckResults;
    private final CountAggregation tenantActionCheckResults_ok;
    private final CountAggregation tenantActionCheckResults_insufficient;

    private final TimeAggregation statefulIndexRebuild = new TimeAggregation.Milliseconds();

    private volatile StatefulPermissions stateful;

    private final ComponentState statefulIndexState = new ComponentState("index_permissions_stateful");
    private final ComponentState statefulAliasState = new ComponentState("alias_permissions_stateful");
    private final ComponentState statefulDataStreamState = new ComponentState("data_stream_permissions_stateful");

    private Future<?> updateFuture;
    
    public RoleBasedActionAuthorization(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
            Meta indexMetadata, Set<String> tenants, ByteSizeValue statefulIndexMaxHeapSize) {
        this(roles, actionGroups, actions, indexMetadata, tenants, statefulIndexMaxHeapSize, Pattern.blank(), MetricsLevel.NONE,
                MultiTenancyConfigurationProvider.DEFAULT);
    }

    public RoleBasedActionAuthorization(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
            Meta indexMetadata, Set<String> tenants, ByteSizeValue statefulIndexMaxHeapSize, Pattern universallyDeniedIndices,
            MetricsLevel metricsLevel, MultiTenancyConfigurationProvider multiTenancyConfigurationProvider) {
        this.roles = roles;
        this.actionGroups = actionGroups;
        this.actions = actions;
        this.metricsLevel = metricsLevel;
        this.statefulIndexMaxHeapSize = statefulIndexMaxHeapSize;
        this.tenantManager = new TenantManager(tenants, multiTenancyConfigurationProvider);

        this.cluster = new ClusterPermissions(roles, actionGroups, actions, metricsLevel);
        this.clusterExclusions = new ClusterPermissionExclusions(roles, actionGroups, actions);
        this.index = new IndexPermissions<Role.Index>(roles, actionGroups, actions, Role::getIndexPermissions, "index_permissions");
        this.alias = new IndexPermissions<Role.Alias>(roles, actionGroups, actions, Role::getAliasPermissions, "alias_permissions");
        this.dataStream = new IndexPermissions<Role.DataStream>(roles, actionGroups, actions, Role::getDataStreamPermissions,
                "data_stream_permissions");
        this.tenant = new TenantPermissions(roles, actionGroups, actions, this.tenantManager.getConfiguredTenantNames());
        this.universallyDeniedIndices = universallyDeniedIndices;

        this.componentState = new ComponentState("role_based_action_authorization");
        this.componentState.addParts(cluster.getComponentState(), clusterExclusions.getComponentState(), index.getComponentState(),
                tenant.getComponentState(), statefulIndexState, statefulAliasState, statefulDataStreamState);

        if (indexMetadata != null) {
            try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
                StatefulPermissions.Index statefulIndex = new StatefulPermissions.Index(roles, actionGroups, actions, indexMetadata,
                        universallyDeniedIndices, statefulIndexMaxHeapSize, statefulIndexState);
                StatefulPermissions.Alias statefulAlias = new StatefulPermissions.Alias(roles, actionGroups, actions, indexMetadata,
                        universallyDeniedIndices, statefulIndexMaxHeapSize, statefulAliasState);
                StatefulPermissions.DataStream statefulDataStream = new StatefulPermissions.DataStream(roles, actionGroups, actions, indexMetadata,
                        universallyDeniedIndices, statefulIndexMaxHeapSize, statefulDataStreamState);

                this.stateful = new StatefulPermissions(statefulIndex, statefulAlias, statefulDataStream, indexMetadata);
            }

        } else {
            this.statefulIndexState.setState(State.SUSPENDED, "no_meta_data");
            this.statefulAliasState.setState(State.SUSPENDED, "no_meta_data");
            this.statefulDataStreamState.setState(State.SUSPENDED, "no_meta_data");
        }

        this.componentState.updateStateFromParts();
        this.componentState.setConfigVersion(roles.getDocVersion());

        if (metricsLevel.detailedEnabled()) {
            indexActionChecks = new TimeAggregation.Nanoseconds();
            indexActionCheckResults = new CountAggregation();
            tenantActionChecks = new TimeAggregation.Nanoseconds();
            tenantActionCheckResults = new CountAggregation();
            indexActionTypes = new CountAggregation();
        } else if (metricsLevel.basicEnabled()) {
            indexActionChecks = new CountAggregation();
            indexActionCheckResults = new CountAggregation();
            tenantActionChecks = new CountAggregation();
            tenantActionCheckResults = new CountAggregation();
            indexActionTypes = new CountAggregation();
        } else {
            indexActionChecks = CountAggregation.noop();
            indexActionCheckResults = CountAggregation.noop();
            tenantActionChecks = CountAggregation.noop();
            tenantActionCheckResults = CountAggregation.noop();
            indexActionTypes = CountAggregation.noop();
        }

        indexActionCheckResults_ok = indexActionCheckResults.getSubCount("ok");
        indexActionCheckResults_partially = indexActionCheckResults.getSubCount("partially_ok");
        indexActionCheckResults_insufficient = indexActionCheckResults.getSubCount("insufficient");
        tenantActionCheckResults_ok = tenantActionCheckResults.getSubCount("ok");
        tenantActionCheckResults_insufficient = tenantActionCheckResults.getSubCount("insufficient");
        indexActionTypes_wellKnown = indexActionTypes.getSubCount("well_known");
        indexActionTypes_nonWellKnown = indexActionTypes.getSubCount("non_well_known");

        if (metricsLevel.basicEnabled()) {
            this.componentState.addMetrics("index_action_check_results", indexActionCheckResults);
            this.componentState.addMetrics("tenant_action_check_results", tenantActionCheckResults);

            this.componentState.addMetrics("index_action_checks", indexActionChecks, "tenant_action_checks", tenantActionChecks,
                    "statful_index_rebuilds", statefulIndexRebuild);

            this.componentState.addMetrics("index_action_types", indexActionTypes);
        }
    }

    @Override
    public PrivilegesEvaluationResult hasClusterPermission(PrivilegesEvaluationContext context, Action action) throws PrivilegesEvaluationException {
        PrivilegesEvaluationResult result = clusterExclusions.contains(action, context.getMappedRoles());

        if (result.getStatus() != PrivilegesEvaluationResult.Status.PENDING) {
            return result.missingPrivileges(action);
        }

        return cluster.contains(action, context.getMappedRoles());
    }

    @Override
    public PrivilegesEvaluationResult hasIndexPermission(PrivilegesEvaluationContext context, Action primaryAction, ImmutableSet<Action> actions,
            ResolvedIndices resolved, Action.Scope actionScope) throws PrivilegesEvaluationException {
        if (metricsLevel.basicEnabled()) {
            actions.forEach((action) -> {
                indexActionTypes.increment();
                if (action instanceof WellKnownAction) {
                    indexActionTypes_wellKnown.increment();
                } else {
                    indexActionTypes_nonWellKnown.increment();

                    if (metricsLevel.detailedEnabled()) {
                        indexActionTypes_nonWellKnown.getSubCount(action.name()).increment();
                    }
                }
            });
        }

        try (Meter meter = Meter.basic(metricsLevel, indexActionChecks)) {
            LocalContext localContext = new LocalContext(this.index.initializationErrors);
            User user = context.getUser();
            ImmutableSet<String> mappedRoles = context.getMappedRoles();

            if (log.isTraceEnabled()) {
                log.trace("hasIndexPermission()\nuser: " + user + "\nactions: " + actions + "\nresolved: " + resolved);
            }

            // TODO this isBlank() creates a performance penalty, because we always skip the following block
            if (resolved.isLocalAll() && universallyDeniedIndices.isBlank()) {
                // If we have a query on all indices, first check for roles which give privileges for *. Thus, we avoid costly index resolutions

                final Meta.IndexLikeObject rowKey = Meta.NonExistent.STAR;

                try (Meter subMeter = meter.basic("local_all")) {
                    CheckTable<Meta.IndexLikeObject, Action> checkTable = CheckTable.create(rowKey, actions);

                    top: for (Action action : actions) {
                        ImmutableSet<String> rolesWithWildcardIndexPrivileges = index.actionToRolesWithWildcardIndexPrivileges.get(action);

                        if (rolesWithWildcardIndexPrivileges != null && rolesWithWildcardIndexPrivileges.containsAny(mappedRoles)) {
                            if (checkTable.check(rowKey, action)) {
                                break top;
                            }
                        }
                    }

                    if (checkTable.isComplete()) {
                        log.trace("Granting request on local_all");
                        indexActionCheckResults_ok.increment();
                        return PrivilegesEvaluationResult.OK;
                    }

                    if (!context.isResolveLocalAll()) {
                        indexActionCheckResults_insufficient.increment();

                        if (!checkTable.isComplete()) {
                            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Insufficient privileges").with(checkTable);
                        } else {
                            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privileges excluded").with(checkTable);
                        }
                    }
                }
            }

            if (resolved.getLocal().getUnion().isEmpty()) {
                log.debug("No local indices; grant the request");
                indexActionCheckResults_ok.increment();

                return PrivilegesEvaluationResult.OK;
            }

            // --------------------
            // Shallow index checks
            // --------------------

            CheckTable<Meta.IndexLikeObject, Action> shallowCheckTable = CheckTable.create(resolved.getLocal().getUnion(), actions);

            StatefulPermissions stateful = this.stateful;

            if (stateful != null) {
                PrivilegesEvaluationResult resultFromStatefulAlias = stateful.alias.hasPermission(user, mappedRoles, actions, resolved, context,
                        shallowCheckTable);

                if (resultFromStatefulAlias != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("resultFromStatefulAlias: {}", resultFromStatefulAlias);
                    }

                    return resultFromStatefulAlias;
                }
                // Note: statefulAlias.hasPermission() modifies as a side effect the checkTable. 
                // We can carry on using this as an intermediate result and further complete checkTable below.

                PrivilegesEvaluationResult resultFromStatefulDataStream = stateful.dataStream.hasPermission(user, mappedRoles, actions, resolved,
                        context, shallowCheckTable);

                if (resultFromStatefulDataStream != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("resultFromStatefulDataStream: {}", resultFromStatefulDataStream);
                    }

                    return resultFromStatefulDataStream;
                }
                // Note: stateful.dataStream.hasPermission() modifies as a side effect the checkTable. 
                // We can carry on using this as an intermediate result and further complete checkTable below.

                PrivilegesEvaluationResult resultFromStatefulIndex = stateful.index.hasPermission(user, mappedRoles, actions, resolved, context,
                        shallowCheckTable, resolved.getLocal().getPureIndices());

                if (resultFromStatefulIndex != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("resultFromStatefulIndex: {}", resultFromStatefulIndex);
                    }

                    return resultFromStatefulIndex;
                }
                // Note: statefulIndex.hasPermission() modifies as a side effect the checkTable. 
                // We can carry on using this as an intermediate result and further complete checkTable below.
            }

            checkWellKnownActionsWithIndexPatternsForShallowCheckTable(context, localContext, shallowCheckTable, resolved, meter);

            if (!shallowCheckTable.isComplete() && resolved.getLocal().hasAliasOrDataStreamMembers()) {
                // If the indices have containing aliases or datastreams, we can check privileges via these
                checkWellKnownActionsWithIndexPatternsViaParentAliases(context, localContext, shallowCheckTable, resolved, meter);
            }

            // If all actions are well-known and performance critical, the index.rolesToActionToIndexPattern data structure that was evaluated above,
            // would have contained all the actions if privileges are provided. If there are non-well-known actions among the
            // actions, we also have to evaluate action patterns to check the authorization
            boolean coveredByStateful = stateful != null && actions.forAllApplies((a) -> a instanceof WellKnownAction && ((WellKnownAction<?,?,?>) a).isPerformanceCritical()) && stateful.covers(resolved.getLocal().getUnion());

            if (!shallowCheckTable.isComplete() && !coveredByStateful) {
                checkNonWellKnownActions(context, localContext, shallowCheckTable, !resolved.getLocal().getAliases().isEmpty(),
                        !resolved.getLocal().getDataStreams().isEmpty(), meter);
            }

            if (!shallowCheckTable.isComplete() && !coveredByStateful && resolved.getLocal().hasAliasOrDataStreamMembers()) {
                checkNonWellKnownActionsViaParentAliases(context, localContext, shallowCheckTable, resolved, meter);
            }

            if (log.isTraceEnabled()) {
                log.trace("Permissions before exclusions:\n" + shallowCheckTable);
            }

            shallowCheckTable.uncheckRowIf((i) -> universallyDeniedIndices.matches(i.resolveDeepToNames(Meta.Alias.ResolutionMode.NORMAL)));

            if (log.isTraceEnabled()) {
                log.trace("Permissions after universallyDeniedIndices exclusions:\n" + shallowCheckTable);
            }

            if (shallowCheckTable.isComplete()) {
                // TODO check whether we should move universallyDeniedUnchecking here because we will never gain these privileges later
                indexActionCheckResults_ok.increment();
                return PrivilegesEvaluationResult.OK.availableIndices(shallowCheckTable.getCompleteRows(), shallowCheckTable);
            }

            ImmutableSet<Meta.Alias> incompleteAliases = resolved.getLocal().getAliases().matching(e -> !shallowCheckTable.isRowComplete(e));
            ImmutableSet<Meta.DataStream> incompleteDataStreams = resolved.getLocal().getDataStreams()
                    .matching(e -> !shallowCheckTable.isRowComplete(e));

            if (actionScope.canOnlyReferToAliases || (incompleteAliases.isEmpty() && incompleteDataStreams.isEmpty())) {
                // Further resolution does not make sense when:
                // - The action can only refer to aliases
                // - There are actually no aliases or indices to further resolve
                // In these cases abort early

                ImmutableSet<Meta.IndexLikeObject> availableIndices = shallowCheckTable.getCompleteRows();

                if (!availableIndices.isEmpty()) {
                    indexActionCheckResults_partially.increment();
                    return PrivilegesEvaluationResult.PARTIALLY_OK.availableIndices(availableIndices, shallowCheckTable, localContext.errors);
                }

                indexActionCheckResults_insufficient.increment();
                return PrivilegesEvaluationResult.INSUFFICIENT.with(shallowCheckTable, localContext.errors)
                        .reason(resolved.getLocal().size() == 1 ? "Insufficient permissions for the referenced index"
                                : "None of " + resolved.getLocal().size() + " referenced indices has sufficient permissions");
            }

            // --------------------------------------------------------------------------------------
            // Resolve aliases for data streams with missing permissions into individual data streams
            // --------------------------------------------------------------------------------------

            ImmutableSet<Meta.Alias> incompleteAliasesForDataStreams = incompleteAliases
                    .matching((alias) -> alias.members().stream().anyMatch(member -> member instanceof Meta.DataStream));
            CheckTable<Meta.IndexLikeObject, Action> prevCheckTable;

            if (!incompleteAliasesForDataStreams.isEmpty()) {
                if (log.isTraceEnabled()) {
                    log.trace("Resolving aliases for data streams to data streams and checking these privileges on the individual data streams: {}",
                            incompleteAliasesForDataStreams);
                }

                try (Meter subMeter = meter.basic("resolve_datastream_aliases")) {
                    ImmutableSet.Builder<Meta.DataStream> resolvedDataStreamsBuilder = new ImmutableSet.Builder<>();
                    ImmutableSet.Builder<Meta.Alias> retainedAliasesBuilder = new ImmutableSet.Builder<>();

                    for (Meta.Alias alias : incompleteAliasesForDataStreams) {
                        int nonDataStreamMembers = 0;

                        for (Meta.IndexLikeObject member : alias.resolve(primaryAction.aliasResolutionMode())) {
                            if (member instanceof Meta.DataStream) {
                                resolvedDataStreamsBuilder.add((Meta.DataStream) member);
                            } else {
                                nonDataStreamMembers++;
                            }
                        }

                        if (nonDataStreamMembers != 0) {
                            retainedAliasesBuilder.add(alias);
                        }
                    }

                    ImmutableSet<Meta.DataStream> resolvedDataStreams = resolvedDataStreamsBuilder.build();
                    ImmutableSet<Meta.Alias> retainedAliases = retainedAliasesBuilder.build();
                    ImmutableSet<Meta.IndexLikeObject> retainedIndices = shallowCheckTable.getRows().without(incompleteAliasesForDataStreams)
                            .with(retainedAliases);
                    CheckTable<Meta.IndexLikeObject, Action> semiDeepCheckTable = CheckTable.create(retainedIndices.with(resolvedDataStreams),
                            actions);
                    semiDeepCheckTable.checkFrom(shallowCheckTable);

                    if (stateful != null) {
                        stateful.dataStream.hasPermission(user, mappedRoles, actions, resolved, context, semiDeepCheckTable);

                        if (semiDeepCheckTable.isComplete()) {
                            indexActionCheckResults_partially.increment();
                            return PrivilegesEvaluationResult.OK_WHEN_RESOLVED.availableIndices(semiDeepCheckTable.getCompleteRows(),
                                    semiDeepCheckTable.with(shallowCheckTable), localContext.errors);
                        }

                        // Note: statefulIndex.hasPermission() modifies as a side effect the checkTable. 
                        // We can carry on using this as an intermediate result and further complete checkTable below.
                    }

                    checkWellKnownActionsWithIndexPatternsForDataStreams(context, localContext, semiDeepCheckTable, resolved, resolvedDataStreams,
                            subMeter);

                    // If all actions are well-known, the index.rolesToActionToIndexPattern data structure that was evaluated above,
                    // would have contained all the actions if privileges are provided. If there are non-well-known actions among the
                    // actions, we also have to evaluate action patterns to check the authorization

                    if (!semiDeepCheckTable.isComplete() && !coveredByStateful) {
                        checkNonWellKnownActions(context, localContext, semiDeepCheckTable, false, true, meter);
                    }

                    semiDeepCheckTable.uncheckRowIf((i) -> universallyDeniedIndices.matches(i.resolveDeepToNames(Meta.Alias.ResolutionMode.NORMAL)));

                    if (semiDeepCheckTable.isComplete()) {
                        indexActionCheckResults_partially.increment();
                        return PrivilegesEvaluationResult.OK_WHEN_RESOLVED.availableIndices(semiDeepCheckTable.getCompleteRows(),
                                semiDeepCheckTable.with(shallowCheckTable), localContext.errors);
                    }

                    prevCheckTable = semiDeepCheckTable;
                }
            } else {
                prevCheckTable = shallowCheckTable;
            }

            // -------------------------------------------------------------------------------
            // Resolve aliases and data streams with missing permissions into individual indices
            // -------------------------------------------------------------------------------

            ImmutableSet<Meta.IndexCollection> incompleteAliasesAndDataStreams = prevCheckTable.getIncompleteRows()
                    .map(i -> i instanceof Meta.IndexCollection ? (Meta.IndexCollection) i : null);

            try (Meter subMeter = meter.basic("resolve_all_aliases")) {
                ImmutableSet<Meta.Index> incompleteDeepResolved = Meta.IndexCollection.resolveDeep(incompleteAliasesAndDataStreams,
                        primaryAction.aliasResolutionMode());
                ImmutableSet<Meta.IndexLikeObject> retainedIndices = prevCheckTable.getRows().without(incompleteAliasesAndDataStreams);

                CheckTable<Meta.IndexLikeObject, Action> deepCheckTable = CheckTable.create(retainedIndices.with(incompleteDeepResolved), actions);
                deepCheckTable.checkFrom(prevCheckTable);

                if (stateful != null) {
                    stateful.index.hasPermission(user, mappedRoles, actions, resolved, context, deepCheckTable.with(shallowCheckTable),
                            incompleteDeepResolved);

                    if (deepCheckTable.isComplete()) {
                        indexActionCheckResults_partially.increment();
                        return PrivilegesEvaluationResult.OK_WHEN_RESOLVED.availableIndices(deepCheckTable.getCompleteRows(), deepCheckTable,
                                localContext.errors);
                    }

                    // Note: statefulIndex.hasPermission() modifies as a side effect the checkTable. 
                    // We can carry on using this as an intermediate result and further complete checkTable below.
                }

                checkWellKnownActionsWithIndexPatternsForDeepCheckTable(context, localContext, deepCheckTable, resolved, meter);

                // If all actions are well-known, the index.rolesToActionToIndexPattern data structure that was evaluated above,
                // would have contained all the actions if privileges are provided. If there are non-well-known actions among the
                // actions, we also have to evaluate action patterns to check the authorization

                if (!deepCheckTable.isComplete() && !coveredByStateful) {
                    checkNonWellKnownActions(context, localContext, deepCheckTable, false, false, meter);
                }

                if (log.isTraceEnabled()) {
                    log.trace("Permissions before exclusions:\n{}", deepCheckTable);
                }

                deepCheckTable.uncheckRowIf((i) -> universallyDeniedIndices.matches(i.name()));

                if (log.isTraceEnabled()) {
                    log.trace("Permissions after universallyDeniedIndices exclusions:\n{}", deepCheckTable);
                }

                if (log.isTraceEnabled()) {
                    log.trace("Permissions after exclusions:\n{}", deepCheckTable);
                }

                // When using deepCheckTable, we never issue the result PrivilegesEvaluationResult.OK, even if the table is complete.
                // This is because async operations could modify an alias to point to other indices while the privilege evaluation is active. 
                // If we would return PrivilegesEvaluationResult.OK in this case, the index resolve operation executed by the particular
                // action will resolve to different indices than we checked here. Thus, we would give privileges even though they are not present.
                // By returning PrivilegesEvaluationResult.PARTIALLY_OK or OK_WHEN_RESOLVED we always force a replacement of the requested indices/aliases by these
                // for which the user actually has privileges for.
                ImmutableSet<Meta.IndexLikeObject> availableIndices = deepCheckTable.getCompleteRows();

                if (deepCheckTable.isComplete()) {
                    indexActionCheckResults_partially.increment();
                    return PrivilegesEvaluationResult.OK_WHEN_RESOLVED.availableIndices(availableIndices, deepCheckTable.with(shallowCheckTable),
                            localContext.errors);
                } else if (!availableIndices.isEmpty()) {
                    indexActionCheckResults_partially.increment();
                    return PrivilegesEvaluationResult.PARTIALLY_OK.availableIndices(availableIndices, deepCheckTable.with(shallowCheckTable),
                            localContext.errors);
                } else {
                    // If we have gained nothing, we just use the shallowCheckTable for reporting in order to keep the information terse
                    indexActionCheckResults_insufficient.increment();
                    return PrivilegesEvaluationResult.INSUFFICIENT.with(shallowCheckTable, localContext.errors)
                            .reason(resolved.getLocal().size() == 1 ? "Insufficient permissions for the referenced index"
                                    : "None of " + resolved.getLocal().size() + " referenced indices has sufficient permissions");
                }
            }

        } finally {
            indexActionCheckResults.increment();
        }
    }

    /**
     * Checks whether we have privileges for all the indices/aliases/data streams using index patterns. Only works for well-known actions.
     * Note: As this works on the distinction between indices/aliases/data streams provided by resolved object, this method can only be used for the
     * shallowCheckTable. It cannot be used for the deepCheckTable.
     */
    private void checkWellKnownActionsWithIndexPatternsForShallowCheckTable(PrivilegesEvaluationContext context, LocalContext localContext,
            CheckTable<Meta.IndexLikeObject, Action> shallowCheckTable, ResolvedIndices resolved, Meter meter) {
        try (Meter subMeter = meter.basic("well_known_action_index_pattern")) {
            User user = context.getUser();
            ImmutableSet<String> mappedRoles = context.getMappedRoles();
            Collection<Meta.Index> resolvedIndices = resolved.getLocal().getPureIndices();
            Collection<Meta.Alias> resolvedAliases = resolved.getLocal().getAliases();
            Collection<Meta.DataStream> resolvedDataStreams = resolved.getLocal().getDataStreams();
            Collection<Meta.NonExistent> resolvedNonExistentIndices = resolved.getLocal().getNonExistingIndices();

            for (String role : mappedRoles) {
                if (!resolvedIndices.isEmpty()) {
                    if (checkWellKnownActionsWithIndexPatterns(context, localContext, user, shallowCheckTable, role, resolvedIndices, this.index,
                            subMeter)) {
                        return;
                    }
                }

                if (!resolvedNonExistentIndices.isEmpty()) {
                    if (checkWellKnownActionsWithIndexPatterns(context, localContext, user, shallowCheckTable, role, resolvedNonExistentIndices,
                            this.index, subMeter)) {
                        return;
                    }
                }

                if (!resolvedAliases.isEmpty()) {
                    if (checkWellKnownActionsWithIndexPatterns(context, localContext, user, shallowCheckTable, role, resolvedAliases, this.alias,
                            subMeter)) {
                        return;
                    }
                }

                if (!resolvedDataStreams.isEmpty()) {
                    if (checkWellKnownActionsWithIndexPatterns(context, localContext, user, shallowCheckTable, role, resolvedDataStreams,
                            this.dataStream, subMeter)) {
                        return;
                    }
                }

            }
        }
    }

    /**
     * Checks whether we have privileges for all the data streams using index patterns. Only works for well-known actions.
     */
    private void checkWellKnownActionsWithIndexPatternsForDataStreams(PrivilegesEvaluationContext context, LocalContext localContext,
            CheckTable<Meta.IndexLikeObject, Action> semiDeepCheckTable, ResolvedIndices resolved, ImmutableSet<Meta.DataStream> resolvedDataStreams,
            Meter meter) {
        try (Meter subMeter = meter.basic("well_known_action_index_pattern_data_streams")) {
            User user = context.getUser();
            ImmutableSet<String> mappedRoles = context.getMappedRoles();

            for (String role : mappedRoles) {
                if (checkWellKnownActionsWithIndexPatterns(context, localContext, user, semiDeepCheckTable, role, resolvedDataStreams,
                        this.dataStream, subMeter)) {
                    return;
                }
            }
        }
    } 


    private void checkWellKnownActionsWithIndexPatternsForDeepCheckTable(PrivilegesEvaluationContext context, LocalContext localContext,
            CheckTable<Meta.IndexLikeObject, Action> deepCheckTable, ResolvedIndices resolved, Meter meter) {
        try (Meter subMeter = meter.basic("well_known_action_index_pattern")) {
            User user = context.getUser();
            ImmutableSet<String> mappedRoles = context.getMappedRoles();

            for (String role : mappedRoles) {
                // TODO optimize conversion to Collection<ResolvedType> resolvedIndexLikeObjects
                if (checkWellKnownActionsWithIndexPatterns2(context, localContext, user, deepCheckTable, role, deepCheckTable.getRows(), this.index,
                        subMeter)) {
                    return;
                }
            }
        }
    }

    private <PrivilegeType extends Role.Index, ResolvedType extends Meta.IndexLikeObject> boolean checkWellKnownActionsWithIndexPatterns(
            PrivilegesEvaluationContext context, LocalContext localContext, User user, CheckTable<Meta.IndexLikeObject, Action> checkTable,
            String role, Collection<ResolvedType> resolvedIndexLikeObjects, IndexPermissions<PrivilegeType> indexPermissions, Meter meter) {

        ImmutableMap<Action, IndexPattern> actionToIndexPattern = indexPermissions.rolesToActionToIndexPattern.get(role);

        if (actionToIndexPattern != null) {
            for (Action action : checkTable.getColumns()) {
                IndexPattern indexPattern = actionToIndexPattern.get(action);

                if (indexPattern != null) {
                    for (Meta.IndexLikeObject indexLike : resolvedIndexLikeObjects) {
                        if (!checkTable.isChecked(indexLike, action)) {
                            try {
                                if (indexPattern.matches(indexLike.name(), user, context, meter) && checkTable.check(indexLike, action)) {
                                    return true;
                                }
                            } catch (PrivilegesEvaluationException e) {
                                // We can ignore these errors, as this max leads to fewer privileges than available
                                log.error("Error while evaluating index pattern of role {}. Ignoring entry", role, e);
                                this.componentState.addLastException("has_index_permission", e);
                                localContext.add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     * TODO merge with checkWellKnownActionsWithIndexPatterns
     */
    private <PrivilegeType extends Role.Index, ResolvedType extends Meta.IndexLikeObject> boolean checkWellKnownActionsWithIndexPatterns2(
            PrivilegesEvaluationContext context, LocalContext localContext, User user, CheckTable<Meta.IndexLikeObject, Action> checkTable,
            String role, Collection<Meta.IndexLikeObject> resolvedIndexLikeObjects, IndexPermissions<PrivilegeType> indexPermissions, Meter meter) {

        ImmutableMap<Action, IndexPattern> actionToIndexPattern = indexPermissions.rolesToActionToIndexPattern.get(role);

        if (actionToIndexPattern != null) {
            for (Action action : checkTable.getColumns()) {
                IndexPattern indexPattern = actionToIndexPattern.get(action);

                if (indexPattern != null) {
                    for (Meta.IndexLikeObject index : resolvedIndexLikeObjects) {
                        if (!checkTable.isChecked(index, action)) {
                            try {
                                if (indexPattern.matches(index.name(), user, context, meter) && checkTable.check(index, action)) {
                                    return true;
                                }
                            } catch (PrivilegesEvaluationException e) {
                                // We can ignore these errors, as this max leads to fewer privileges than available
                                log.error("Error while evaluating index pattern of role {}. Ignoring entry", role, e);
                                this.componentState.addLastException("has_index_permission", e);
                                localContext.add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     * Checks whether we have privileges for the requested pure indices via alias or data stream permissions.
     */
    private void checkWellKnownActionsWithIndexPatternsViaParentAliases(PrivilegesEvaluationContext context, LocalContext localContext,
            CheckTable<Meta.IndexLikeObject, Action> checkTable, ResolvedIndices resolved, Meter meter) {
        User user = context.getUser();
        ImmutableSet<String> mappedRoles = context.getMappedRoles();

        try (Meter subMeter = meter.basic("parent_aliases_with_well_known_action_index_pattern")) {

            top: for (Meta.IndexLikeObject index : checkTable.getIncompleteRows()) {
                if (index.parentDataStreamName() == null && index.parentAliasNames().isEmpty()) {
                    continue;
                }

                for (String role : mappedRoles) {
                    ImmutableMap<Action, IndexPattern> actionToAliasPattern = this.alias.rolesToActionToIndexPattern.get(role);
                    ImmutableMap<Action, IndexPattern> actionToDataStreamPattern = this.dataStream.rolesToActionToIndexPattern.get(role);

                    if (actionToAliasPattern != null || actionToDataStreamPattern != null) {
                        for (Action action : checkTable.iterateUncheckedColumns(index)) {
                            if (index.parentDataStreamName() != null && actionToDataStreamPattern != null) {
                                IndexPattern indexPattern = actionToDataStreamPattern.get(action);

                                if (indexPattern != null) {
                                    try {
                                        if (indexPattern.matches(index.parentDataStreamName(), user, context, subMeter)
                                                && checkTable.check(index, action)) {
                                            break top;
                                        }
                                    } catch (PrivilegesEvaluationException e) {
                                        // We can ignore these errors, as this max leads to fewer privileges than available
                                        log.error("Error while evaluating index pattern of role {}. Ignoring entry", role, e);
                                        this.componentState.addLastException("has_index_permission", e);
                                        localContext.add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                    }
                                }
                            }

                            Collection<String> ancestorAliasNames = index.ancestorAliasNames();
                            if (!ancestorAliasNames.isEmpty() && actionToAliasPattern != null) {
                                IndexPattern indexPattern = actionToAliasPattern.get(action);

                                if (indexPattern != null) {
                                    try {
                                        if (indexPattern.matches(ancestorAliasNames, user, context, subMeter) && checkTable.check(index, action)) {
                                            break top;
                                        }
                                    } catch (PrivilegesEvaluationException e) {
                                        // We can ignore these errors, as this max leads to fewer privileges than available
                                        log.error("Error while evaluating index pattern of role {}. Ignoring entry", role, e);
                                        this.componentState.addLastException("has_index_permission", e);
                                        localContext.add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void checkNonWellKnownActions(PrivilegesEvaluationContext context, LocalContext localContext,
            CheckTable<Meta.IndexLikeObject, Action> checkTable, boolean checkAliases, boolean checkDataStreams, Meter meter) {
        User user = context.getUser();
        ImmutableSet<String> mappedRoles = context.getMappedRoles();

        try (Meter subMeter = meter.basic("non_well_known_actions_index_pattern")) {
            top: for (String role : mappedRoles) {
                ImmutableMap<Pattern, IndexPattern> actionPatternToIndexPattern = this.index.rolesToActionPatternToIndexPattern.get(role);
                ImmutableMap<Pattern, IndexPattern> actionPatternToAliasPattern = checkAliases
                        ? this.alias.rolesToActionPatternToIndexPattern.get(role)
                        : null;
                ImmutableMap<Pattern, IndexPattern> actionPatternToDataStreamPattern = checkDataStreams
                        ? this.dataStream.rolesToActionPatternToIndexPattern.get(role)
                        : null;

                if (actionPatternToIndexPattern != null || actionPatternToAliasPattern != null || actionPatternToDataStreamPattern != null) {

                    for (Action action : checkTable.getIncompleteColumns()) {
                        if (actionPatternToIndexPattern != null) {
                            for (Map.Entry<Pattern, IndexPattern> entry : actionPatternToIndexPattern.entrySet()) {
                                Pattern actionPattern = entry.getKey();
                                IndexPattern indexPattern = entry.getValue();

                                if (actionPattern.matches(action.name())) {
                                    for (Meta.IndexLikeObject index : checkTable.iterateUncheckedRows(action)) {
                                        if (index instanceof Meta.IndexOrNonExistent) {
                                            try {
                                                if (indexPattern.matches(index.name(), user, context, subMeter) && checkTable.check(index, action)) {
                                                    break top;
                                                }
                                            } catch (PrivilegesEvaluationException e) {
                                                // We can ignore these errors, as this max leads to fewer privileges than available
                                                log.error("Error while evaluating index pattern. Ignoring entry", e);
                                                this.componentState.addLastException("has_index_permission", e);
                                                localContext
                                                        .add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if (actionPatternToAliasPattern != null) {
                            for (Map.Entry<Pattern, IndexPattern> entry : actionPatternToAliasPattern.entrySet()) {
                                Pattern actionPattern = entry.getKey();
                                IndexPattern indexPattern = entry.getValue();

                                if (actionPattern.matches(action.name())) {
                                    for (Meta.IndexLikeObject index : checkTable.iterateUncheckedRows(action)) {
                                        if (index instanceof Meta.Alias) {
                                            try {
                                                if (indexPattern.matches(index.name(), user, context, subMeter) && checkTable.check(index, action)) {
                                                    break top;
                                                }
                                            } catch (PrivilegesEvaluationException e) {
                                                // We can ignore these errors, as this max leads to fewer privileges than available
                                                log.error("Error while evaluating index pattern. Ignoring entry", e);
                                                this.componentState.addLastException("has_index_permission", e);
                                                localContext
                                                        .add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if (actionPatternToDataStreamPattern != null) {
                            for (Map.Entry<Pattern, IndexPattern> entry : actionPatternToDataStreamPattern.entrySet()) {
                                Pattern actionPattern = entry.getKey();
                                IndexPattern indexPattern = entry.getValue();

                                if (actionPattern.matches(action.name())) {
                                    for (Meta.IndexLikeObject index : checkTable.iterateUncheckedRows(action)) {
                                        if (index instanceof Meta.DataStream) {
                                            try {
                                                if (indexPattern.matches(index.name(), user, context, subMeter) && checkTable.check(index, action)) {
                                                    break top;
                                                }
                                            } catch (PrivilegesEvaluationException e) {
                                                // We can ignore these errors, as this max leads to fewer privileges than available
                                                log.error("Error while evaluating index pattern. Ignoring entry", e);
                                                this.componentState.addLastException("has_index_permission", e);
                                                localContext
                                                        .add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks whether we have privileges for the requested pure indices via alias or data stream permissions.
     */
    private void checkNonWellKnownActionsViaParentAliases(PrivilegesEvaluationContext context, LocalContext localContext,
            CheckTable<Meta.IndexLikeObject, Action> checkTable, ResolvedIndices resolved, Meter meter) {
        User user = context.getUser();
        ImmutableSet<String> mappedRoles = context.getMappedRoles();

        try (Meter subMeter = meter.basic("parent_aliases_with_non_well_known_actions_index_pattern")) {

            top: for (Meta.IndexLikeObject index : checkTable.getIncompleteRows()) {
                if (index.parentDataStreamName() == null && index.parentAliasNames().isEmpty()) {
                    continue;
                }

                for (String role : mappedRoles) {
                    ImmutableMap<Pattern, IndexPattern> actionToAliasPattern = this.alias.rolesToActionPatternToIndexPattern.get(role);
                    ImmutableMap<Pattern, IndexPattern> actionToDataStreamPattern = this.dataStream.rolesToActionPatternToIndexPattern.get(role);

                    if (actionToAliasPattern != null || actionToDataStreamPattern != null) {
                        for (Action action : checkTable.iterateUncheckedColumns(index)) {
                            if (index.parentDataStreamName() != null && actionToDataStreamPattern != null) {
                                for (Map.Entry<Pattern, IndexPattern> entry : actionToDataStreamPattern.entrySet()) {
                                    Pattern actionPattern = entry.getKey();
                                    IndexPattern indexPattern = entry.getValue();

                                    if (actionPattern.matches(action.name())) {
                                        try {
                                            if (index.parentDataStreamName() != null
                                                    && indexPattern.matches(index.parentDataStreamName(), user, context, subMeter)
                                                    && checkTable.check(index, action)) {
                                                break top;
                                            }
                                        } catch (PrivilegesEvaluationException e) {
                                            // We can ignore these errors, as this max leads to fewer privileges than available
                                            log.error("Error while evaluating index pattern of role {}. Ignoring entry", role, e);
                                            this.componentState.addLastException("has_index_permission", e);
                                            localContext.add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                        }
                                    }
                                }
                            }

                            Collection<String> ancestorAliasNames = index.ancestorAliasNames();
                            if (!ancestorAliasNames.isEmpty() && actionToAliasPattern != null) {
                                for (Map.Entry<Pattern, IndexPattern> entry : actionToAliasPattern.entrySet()) {
                                    Pattern actionPattern = entry.getKey();
                                    IndexPattern indexPattern = entry.getValue();

                                    if (actionPattern.matches(action.name())) {
                                        try {
                                            if (indexPattern.matches(ancestorAliasNames, user, context, subMeter)
                                                    && checkTable.check(index, action)) {
                                                break top;
                                            }
                                        } catch (PrivilegesEvaluationException e) {
                                            // We can ignore these errors, as this max leads to fewer privileges than available
                                            log.error("Error while evaluating index pattern of role {}. Ignoring entry", role, e);
                                            this.componentState.addLastException("has_index_permission", e);
                                            localContext.add(new PrivilegesEvaluationResult.Error("Error while evaluating index pattern", e, role));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public PrivilegesEvaluationResult hasTenantPermission(PrivilegesEvaluationContext context, Action action, String requestedTenant)
            throws PrivilegesEvaluationException {
        try (Meter meter = Meter.basic(metricsLevel, tenantActionChecks)) {
            User user = context.getUser();
            ImmutableSet<String> mappedRoles = context.getMappedRoles();

            ImmutableList<PrivilegesEvaluationResult.Error> errors = this.tenant.initializationErrors;

            ImmutableMap<String, ImmutableSet<String>> tenantToRoles = tenant.actionToTenantToRoles.get(action);

            if (tenantToRoles != null) {
                ImmutableSet<String> roles = tenantToRoles.get(requestedTenant);

                if (roles != null && roles.containsAny(mappedRoles)) {
                    tenantActionCheckResults_ok.increment();
                    return PrivilegesEvaluationResult.OK;
                }
            }

            if (!tenantManager.isTenantHeaderValid(requestedTenant)) {
                log.info("Invalid tenant requested: {}", requestedTenant);
                tenantActionCheckResults_insufficient.increment();
                return PrivilegesEvaluationResult.INSUFFICIENT.reason("Invalid requested tenant");
            }

            try (Meter subMeter = meter.basic("action_tenant_pattern")) {
                for (String role : mappedRoles) {
                    ImmutableMap<Action, ImmutableSet<Template<Pattern>>> actionToTenantPattern = tenant.roleToActionToTenantPattern.get(role);

                    if (actionToTenantPattern != null) {
                        ImmutableSet<Template<Pattern>> tenantTemplates = actionToTenantPattern.get(action);

                        if (tenantTemplates != null) {
                            for (Template<Pattern> tenantTemplate : tenantTemplates) {
                                try (Meter subMeter2 = subMeter.basic("render_tenant_template")) {
                                    Pattern tenantPattern = tenantTemplate.render(user);

                                    if (tenantPattern.matches(requestedTenant)) {
                                        tenantActionCheckResults_ok.increment();
                                        return PrivilegesEvaluationResult.OK;
                                    }
                                } catch (ExpressionEvaluationException e) {
                                    errors = errors.with(new PrivilegesEvaluationResult.Error("Error while evaluating tenant pattern", e, role));
                                    log.error("Error while evaluating tenant privilege", e);
                                    this.componentState.addLastException("has_tenant_permission", e);
                                }
                            }
                        }
                    }
                }
            }

            tenantActionCheckResults_insufficient.increment();
            return PrivilegesEvaluationResult.INSUFFICIENT.with(errors).missingPrivileges(action);
        } finally {
            tenantActionCheckResults.increment();
        }
    }

    private void update(Meta indexMetadata) {
        if (stateful == null || !stateful.indexMetadata.equals(indexMetadata)) {
            try (Meter meter = Meter.basic(metricsLevel, statefulIndexRebuild)) {
                if (log.isTraceEnabled()) {
                    log.trace("Updating due to index metadata change: {}", indexMetadata);
                }

                StatefulPermissions.Index statefulIndex = new StatefulPermissions.Index(roles, actionGroups, actions, indexMetadata,
                        universallyDeniedIndices, statefulIndexMaxHeapSize, statefulIndexState);
                StatefulPermissions.Alias statefulAlias = new StatefulPermissions.Alias(roles, actionGroups, actions, indexMetadata,
                        universallyDeniedIndices, statefulIndexMaxHeapSize, statefulAliasState);
                StatefulPermissions.DataStream statefulDataStream = new StatefulPermissions.DataStream(roles, actionGroups, actions, indexMetadata,
                        universallyDeniedIndices, statefulIndexMaxHeapSize, statefulDataStreamState);

                this.stateful = new StatefulPermissions(statefulIndex, statefulAlias, statefulDataStream, indexMetadata);

                this.componentState.updateStateFromParts();
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Got metadata update, but metadata was unchanged: {}", indexMetadata);
            }
        }
    }
    

    /**
     * Updates the stateful index configuration asynchronously with the index metadata from the current cluster state.
     * As the update process can take some seconds for clusters with many indices, this method "de-bounces" the updates,
     * i.e., a further update will be only initiated after the previous update has finished. This is okay as this class
     * can handle the case that it do not have the most recent information. It will fall back to slower methods then.
     */
    public synchronized void updateStatefulIndexPrivilegesAsync(ClusterService clusterService, ThreadPool threadPool) {
        long currentMetadataVersion = clusterService.state().metadata().version();

        StatefulPermissions stateful = this.stateful;

        if (stateful != null && currentMetadataVersion <= stateful.indexMetadata.version()) {
            return;
        }

        if (this.updateFuture == null || this.updateFuture.isDone()) {
            this.updateFuture = threadPool.generic().submit(() -> {
                for (int i = 0;; i++) {
                    if (i > 10) {
                        try {
                            // In case we got many consecutive updates, let's sleep a little to let
                            // other operations catch up.
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    
                    Meta indexMetadata = Meta.from(clusterService);

                    synchronized (RoleBasedActionAuthorization.this) {
                        if (indexMetadata.version() <= RoleBasedActionAuthorization.this.stateful.indexMetadata.version()) {
                            return;
                        }
                    }

                    try {
                        log.debug("Updating ActionPrivileges with metadata version {}", indexMetadata.version());
                        update(indexMetadata);
                    } catch (Exception e) {
                        log.error("Error while updating ActionPrivileges", e);
                    } finally {
                        synchronized (RoleBasedActionAuthorization.this) {
                            if (RoleBasedActionAuthorization.this.updateFuture.isCancelled()) {
                                return;
                            }
                        }
                    }
                }
            });
        }
    }

    public synchronized void shutdown() {
        if (this.updateFuture != null && !this.updateFuture.isDone()) {
            this.updateFuture.cancel(true);
        }
    }    

    static class ClusterPermissions implements ComponentStateProvider {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableSet<String> rolesWithWildcardPermissions;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;
        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;
        private final CountAggregation checks;
        private final CountAggregation nonWellKnownChecks;
        private final CountAggregation wildcardChecks;
        private final MetricsLevel metricsLevel;

        ClusterPermissions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions, MetricsLevel metricsLevel) {
            this.componentState = new ComponentState("cluster_permissions");

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRoles = new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());
            ImmutableSet.Builder<String> rolesWithWildcardPermissions = new ImmutableSet.Builder<>();
            ImmutableMap.Builder<String, Pattern> rolesToActionPattern = new ImmutableMap.Builder<>();
            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();
                    ImmutableSet<String> permissions = actionGroups.resolve(role.getClusterPermissions());
                    ImmutableSet<String> excludedPermissions = actionGroups.resolve(role.getExcludeClusterPermissions());
                    Pattern excludedPattern = Pattern.createWithoutExclusions(excludedPermissions);
                    List<Pattern> patterns = new ArrayList<>();

                    if (permissions.contains("*") && excludedPermissions.isEmpty()) {
                        rolesWithWildcardPermissions.add(roleName);
                        continue;
                    }

                    for (String permission : permissions) {
                        if (Pattern.isConstant(permission)) {
                            if (!excludedPattern.matches(permission) && isActionName(permission)) {
                                actionToRoles.get(actions.get(permission)).add(roleName);
                            }
                        } else {
                            Pattern pattern = Pattern.create(permission);

                            ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.clusterActions()
                                    .matching((a) -> pattern.matches(a.name()) && !excludedPattern.matches(a.name()));

                            for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                actionToRoles.get(action).add(roleName);
                            }

                            patterns.add(pattern);
                        }
                    }

                    if (!patterns.isEmpty()) {
                        rolesToActionPattern.put(roleName, Pattern.join(patterns).excluding(excludedPattern));
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: {}\nThis should have been caught before. Ignoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid pattern in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: {}\nIgnoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.actionToRoles = actionToRoles.build(ImmutableSet.Builder::build);
            this.rolesWithWildcardPermissions = rolesWithWildcardPermissions.build();
            this.rolesToActionPattern = rolesToActionPattern.build();
            this.initializationErrors = initializationErrors.build();

            this.componentState.setConfigVersion(roles.getDocVersion());

            this.checks = CountAggregation.basic(metricsLevel);
            this.nonWellKnownChecks = checks.getSubCount("non_well_known_actions");
            this.wildcardChecks = checks.getSubCount("wildcard");
            this.metricsLevel = metricsLevel;

            if (metricsLevel.basicEnabled()) {
                this.componentState.addMetrics("checks", checks);
                this.componentState.addMetrics("action_to_roles_map", new Count(actionToRoles.size()));
                this.componentState.addMetrics("roles_to_action_pattern_map", new Count(rolesToActionPattern.size()));
            }

            if (this.initializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.addDetail(initializationErrors);
            }
        }

        PrivilegesEvaluationResult contains(Action action, Set<String> roles) {
            checks.increment();

            if (rolesWithWildcardPermissions.containsAny(roles)) {
                wildcardChecks.increment();
                return PrivilegesEvaluationResult.OK;
            }

            ImmutableSet<String> rolesWithPrivileges = this.actionToRoles.get(action);

            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(roles)) {
                return PrivilegesEvaluationResult.OK;
            }

            if (!(action instanceof WellKnownAction)) {
                // WellKnownActions are guaranteed to be in the collections above

                try (Meter m = Meter.basic(MetricsLevel.BASIC, nonWellKnownChecks)) {
                    if (metricsLevel.detailedEnabled()) {
                        m.count(action.name());
                    }

                    for (String role : roles) {
                        Pattern pattern = this.rolesToActionPattern.get(role);

                        if (pattern != null && pattern.matches(action.name())) {
                            return PrivilegesEvaluationResult.OK;
                        }
                    }
                }
            }

            return PrivilegesEvaluationResult.INSUFFICIENT.with(initializationErrors).missingPrivileges(action);
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
        }
    }

    static class ClusterPermissionExclusions implements ComponentStateProvider {
        private final ImmutableMap<Action, ImmutableSet<String>> actionToRoles;
        private final ImmutableMap<String, Pattern> rolesToActionPattern;
        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;

        ClusterPermissionExclusions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions) {
            this.componentState = new ComponentState("cluster_permission_exclusions");

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRoles = new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>()
                    .defaultValue((k) -> new ImmutableSet.Builder<String>());
            ImmutableMap.Builder<String, Pattern> rolesToActionPattern = new ImmutableMap.Builder<>();
            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();
                    ImmutableSet<String> permissions = actionGroups.resolve(role.getExcludeClusterPermissions());
                    List<Pattern> patterns = new ArrayList<>();

                    for (String permission : permissions) {
                        if (Pattern.isConstant(permission)) {
                            actionToRoles.get(actions.get(permission)).add(roleName);
                        } else {
                            Pattern pattern = Pattern.create(permission);

                            ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.clusterActions()
                                    .matching((a) -> pattern.matches(a.name()));

                            for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                actionToRoles.get(action).add(roleName);
                            }

                            patterns.add(pattern);
                        }
                    }

                    if (!patterns.isEmpty()) {
                        rolesToActionPattern.put(roleName, Pattern.join(patterns));
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid pattern in role: {}\nThis should have been caught before. Ignoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid pattern in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: {}\nIgnoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.actionToRoles = actionToRoles.build(ImmutableSet.Builder::build);
            this.rolesToActionPattern = rolesToActionPattern.build();
            this.initializationErrors = initializationErrors.build();

            this.componentState.setConfigVersion(roles.getDocVersion());

            if (this.initializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.addDetail(this.initializationErrors);
            }
        }

        PrivilegesEvaluationResult contains(Action action, Set<String> roles) {
            ImmutableSet<String> rolesWithPrivileges = this.actionToRoles.get(action);

            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(roles)) {
                return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privilege exclusion in role " + rolesWithPrivileges.intersection(roles));
            }

            if (!(action instanceof WellKnownAction)) {
                // WellKnownActions are guaranteed to be in the collections above

                for (String role : roles) {
                    Pattern pattern = this.rolesToActionPattern.get(role);

                    if (pattern != null && pattern.matches(action.name())) {
                        return PrivilegesEvaluationResult.INSUFFICIENT.reason("Privilege exclusion in role " + role);
                    }
                }
            }

            return PrivilegesEvaluationResult.PENDING;
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
        }

    }

    static class IndexPermissions<I extends Role.Index> implements ComponentStateProvider {
        private final ImmutableMap<String, ImmutableMap<Action, IndexPattern>> rolesToActionToIndexPattern;
        private final ImmutableMap<String, ImmutableMap<Pattern, IndexPattern>> rolesToActionPatternToIndexPattern;

        private final ImmutableMap<Action, ImmutableSet<String>> actionToRolesWithWildcardIndexPrivileges;

        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;
        private final String componentName;

        IndexPermissions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
                Function<Role, Iterable<I>> getPermissionsFunction, String componentName) {
            this.componentState = new ComponentState(componentName);
            this.componentName = componentName;

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>> rolesToActionToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Action, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>> rolesToActionPatternsToIndexPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Pattern, IndexPattern.Builder>>().defaultValue(
                            (k) -> new ImmutableMap.Builder<Pattern, IndexPattern.Builder>().defaultValue((k2) -> new IndexPattern.Builder()));

            ImmutableMap.Builder<Action, ImmutableSet.Builder<String>> actionToRolesWithWildcardIndexPrivileges = //
                    new ImmutableMap.Builder<Action, ImmutableSet.Builder<String>>().defaultValue((k) -> new ImmutableSet.Builder<String>());

            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (I indexPermissions : getPermissionsFunction.apply(role)) {
                        ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getAllowedActions());

                        for (String permission : permissions) {
                            if (Pattern.isConstant(permission)) {
                                rolesToActionToIndexPattern.get(roleName).get(actions.get(permission)).add(indexPermissions.getIndexPatterns());

                                if (indexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                                    actionToRolesWithWildcardIndexPrivileges.get(actions.get(permission)).add(roleName);
                                }
                            } else {
                                Pattern actionPattern = Pattern.create(permission);

                                ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexLikeActions()
                                        .matching((a) -> actionPattern.matches(a.name()));

                                for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                    rolesToActionToIndexPattern.get(roleName).get(action).add(indexPermissions.getIndexPatterns());

                                    if (indexPermissions.getIndexPatterns().getPattern().isWildcard()) {
                                        actionToRolesWithWildcardIndexPrivileges.get(action).add(roleName);
                                    }
                                }

                                rolesToActionPatternsToIndexPattern.get(roleName).get(actionPattern).add(indexPermissions.getIndexPatterns());
                            }
                        }
                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid configuration in role: {}\nThis should have been caught before. Ignoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid pattern in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: {}\nIgnoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.rolesToActionToIndexPattern = rolesToActionToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));
            this.rolesToActionPatternToIndexPattern = rolesToActionPatternsToIndexPattern.build((b) -> b.build(IndexPattern.Builder::build));

            this.actionToRolesWithWildcardIndexPrivileges = actionToRolesWithWildcardIndexPrivileges.build(ImmutableSet.Builder::build);

            this.initializationErrors = initializationErrors.build();

            this.componentState.setConfigVersion(roles.getDocVersion());

            if (this.initializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.addDetail(initializationErrors);
            }

            if (log.isTraceEnabled()) {
                log.trace("Built {}", this.toString());
            }
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
        }

        @Override
        public String toString() {
            return componentName + ":\nrolesToActionToIndexPattern: " + rolesToActionToIndexPattern + "\nrolesToActionPatternToIndexPattern: "
                    + rolesToActionPatternToIndexPattern + "\nactionToRolesWithWildcardIndexPrivileges: " + actionToRolesWithWildcardIndexPrivileges
                    + "\n";
        }

    }

    static class StatefulPermissions {

        final Index index;
        final Alias alias;
        final DataStream dataStream;
        final Meta indexMetadata;

        StatefulPermissions(Index index, Alias alias, DataStream dataStream, Meta indexMetadata) {
            this.index = index;
            this.alias = alias;
            this.dataStream = dataStream;
            this.indexMetadata = indexMetadata;
        }

        boolean covers(Set<Meta.IndexLikeObject> indexLikeObjects) {
            for (Meta.IndexLikeObject indexLike : indexLikeObjects) {
                if (!indexLike.exists()) {
                    return false;
                }
                
                Meta.IndexLikeObject covered = indexMetadata.getIndexOrLike(indexLike.name());
                
                if (covered == null || !covered.equals(indexLike)) {
                    return false;
                }
            }
            
            return true;
        }
        
        /**
         * Objects of this class collect permissions related to concrete, existing indices. These permissions are sourced from the roles configuration; in particular: the index_permissions attribute.
         * In addition, the alias_permissions attribute is also used to source permissions for the concrete indices that are members of the aliases.
         * 
         * However, we do not hold here permissions for backing indices for data streams. As data streams backing indices are usually not supposed to directly used,
         *s we do not need to have an ultra-fast solution for this. On the other hand, there might be many backing indices, which could lead to a blow up of required heap
         */
        static class Index implements ComponentStateProvider {
            private final ImmutableMap<WellKnownAction<?, ?, ?>, Map<String, ImmutableCompactSubSet<String>>> actionToIndexToRoles;
            private final Meta indexMetadata;

            private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
            private final ComponentState componentState;
            private final Pattern universallyDeniedIndices;
            private final int estimatedByteSize;

            Index(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions, Meta indexMetadata,
                    Pattern universallyDeniedIndices, ByteSizeValue statefulIndexMaxHeapSize, ComponentState componentState) {
                Set<String> roleNames = IndexedImmutableSet.of(roles.getCEntries().keySet());                
                DeduplicatingCompactSubSetBuilder<String> roleSetBuilder = new DeduplicatingCompactSubSetBuilder<>(roleNames);
                CompactMapGroupBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>> indexMapBuilder = new CompactMapGroupBuilder<>(
                        indexMetadata.indexLikeObjects().keySet(), (k2) -> roleSetBuilder.createSubSetBuilder());

                ImmutableMap.Builder<WellKnownAction<?, ?, ?>, CompactMapGroupBuilder.MapBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>> actionToIndexToRoles = //
                        new ImmutableMap.Builder<WellKnownAction<?, ?, ?>, CompactMapGroupBuilder.MapBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>>()
                                .defaultValue((k) -> indexMapBuilder.createMapBuilder());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                Iterable<String> indexNames = indexMetadata.namesOfIndices();

                top: for (String roleName : roleNames) {
                    try {
                        Role role = roles.getCEntry(roleName);
                        roleSetBuilder.next(roleName);

                        for (Role.Index indexPermissions : role.getIndexPermissions()) {
                            ImmutableSet<String> permissions = actionGroups.resolve(indexPermissions.getAllowedActions());
                            Pattern indexPattern = indexPermissions.getIndexPatterns().getPattern();

                            if (indexPattern.isWildcard()) {
                                // Wildcard index patterns are handled in the static IndexPermissions object.
                                continue;
                            }

                            if (indexPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            for (String permission : permissions) {
                                if (Pattern.isConstant(permission)) {
                                    Action action = actions.get(permission);

                                    if (action instanceof WellKnownAction) {
                                        for (String index : indexPattern.iterateMatching(indexNames)) {
                                            actionToIndexToRoles.get((WellKnownAction<?, ?, ?>) action).get(index).add(roleName);
                                        }
                                    }
                                } else {
                                    Pattern pattern = Pattern.create(permission);

                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexLikeActionsPerformanceCritical()
                                            .matching((a) -> pattern.matches(a.name()));

                                    for (String index : indexPattern.iterateMatching(indexNames)) {
                                        for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                            actionToIndexToRoles.get(action).get(index).add(roleName);
                                        }
                                    }
                                }
                            }
                        }

                        for (Role.Index aliasPermissions : role.getAliasPermissions()) {
                            ImmutableSet<String> permissions = actionGroups.resolve(aliasPermissions.getAllowedActions());
                            Pattern aliasPattern = aliasPermissions.getIndexPatterns().getPattern();

                            if (aliasPattern.isWildcard()) {
                                // Wildcard index patterns are handled in the static IndexPermissions object.
                                continue;
                            }

                            if (aliasPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            for (String permission : permissions) {
                                if (Pattern.isConstant(permission)) {
                                    Action action = actions.get(permission);

                                    if (action instanceof WellKnownAction) {
                                        for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                            alias.resolveDeepToNames(Meta.Alias.ResolutionMode.NORMAL).forEach((index) -> {
                                                actionToIndexToRoles.get((WellKnownAction<?, ?, ?>) action).get(index).add(roleName);
                                            });
                                        }
                                    }
                                } else {
                                    Pattern pattern = Pattern.create(permission);

                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexLikeActionsPerformanceCritical()
                                            .matching((a) -> pattern.matches(a.name()));

                                    for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.IndexCollection::name)) {
                                        alias.resolveDeepToNames(Meta.Alias.ResolutionMode.NORMAL).forEach((index) -> {
                                            for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                                actionToIndexToRoles.get((WellKnownAction<?, ?, ?>) action).get(index).add(roleName);
                                            }
                                        });
                                    }
                                }
                            }
                        }

                        if (roleSetBuilder.getEstimatedByteSize() + indexMapBuilder.getEstimatedByteSize() > statefulIndexMaxHeapSize.getBytes()) {
                            log.info("Size of precomputed index privileges exceeds configured limit ({}). Using capped data structure."
                                    + "This might lead to slightly lower performance during privilege evaluation. Consider raising {} or closing unneeded indices.",
                                    statefulIndexMaxHeapSize, PRECOMPUTED_PRIVILEGES_MAX_HEAP_SIZE);
                            break top;
                        }

                        // We do not create a stateful map for backing indices for data streams. As data streams backing indices are usually not supposed to directly used,
                        // we do not need to have an ultra-fast solution for this. On the other hand, there might be many backing indices, which could lead to a blow up of required heap

                    } catch (ConfigValidationException e) {
                        log.error("Invalid pattern in role: {}\nThis should have been caught before. Ignoring role.", roleName, e);
                        rolesToInitializationErrors.get(roleName).with(e);
                    } catch (Exception e) {
                        log.error("Unexpected exception while processing role: {}\nIgnoring role.", roleName, e);
                        rolesToInitializationErrors.get(roleName).with(e);
                    }
                }

                DeduplicatingCompactSubSetBuilder.Completed<String> completedRoleSetBuilder = roleSetBuilder.build();

                this.estimatedByteSize = roleSetBuilder.getEstimatedByteSize() + indexMapBuilder.getEstimatedByteSize();

                this.actionToIndexToRoles = actionToIndexToRoles.build((b) -> b.build(subSetBuilder -> subSetBuilder.build(completedRoleSetBuilder)));
                this.indexMetadata = indexMetadata;

                this.universallyDeniedIndices = universallyDeniedIndices;

                this.rolesToInitializationErrors = rolesToInitializationErrors.build(ImmutableList.Builder::build);
                this.componentState = componentState;
                this.componentState.setConfigVersion(roles.getDocVersion());

                if (this.rolesToInitializationErrors.isEmpty()) {
                    this.componentState.setInitialized();
                    this.componentState.setMessage("Initialized with " + indexMetadata);
                } else {
                    this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                    this.componentState.setMessage("Roles with initialization errors: " + this.rolesToInitializationErrors.keySet());
                    this.componentState.addDetail(rolesToInitializationErrors);
                }

                this.componentState.addMetrics("estimated_byte_size", new Count(this.estimatedByteSize));
            }

            PrivilegesEvaluationResult hasPermission(User user, ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions,
                    ResolvedIndices resolvedIndices, PrivilegesEvaluationContext context, CheckTable<Meta.IndexLikeObject, Action> checkTable,
                    Collection<Meta.Index> indices) throws PrivilegesEvaluationException {
                if (indices.isEmpty()) {
                    return null;
                }


                top: for (Action action : actions) {
                    if (!(action instanceof WellKnownAction && ((WellKnownAction<?, ?,?>) action).isPerformanceCritical())) {
                        continue;
                    }
                    
                    Map<String, ImmutableCompactSubSet<String>> indexToRoles = actionToIndexToRoles.get(action);

                    if (indexToRoles != null) {
                        for (Meta.Index index : indices) {
                            ImmutableCompactSubSet<String> rolesWithPrivileges = indexToRoles.get(index.name());

                            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(mappedRoles)
                                    && !isExcluded(action, index.name(), user, mappedRoles, context)) {

                                if (checkTable.check(index, action)) {
                                    break top;
                                }
                            }
                        }
                    }
                }

                if (checkTable.isComplete()) {
                    return PrivilegesEvaluationResult.OK;
                } else {
                    return null;
                }
            }

            private boolean isExcluded(Action action, String index, User user, ImmutableSet<String> mappedRoles,
                    PrivilegesEvaluationContext context) {
                return universallyDeniedIndices.matches(index);
            }

            @Override
            public ComponentState getComponentState() {
                return this.componentState;
            }

        }

        /**
         * Objects of this class collect permissions related to concrete aliases. These permissions are sourced from the alias_permissions attribute of the roles configuration
         */
        static class Alias implements ComponentStateProvider {
            private final ImmutableMap<WellKnownAction<?, ?, ?>, Map<String, ImmutableCompactSubSet<String>>> actionToAliasToRoles;
            private final Meta indexMetadata;

            private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
            private final ComponentState componentState;
            private final Pattern universallyDeniedIndices;
            private final int estimatedByteSize;

            Alias(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions, Meta indexMetadata,
                    Pattern universallyDeniedIndices, ByteSizeValue statefulIndexMaxHeapSize, ComponentState componentState) {
                Set<String> roleNames = IndexedImmutableSet.of(roles.getCEntries().keySet());                
                DeduplicatingCompactSubSetBuilder<String> roleSetBuilder = new DeduplicatingCompactSubSetBuilder<>(roleNames);
                CompactMapGroupBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>> indexMapBuilder = new CompactMapGroupBuilder<>(
                        indexMetadata.indexLikeObjects().keySet(), (k2) -> roleSetBuilder.createSubSetBuilder());

                ImmutableMap.Builder<WellKnownAction<?, ?, ?>, CompactMapGroupBuilder.MapBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>> actionToAliasToRoles = //
                        new ImmutableMap.Builder<WellKnownAction<?, ?, ?>, CompactMapGroupBuilder.MapBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>>()
                                .defaultValue((k) -> indexMapBuilder.createMapBuilder());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                top: for (String roleName : roleNames) {
                    try {
                        Role role = roles.getCEntry(roleName);
                        roleSetBuilder.next(roleName);

                        for (Role.Alias aliasPermissions : role.getAliasPermissions()) {
                            ImmutableSet<String> permissions = actionGroups.resolve(aliasPermissions.getAllowedActions());
                            Pattern indexPattern = aliasPermissions.getIndexPatterns().getPattern();

                            if (indexPattern.isWildcard()) {
                                // Wildcard index patterns are handled in the static IndexPermissions object.
                                // TODO check
                                continue;
                            }

                            if (indexPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            for (String permission : permissions) {
                                if (Pattern.isConstant(permission)) {
                                    Action action = actions.get(permission);

                                    if (action instanceof WellKnownAction) {
                                        for (Meta.Alias alias : indexPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                            actionToAliasToRoles.get((WellKnownAction<?, ?, ?>) action).get(alias.name()).add(roleName);
                                        }
                                    }
                                } else {
                                    Pattern pattern = Pattern.create(permission);

                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexLikeActionsPerformanceCritical()
                                            .matching((a) -> pattern.matches(a.name()));

                                    for (Meta.Alias alias : indexPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                        for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                            actionToAliasToRoles.get(action).get(alias.name()).add(roleName);
                                        }
                                    }
                                }
                            }

                            if (roleSetBuilder.getEstimatedByteSize() + indexMapBuilder.getEstimatedByteSize() > statefulIndexMaxHeapSize
                                    .getBytes()) {
                                log.info("Size of precomputed index privileges exceeds configured limit ({}). Using capped data structure."
                                        + "This might lead to slightly lower performance during privilege evaluation. Consider raising {} or closing unneeded indices.",
                                        statefulIndexMaxHeapSize, PRECOMPUTED_PRIVILEGES_MAX_HEAP_SIZE);
                                break top;
                            }
                        }

                    } catch (ConfigValidationException e) {
                        log.error("Invalid pattern in role: {}\nThis should have been caught before. Ignoring role.", roleName, e);
                        rolesToInitializationErrors.get(roleName).with(e);
                    } catch (Exception e) {
                        log.error("Unexpected exception while processing role: {}\nIgnoring role.", roleName, e);
                        rolesToInitializationErrors.get(roleName).with(e);
                    }
                }

                DeduplicatingCompactSubSetBuilder.Completed<String> completedRoleSetBuilder = roleSetBuilder.build();
                this.estimatedByteSize = roleSetBuilder.getEstimatedByteSize() + indexMapBuilder.getEstimatedByteSize();
                this.actionToAliasToRoles = actionToAliasToRoles.build((b) -> b.build(subSetBuilder -> subSetBuilder.build(completedRoleSetBuilder)));
                this.indexMetadata = indexMetadata;

                this.universallyDeniedIndices = universallyDeniedIndices;

                this.rolesToInitializationErrors = rolesToInitializationErrors.build(ImmutableList.Builder::build);
                this.componentState = componentState;
                this.componentState.setConfigVersion(roles.getDocVersion());
                this.componentState.setConfigProperty("es_metadata_version", indexMetadata.version());

                if (this.rolesToInitializationErrors.isEmpty()) {
                    this.componentState.setInitialized();
                    this.componentState.setMessage("Initialized with " + indexMetadata.aliases().size());
                } else {
                    this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                    this.componentState.setMessage("Roles with initialization errors: " + this.rolesToInitializationErrors.keySet());
                    this.componentState.addDetail(rolesToInitializationErrors);
                }

                this.componentState.addMetrics("estimated_byte_size", new Count(this.estimatedByteSize));
            }

            PrivilegesEvaluationResult hasPermission(User user, ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions,
                    ResolvedIndices resolvedIndices, PrivilegesEvaluationContext context, CheckTable<Meta.IndexLikeObject, Action> checkTable)
                    throws PrivilegesEvaluationException {
                Collection<Meta.Alias> aliases = resolvedIndices.getLocal().getAliases();
                if (aliases.isEmpty()) {
                    return null;
                }

                top: for (Action action : actions) {
                    if (!(action instanceof WellKnownAction && ((WellKnownAction<?, ?,?>) action).isPerformanceCritical())) {
                        continue;
                    }
                    
                    Map<String, ImmutableCompactSubSet<String>> aliasToRoles = actionToAliasToRoles.get(action);

                    if (aliasToRoles != null) {
                        for (Meta.Alias alias : aliases) {
                            ImmutableCompactSubSet<String> rolesWithPrivileges = aliasToRoles.get(alias.name());

                            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(mappedRoles)
                                    && !isExcluded(action, alias.name(), user, mappedRoles, context)) {

                                if (checkTable.check(alias, action)) {
                                    break top;
                                }
                            }
                        }
                    }
                }

                if (checkTable.isComplete()) {
                    return PrivilegesEvaluationResult.OK;
                } else {
                    return null;
                }
            }

            private boolean isExcluded(Action action, String index, User user, ImmutableSet<String> mappedRoles,
                    PrivilegesEvaluationContext context) {
                // TODO check 
                if (universallyDeniedIndices.matches(index)) {
                    return true;
                }

                return false;
            }

            @Override
            public ComponentState getComponentState() {
                return this.componentState;
            }

        }

        /**
         * Objects of this class collect permissions related to concrete data streams. These permissions are sourced from the data_stream_permissions attribute of the roles configuration
         */
        static class DataStream implements ComponentStateProvider {
            private final ImmutableMap<WellKnownAction<?, ?, ?>, Map<String, ImmutableCompactSubSet<String>>> actionToAliasToRoles;
            private final Meta indexMetadata;

            private final ImmutableMap<String, ImmutableList<Exception>> rolesToInitializationErrors;
            private final ComponentState componentState;
            private final Pattern universallyDeniedIndices;
            private final int estimatedByteSize;

            DataStream(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions, Meta indexMetadata,
                    Pattern universallyDeniedIndices, ByteSizeValue statefulIndexMaxHeapSize, ComponentState componentState) {
                Set<String> roleNames = IndexedImmutableSet.of(roles.getCEntries().keySet());                
                DeduplicatingCompactSubSetBuilder<String> roleSetBuilder = new DeduplicatingCompactSubSetBuilder<>(roleNames);
                CompactMapGroupBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>> indexMapBuilder = new CompactMapGroupBuilder<>(
                        indexMetadata.indexLikeObjects().keySet(), (k2) -> roleSetBuilder.createSubSetBuilder());

                ImmutableMap.Builder<WellKnownAction<?, ?, ?>, CompactMapGroupBuilder.MapBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>> actionToDataStreamToRoles = //
                        new ImmutableMap.Builder<WellKnownAction<?, ?, ?>, CompactMapGroupBuilder.MapBuilder<String, DeduplicatingCompactSubSetBuilder.SubSetBuilder<String>>>()
                                .defaultValue((k) -> indexMapBuilder.createMapBuilder());

                ImmutableMap.Builder<String, ImmutableList.Builder<Exception>> rolesToInitializationErrors = new ImmutableMap.Builder<String, ImmutableList.Builder<Exception>>()
                        .defaultValue((k) -> new ImmutableList.Builder<Exception>());

                Set<String> dataStreamNames = indexMetadata.dataStreams().map(Meta.DataStream::name);

                top: for (String roleName : roleNames) {
                    try {
                        Role role = roles.getCEntries().get(roleName);
                        roleSetBuilder.next(roleName);

                        for (Role.DataStream dataStreamPermissions : role.getDataStreamPermissions()) {
                            ImmutableSet<String> permissions = actionGroups.resolve(dataStreamPermissions.getAllowedActions());
                            Pattern indexPattern = dataStreamPermissions.getIndexPatterns().getPattern();

                            if (indexPattern.isWildcard()) {
                                // Wildcard index patterns are handled in the static IndexPermissions object.
                                // TODO check
                                continue;
                            }

                            if (indexPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            for (String permission : permissions) {
                                if (Pattern.isConstant(permission)) {
                                    Action action = actions.get(permission);

                                    if (action instanceof WellKnownAction) {
                                        for (String alias : indexPattern.iterateMatching(dataStreamNames)) {
                                            actionToDataStreamToRoles.get((WellKnownAction<?, ?, ?>) action).get(alias).add(roleName);
                                        }
                                    }
                                } else {
                                    Pattern pattern = Pattern.create(permission);
                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexLikeActionsPerformanceCritical()
                                            .matching((a) -> pattern.matches(a.name()));

                                    for (String alias : indexPattern.iterateMatching(dataStreamNames)) {
                                        for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                            actionToDataStreamToRoles.get(action).get(alias).add(roleName);
                                        }
                                    }
                                }
                            }
                        }

                        for (Role.Alias aliasPermissions : role.getAliasPermissions()) {
                            ImmutableSet<String> permissions = actionGroups.resolve(aliasPermissions.getAllowedActions());
                            Pattern aliasPattern = aliasPermissions.getIndexPatterns().getPattern();

                            if (aliasPattern.isWildcard()) {
                                // Wildcard index patterns are handled in the static IndexPermissions object.
                                continue;
                            }

                            if (aliasPattern.isBlank()) {
                                // The pattern is likely blank because there are only templated patterns. Index patterns with templates are not handled here, but in the static IndexPermissions object
                                continue;
                            }

                            for (String permission : permissions) {
                                if (Pattern.isConstant(permission)) {
                                    Action action = actions.get(permission);

                                    if (action instanceof WellKnownAction) {
                                        for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.Alias::name)) {
                                            alias.members().forEach((indexLikeObject) -> {
                                                if (indexLikeObject instanceof DataStream) {
                                                    actionToDataStreamToRoles.get((WellKnownAction<?, ?, ?>) action).get(indexLikeObject.name())
                                                            .add(roleName);
                                                }
                                            });
                                        }
                                    }
                                } else {
                                    Pattern pattern = Pattern.create(permission);

                                    ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.indexLikeActionsPerformanceCritical()
                                            .matching((a) -> pattern.matches(a.name()));

                                    for (Meta.Alias alias : aliasPattern.iterateMatching(indexMetadata.aliases(), Meta.IndexCollection::name)) {
                                        alias.members().forEach((indexLikeObject) -> {
                                            if (indexLikeObject instanceof DataStream) {
                                                for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                                    actionToDataStreamToRoles.get((WellKnownAction<?, ?, ?>) action).get(indexLikeObject.name())
                                                            .add(roleName);
                                                }
                                            }
                                        });
                                    }
                                }
                            }
                        }

                        if (roleSetBuilder.getEstimatedByteSize() + indexMapBuilder.getEstimatedByteSize() > statefulIndexMaxHeapSize.getBytes()) {
                            log.info("Size of precomputed index privileges exceeds configured limit ({}). Using capped data structure."
                                    + "This might lead to slightly lower performance during privilege evaluation. Consider raising {} or closing unneeded indices.",
                                    statefulIndexMaxHeapSize, PRECOMPUTED_PRIVILEGES_MAX_HEAP_SIZE);
                            break top;
                        }

                    } catch (ConfigValidationException e) {
                        log.error("Invalid pattern in role: {}\nThis should have been caught before. Ignoring role.", roleName, e);
                        rolesToInitializationErrors.get(roleName).with(e);
                    } catch (Exception e) {
                        log.error("Unexpected exception while processing role: {}\nIgnoring role.", roleName, e);
                        rolesToInitializationErrors.get(roleName).with(e);
                    }
                }

                DeduplicatingCompactSubSetBuilder.Completed<String> completedRoleSetBuilder = roleSetBuilder.build();

                this.estimatedByteSize = roleSetBuilder.getEstimatedByteSize() + indexMapBuilder.getEstimatedByteSize();

                this.actionToAliasToRoles = actionToDataStreamToRoles
                        .build((b) -> b.build(subSetBuilder -> subSetBuilder.build(completedRoleSetBuilder)));
                this.indexMetadata = indexMetadata;

                this.universallyDeniedIndices = universallyDeniedIndices;

                this.rolesToInitializationErrors = rolesToInitializationErrors.build(ImmutableList.Builder::build);
                this.componentState = componentState;
                this.componentState.setConfigVersion(roles.getDocVersion());
                this.componentState.setConfigProperty("es_metadata_version", indexMetadata.version());

                if (this.rolesToInitializationErrors.isEmpty()) {
                    this.componentState.setInitialized();
                    this.componentState.setMessage("Initialized with " + dataStreamNames.size() + " data streams");
                } else {
                    this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                    this.componentState.setMessage("Roles with initialization errors: " + this.rolesToInitializationErrors.keySet());
                    this.componentState.addDetail(rolesToInitializationErrors);
                }
            }

            PrivilegesEvaluationResult hasPermission(User user, ImmutableSet<String> mappedRoles, ImmutableSet<Action> actions,
                    ResolvedIndices resolvedIndices, PrivilegesEvaluationContext context, CheckTable<Meta.IndexLikeObject, Action> checkTable)
                    throws PrivilegesEvaluationException {
                Collection<Meta.DataStream> dataStreams = resolvedIndices.getLocal().getDataStreams();
                if (dataStreams.isEmpty()) {
                    return null;
                }

         
                top: for (Action action : actions) {
                    if (!(action instanceof WellKnownAction && ((WellKnownAction<?, ?,?>) action).isPerformanceCritical())) {
                        continue;
                    }
                    
                    Map<String, ImmutableCompactSubSet<String>> aliasToRoles = actionToAliasToRoles.get(action);

                    if (aliasToRoles != null) {
                        for (Meta.DataStream dataStream : dataStreams) {
                            ImmutableCompactSubSet<String> rolesWithPrivileges = aliasToRoles.get(dataStream.name());

                            if (rolesWithPrivileges != null && rolesWithPrivileges.containsAny(mappedRoles)
                                    && !isExcluded(action, dataStream.name(), user, mappedRoles, context)) {

                                if (checkTable.check(dataStream, action)) {
                                    break top;
                                }
                            }
                        }

                    }
                }

                if (checkTable.isComplete()) {
                    return PrivilegesEvaluationResult.OK;
                } else {
                    return null;
                }
            }

            private boolean isExcluded(Action action, String index, User user, ImmutableSet<String> mappedRoles,
                    PrivilegesEvaluationContext context) {
                // TODO check 
                if (universallyDeniedIndices.matches(index)) {
                    return true;
                }

                return false;
            }

            @Override
            public ComponentState getComponentState() {
                return this.componentState;
            }

        }
    }

    static class TenantPermissions implements ComponentStateProvider {
        private final ImmutableMap<Action, ImmutableMap<String, ImmutableSet<String>>> actionToTenantToRoles;
        private final ImmutableMap<String, ImmutableMap<Action, ImmutableSet<Template<Pattern>>>> roleToActionToTenantPattern;

        private final ImmutableList<PrivilegesEvaluationResult.Error> initializationErrors;
        private final ComponentState componentState;

        TenantPermissions(SgDynamicConfiguration<Role> roles, ActionGroup.FlattenedIndex actionGroups, Actions actions,
                ImmutableSet<String> tenants) {

            ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>> actionToTenantToRoles = //
                    new ImmutableMap.Builder<Action, ImmutableMap.Builder<String, ImmutableSet.Builder<String>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<String, ImmutableSet.Builder<String>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<String>()));

            ImmutableMap.Builder<String, ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>> roleToActionToTenantPattern = //
                    new ImmutableMap.Builder<String, ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>>()
                            .defaultValue((k) -> new ImmutableMap.Builder<Action, ImmutableSet.Builder<Template<Pattern>>>()
                                    .defaultValue((k2) -> new ImmutableSet.Builder<Template<Pattern>>()));

            ImmutableList.Builder<PrivilegesEvaluationResult.Error> initializationErrors = new ImmutableList.Builder<>();

            for (Map.Entry<String, Role> entry : roles.getCEntries().entrySet()) {
                try {
                    String roleName = entry.getKey();
                    Role role = entry.getValue();

                    for (Role.Tenant tenantPermissions : role.getTenantPermissions()) {
                        ImmutableSet<String> permissions = actionGroups.resolve(tenantPermissions.getAllowedActions());

                        for (String permission : permissions) {
                            for (Template<Pattern> tenantPatternTemplate : tenantPermissions.getTenantPatterns()) {
                                if (tenantPatternTemplate.isConstant()) {
                                    Pattern tenantPattern = tenantPatternTemplate.getConstantValue();
                                    ImmutableSet<String> matchingTenants = tenants.matching(tenantPattern);

                                    if (Pattern.isConstant(permission)) {
                                        for (String tenant : matchingTenants) {
                                            actionToTenantToRoles.get(actions.get(permission)).get(tenant).add(roleName);
                                        }
                                    } else {
                                        Pattern actionPattern = Pattern.create(permission);

                                        ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.tenantActions()
                                                .matching((a) -> actionPattern.matches(a.name()));

                                        for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                            for (String tenant : matchingTenants) {
                                                actionToTenantToRoles.get(action).get(tenant).add(roleName);
                                            }
                                        }
                                    }
                                } else {
                                    if (Pattern.isConstant(permission)) {
                                        roleToActionToTenantPattern.get(roleName).get(actions.get(permission)).add(tenantPatternTemplate);
                                    } else {
                                        Pattern actionPattern = Pattern.create(permission);

                                        ImmutableSet<WellKnownAction<?, ?, ?>> providedPrivileges = actions.tenantActions()
                                                .matching((a) -> actionPattern.matches(a.name()));

                                        for (WellKnownAction<?, ?, ?> action : providedPrivileges) {
                                            roleToActionToTenantPattern.get(roleName).get(action).add(tenantPatternTemplate);
                                        }
                                    }
                                }

                            }
                        }

                    }

                } catch (ConfigValidationException e) {
                    log.error("Invalid configuration in role: {}\nThis should have been caught before. Ignoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Invalid configuration in role", e, entry.getKey()));
                } catch (Exception e) {
                    log.error("Unexpected exception while processing role: {}\nIgnoring role.", entry, e);
                    initializationErrors.with(new PrivilegesEvaluationResult.Error("Unexpected exception while processing role", e, entry.getKey()));
                }
            }

            this.actionToTenantToRoles = actionToTenantToRoles.build((b) -> b.build(ImmutableSet.Builder::build));
            this.roleToActionToTenantPattern = roleToActionToTenantPattern.build((b) -> b.build(ImmutableSet.Builder::build));

            this.initializationErrors = initializationErrors.build();
            this.componentState = new ComponentState("tenant_permissions");
            this.componentState.setConfigVersion(roles.getDocVersion());

            if (this.initializationErrors.isEmpty()) {
                this.componentState.setInitialized();
            } else {
                this.componentState.setState(State.PARTIALLY_INITIALIZED, "contains_invalid_roles");
                this.componentState.addDetail(initializationErrors);
            }
        }

        @Override
        public ComponentState getComponentState() {
            return this.componentState;
        }

    }

    static class IndexPattern {

        private final Pattern pattern;
        private final ImmutableList<Role.IndexPatterns.IndexPatternTemplate> patternTemplates;
        private final ImmutableList<Role.IndexPatterns.DateMathExpression> dateMathExpressions;

        IndexPattern(Pattern pattern, ImmutableList<Role.IndexPatterns.IndexPatternTemplate> patternTemplates,
                ImmutableList<Role.IndexPatterns.DateMathExpression> dateMathExpressions) {
            this.pattern = pattern;
            this.patternTemplates = patternTemplates;
            this.dateMathExpressions = dateMathExpressions;
        }

        public boolean matches(String index, User user, PrivilegesEvaluationContext context, Meter meter) throws PrivilegesEvaluationException {
            if (pattern.matches(index)) {
                return true;
            }

            if (!patternTemplates.isEmpty()) {
                for (Role.IndexPatterns.IndexPatternTemplate patternTemplate : this.patternTemplates) {
                    try (Meter subMeter = meter.basic("render_index_pattern_template")) {
                        Pattern pattern = context.getRenderedPattern(patternTemplate.getTemplate());

                        if (pattern.matches(index) && !patternTemplate.getExclusions().matches(index)) {
                            return true;
                        }
                    } catch (ExpressionEvaluationException e) {
                        throw new PrivilegesEvaluationException("Error while evaluating dynamic index pattern: " + patternTemplate, e);
                    }
                }
            }

            if (!dateMathExpressions.isEmpty()) {
                // Note: The use of date math expressions in privileges is deprecated as it conceptually does not fit well. 
                // We need to conceive a replacement
                try (Meter subMeter = meter.basic("render_date_math_expression")) {
                    for (Role.IndexPatterns.DateMathExpression dateMathExpression : this.dateMathExpressions) {
                        try {
                            String resolvedExpression = com.floragunn.searchsupport.queries.DateMathExpressionResolver
                                    .resolveExpression(dateMathExpression.getDateMathExpression());

                            if (!Template.containsPlaceholders(resolvedExpression)) {
                                Pattern pattern = Pattern.create(resolvedExpression);

                                if (pattern.matches(index) && !dateMathExpression.getExclusions().matches(index)) {
                                    return true;
                                }
                            } else {
                                Template<Pattern> patternTemplate = new Template<>(resolvedExpression, Pattern::create);
                                Pattern pattern = patternTemplate.render(user);

                                if (pattern.matches(index) && !dateMathExpression.getExclusions().matches(index)) {
                                    return true;
                                }
                            }
                        } catch (Exception e) {
                            throw new PrivilegesEvaluationException("Error while evaluating date math expression: " + dateMathExpression, e);
                        }
                    }
                }
            }

            return false;
        }

        public boolean matches(Iterable<String> indices, User user, PrivilegesEvaluationContext context, Meter meter)
                throws PrivilegesEvaluationException {
            for (String index : indices) {
                if (this.matches(index, user, context, meter)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public String toString() {
            if (pattern != null && patternTemplates != null && patternTemplates.size() != 0) {
                return pattern + " " + patternTemplates;
            } else if (pattern != null) {
                return pattern.toString();
            } else if (patternTemplates != null) {
                return patternTemplates.toString();
            } else {
                return "-/-";
            }
        }

        static class Builder {
            private List<Pattern> constantPatterns = new ArrayList<>();
            private List<Role.IndexPatterns.IndexPatternTemplate> patternTemplates = new ArrayList<>();
            private List<Role.IndexPatterns.DateMathExpression> dateMathExpressions = new ArrayList<>();

            void add(Role.IndexPatterns indexPattern) {
                this.constantPatterns.add(indexPattern.getPattern());
                this.patternTemplates.addAll(indexPattern.getPatternTemplates());
                this.dateMathExpressions.addAll(indexPattern.getDateMathExpressions());
            }

            IndexPattern build() {
                return new IndexPattern(Pattern.join(constantPatterns), ImmutableList.of(patternTemplates), ImmutableList.of(dateMathExpressions));
            }
        }

    }

    public ActionGroup.FlattenedIndex getActionGroups() {
        return actionGroups;
    }

    private static boolean isActionName(String actionName) {
        return actionName.indexOf(':') != -1;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    static class LocalContext {
        ImmutableList<PrivilegesEvaluationResult.Error> errors;

        LocalContext() {
            this.errors = ImmutableList.empty();
        }

        LocalContext(ImmutableList<PrivilegesEvaluationResult.Error> initialErrors) {
            this.errors = initialErrors;
        }

        void add(PrivilegesEvaluationResult.Error error) {
            this.errors = this.errors.with(error);
        }

    }

}
