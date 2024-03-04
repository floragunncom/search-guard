/*
 * Copyright 2015-2024 floragunn GmbH
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

package com.floragunn.searchguard.authz.actions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.IndicesRequest.Replaceable;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstraction.Alias;
import org.elasticsearch.cluster.metadata.IndexAbstraction.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotUtils;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.queries.DateMathExpressionResolver;
import com.floragunn.searchsupport.queries.WildcardExpressionResolver;

public class ActionRequestIntrospector {

    private static final IndicesOptions EXACT = new IndicesOptions(EnumSet.noneOf(IndicesOptions.Option.class),
            EnumSet.noneOf(IndicesOptions.WildcardStates.class));

    private static final Set<String> NAME_BASED_SHORTCUTS_FOR_CLUSTER_ACTIONS = ImmutableSet.of("indices:data/read/msearch/template",
            "indices:data/read/search/template", "indices:data/read/sql/translate", "indices:data/read/sql", "indices:data/read/sql/close_cursor",
            "cluster:admin/scripts/painless/execute");

    private final static Logger log = LogManager.getLogger(ActionRequestIntrospector.class);
    private final Supplier<Meta> metaDataSupplier;
    private final Supplier<SystemIndexAccess> systemIndexAccessSupplier;
    private final BooleanSupplier isLocalNodeElectedMaster;
    private final Function<RestoreSnapshotRequest, SnapshotInfo> getSnapshotInfoFunction;

    public ActionRequestIntrospector(Supplier<Meta> metaDataSupplier, Supplier<SystemIndexAccess> systemIndexAccessSupplier,
            BooleanSupplier isLocalNodeElectedMaster, Function<RestoreSnapshotRequest, SnapshotInfo> getSnapshotInfoFunction) {
        this.metaDataSupplier = metaDataSupplier;
        this.isLocalNodeElectedMaster = isLocalNodeElectedMaster;
        this.getSnapshotInfoFunction = getSnapshotInfoFunction;
        this.systemIndexAccessSupplier = systemIndexAccessSupplier;
    }

    public ActionRequestInfo getActionRequestInfo(Action action, Object request) {

        if (NAME_BASED_SHORTCUTS_FOR_CLUSTER_ACTIONS.contains(action.name())) {
            return CLUSTER_REQUEST;
        }

        if (request instanceof SingleShardRequest) {
            // SingleShardRequest can reference exactly one index or no indices at all (which might be a bit surprising)
            SingleShardRequest<?> singleShardRequest = (SingleShardRequest<?>) request;

            if (singleShardRequest.index() != null) {
                return new ActionRequestInfo(singleShardRequest.index(), SingleShardRequest.INDICES_OPTIONS, IndicesRequestInfo.Scope.ANY);
            } else {
                // Actions which can have a null index:
                // - AnalyzeAction.Request
                // - PainlessExecuteAction ("cluster:admin/scripts/painless/execute"): This is a cluster action, so index information does not matter here
                // Here, we assume that the request references all indices. However, this is not really true for the AnalyzeAction.Request, which indeed references no index at all in this case.
                // We have in reduceIndices() a special case for AnalyzeAction.Request which takes care that the user just needs to have the privilege for any index.

                return new ActionRequestInfo("*", IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN, IndicesRequestInfo.Scope.ANY);
            }
        } else if (request instanceof IndicesRequest) {
            if (action.isDataStreamPrivilege()) {
                return new ActionRequestInfo((IndicesRequest) request, IndicesRequestInfo.Scope.DATA_STREAM);
            } else if (request instanceof AliasesRequest) {
                AliasesRequest aliasesRequest = (AliasesRequest) request;
                IndicesRequest indicesRequest = (IndicesRequest) request;

                return new ActionRequestInfo(indicesRequest.indices(), indicesRequest.indicesOptions(), IndicesRequestInfo.Scope.INDICES_DATA_STREAMS)//
                        .additional(IndicesRequestInfo.AdditionalInfoRole.ALIASES, aliasesRequest.aliases(),
                                aliasesRequest.expandAliasesWildcards() ? IndicesOptions.lenientExpandHidden() : EXACT,
                                IndicesRequestInfo.Scope.ALIAS);
            } else if (request instanceof IndicesAliasesRequest) {
                IndicesAliasesRequest indicesAliasesRequest = (IndicesAliasesRequest) request;
                ActionRequestInfo result = new ActionRequestInfo(ImmutableSet.empty());

                for (IndicesAliasesRequest.AliasActions aliasAction : indicesAliasesRequest.getAliasActions()) {
                    switch (aliasAction.actionType()) {
                    case ADD:
                        result = result.with(aliasAction, IndicesRequestInfo.Scope.INDICES_DATA_STREAMS) //
                                .additional(IndicesRequestInfo.AdditionalInfoRole.ALIASES, aliasAction.aliases(), EXACT,
                                        IndicesRequestInfo.Scope.ALIAS);
                        break;
                    case REMOVE:
                        result = result.with(aliasAction, IndicesRequestInfo.Scope.INDICES_DATA_STREAMS) //
                                .additional(IndicesRequestInfo.AdditionalInfoRole.ALIASES, aliasAction.aliases(),
                                        aliasAction.expandAliasesWildcards() ? IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN : EXACT,
                                        IndicesRequestInfo.Scope.ALIAS);
                        break;
                    case REMOVE_INDEX:
                        // This is the most weird part of IndicesAliasesRequest: You can delete an index - completely unrelated to aliases.
                        result = result.with(aliasAction, IndicesRequestInfo.Scope.INDICES_DATA_STREAMS);
                        break;
                    }
                }

                return result;
            } else if (request instanceof CreateIndexRequest) {
                CreateIndexRequest createIndexRequest = (CreateIndexRequest) request;

                if (createIndexRequest.aliases() == null || createIndexRequest.aliases().isEmpty()) {
                    return new ActionRequestInfo((IndicesRequest) request, IndicesRequestInfo.Scope.ANY);
                } else {
                    return new ActionRequestInfo((IndicesRequest) request, IndicesRequestInfo.Scope.ANY)//
                            .additional(IndicesRequestInfo.AdditionalInfoRole.MANAGE_ALIASES,
                                    ImmutableList.of(createIndexRequest.aliases()).map(a -> a.name()), EXACT, IndicesRequestInfo.Scope.ALIAS);
                }
            } else if (request instanceof PutMappingRequest) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;

                if (putMappingRequest.getConcreteIndex() != null) {
                    return new ActionRequestInfo(putMappingRequest.getConcreteIndex().getName(), EXACT, IndicesRequestInfo.Scope.ANY);
                } else {
                    return new ActionRequestInfo(putMappingRequest, IndicesRequestInfo.Scope.ANY);
                }
            } else if (request instanceof FieldCapabilitiesIndexRequest) {
                // FieldCapabilitiesIndexRequest implements IndicesRequest. However,  this delegates to the original indices specified in the FieldCapabilitiesIndexRequest.
                // On the level of FieldCapabilitiesIndexRequest, it is sufficient to only consider the index stored in the index attribute. 

                return new ActionRequestInfo(((FieldCapabilitiesIndexRequest) request).index(), EXACT, IndicesRequestInfo.Scope.ANY);
            } else if (request instanceof ResizeRequest) {
                // Note: The targetIndex of ResizeRequest gets special treatment in PrivilegesEvaluator

                // ResizeRequest returns incorrect indicesOptions, so we hardcode them here
                ResizeRequest resizeRequest = (ResizeRequest) request;

                return new ActionRequestInfo(resizeRequest.getSourceIndex(), EXACT, IndicesRequestInfo.Scope.ANY).additional(
                        IndicesRequestInfo.AdditionalInfoRole.RESIZE_TARGET, ((ResizeRequest) request).getTargetIndexRequest(),
                        IndicesRequestInfo.Scope.ANY);
            } else if (request instanceof ResolveIndexAction.Request) {
                return new ActionRequestInfo((IndicesRequest) request, IndicesRequestInfo.Scope.ANY_DISTINCT);
            } else {
                // request instanceof IndicesRequest
                return new ActionRequestInfo((IndicesRequest) request, IndicesRequestInfo.Scope.ANY);
            }
        } else if (request instanceof CompositeIndicesRequest) {

            if (request instanceof BulkRequest) {
                return new ActionRequestInfo(((BulkRequest) request).requests(), IndicesRequestInfo.Scope.ANY);
            } else if (request instanceof MultiGetRequest) {
                return new ActionRequestInfo(((MultiGetRequest) request).getItems(), IndicesRequestInfo.Scope.ANY);
            } else if (request instanceof MultiSearchRequest) {
                return new ActionRequestInfo(((MultiSearchRequest) request).requests(), IndicesRequestInfo.Scope.ANY);
            } else if (request instanceof MultiTermVectorsRequest) {
                return new ActionRequestInfo(((MultiTermVectorsRequest) request).getRequests(), IndicesRequestInfo.Scope.ANY);
            } else if (request instanceof ReindexRequest) {
                return CLUSTER_REQUEST;
            } else {
                log.warn("Unknown action request: {} ", request.getClass().getName());
                return UNKNOWN;
            }

        } else if (request instanceof RestoreSnapshotRequest) {

            if (!isLocalNodeElectedMaster.getAsBoolean()) {
                return UNKNOWN;
            }

            RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;
            SnapshotInfo snapshotInfo = this.getSnapshotInfoFunction.apply(restoreRequest);

            if (snapshotInfo == null) {
                log.warn("snapshot repository '" + restoreRequest.repository() + "', snapshot '" + restoreRequest.snapshot() + "' not found");
                return UNKNOWN;
            } else {
                final List<String> requestedResolvedIndices = SnapshotUtils.filterIndices(snapshotInfo.indices(), restoreRequest.indices(),
                        restoreRequest.indicesOptions());
                final List<String> renamedTargetIndices = renamedIndices(restoreRequest, requestedResolvedIndices);

                if (log.isDebugEnabled()) {
                    log.debug("snapshot {} contains {}", snapshotInfo.snapshotId().getName(), renamedTargetIndices);
                }

                return new ActionRequestInfo(renamedTargetIndices, EXACT, IndicesRequestInfo.Scope.ANY);
            }
        } else if (request instanceof BaseNodesRequest) {
            return CLUSTER_REQUEST;
        } else if (request instanceof MainRequest) {
            return CLUSTER_REQUEST;
        } else if (request instanceof ClearScrollRequest) {
            return CLUSTER_REQUEST;
        } else if (request instanceof SearchScrollRequest) {
            return CLUSTER_REQUEST;
        } else {
            if (action.name().startsWith("indices:")) {
                log.warn("Unknown action request: {}", request.getClass().getName());
            } else {
                log.debug("Unknown action request: {}", request.getClass().getName());
            }
            return UNKNOWN;
        }
    }

    public PrivilegesEvaluationResult reduceIndices(Action action, Object request, ImmutableSet<String> keepIndices,
            ImmutableMap<ActionRequestIntrospector.IndicesRequestInfo.AdditionalInfoRole, ImmutableSet<String>> additionalKeepIndices,
            ActionRequestInfo actionRequestInfo) throws PrivilegesEvaluationException {

        if (request instanceof AnalyzeAction.Request) {
            AnalyzeAction.Request analyzeRequest = (AnalyzeAction.Request) request;

            if (analyzeRequest.index() == null) {
                // This actually does not refer to any index. Let the request pass, as have at least some privileges for the action
                return PrivilegesEvaluationResult.OK;
            } else if (keepIndices.contains(analyzeRequest.index())) {
                return PrivilegesEvaluationResult.OK;
            } else {
                return PrivilegesEvaluationResult.INSUFFICIENT;
            }
        } else if (request instanceof IndicesRequest.Replaceable) {
            IndicesRequest.Replaceable replaceableIndicesRequest = (IndicesRequest.Replaceable) request;
            actionRequestInfo = ensureActionRequestInfo(action, replaceableIndicesRequest, actionRequestInfo);

            if (request instanceof AliasesRequest) {
                // AliasesRequest is a sub interface of IndicesRequest.Replaceable
                AliasesRequest aliasesRequest = (AliasesRequest) request;

                if (keepIndices != null) {
                    ImmutableSet<String> newIndices = ImmutableSet.of(keepIndices).with(actionRequestInfo.getResolvedIndices().getRemoteIndices());

                    if (!keepIndices.isEmpty()) {
                        aliasesRequest.indices(toArray(newIndices));
                    } else {
                        return PrivilegesEvaluationResult.EMPTY;
                    }
                }

                ImmutableSet<String> keepAliases = additionalKeepIndices.get(ActionRequestIntrospector.IndicesRequestInfo.AdditionalInfoRole.ALIASES);
                if (keepAliases != null) {
                    if (!keepAliases.isEmpty()) {
                        aliasesRequest.replaceAliases(toArray(keepAliases));
                    } else {
                        return PrivilegesEvaluationResult.EMPTY;
                    }
                }

                validateIndexReduction(action, replaceableIndicesRequest, keepIndices);
                return PrivilegesEvaluationResult.OK;
            } else {
                // request instanceof IndicesRequest.Replaceable

                ResolvedIndices resolvedIndices = actionRequestInfo.getResolvedIndices();
                ImmutableSet<String> actualIndices = resolvedIndices.getLocal().getUnion().map(Meta.IndexLikeObject::name);

                if (keepIndices.containsAll(actualIndices)) {
                    return PrivilegesEvaluationResult.OK;
                }

                // TODO check if this is really necessary here or checked before
                if (!replaceableIndicesRequest.indicesOptions().ignoreUnavailable() && !containsWildcard(replaceableIndicesRequest)) {
                    return PrivilegesEvaluationResult.INSUFFICIENT;
                }

                ImmutableSet<String> newIndices = ImmutableSet.of(keepIndices).with(resolvedIndices.getRemoteIndices());

                if (log.isTraceEnabled()) {
                    log.trace("reduceIndicesForIgnoreUnavailable: keep: {}; actual: {}; newIndices: {}; remote: {}", keepIndices, actualIndices,
                            newIndices, resolvedIndices.getRemoteIndices());
                }

                if (newIndices.size() > 0) {
                    replaceableIndicesRequest.indices(toArray(newIndices));
                    validateIndexReduction(action, replaceableIndicesRequest, keepIndices);
                    return PrivilegesEvaluationResult.OK;
                } else {
                    return PrivilegesEvaluationResult.EMPTY;
                }
            }
        }

        return PrivilegesEvaluationResult.INSUFFICIENT;
    }

    private void validateIndexReduction(Action action, Object request, Set<String> keepIndices) throws PrivilegesEvaluationException {
        ActionRequestInfo newInfo = getActionRequestInfo(action, request);

        if (log.isDebugEnabled()) {
            log.debug("Reduced request to:\n{}\n{}", request, newInfo);
        }

        // TODO optimize and check
        if (!keepIndices.containsAll(newInfo.getMainResolvedIndices().getLocal().getUnion().map(Meta.IndexLikeObject::name))) {
            throw new PrivilegesEvaluationException("Indices were not properly reduced: " + request + "; new resolved:"
                    + newInfo.getMainResolvedIndices() + "; keep: " + keepIndices);
        }
    }

    public boolean forceEmptyResult(Action action, Object request) throws PrivilegesEvaluationException {
        if (request instanceof IndicesRequest.Replaceable) {
            IndicesRequest.Replaceable replaceableIndicesRequest = (IndicesRequest.Replaceable) request;

            if (replaceableIndicesRequest.indicesOptions().expandWildcardsOpen()
                    || replaceableIndicesRequest.indicesOptions().expandWildcardsClosed()) {
                replaceableIndicesRequest.indices(new String[] { ".force_no_index*", "-*" });
            } else {
                replaceableIndicesRequest.indices(new String[0]);
            }

            if (request instanceof GetAliasesRequest) {
                ((GetAliasesRequest) request).aliases(new String[] { ".force_no_alias*", "-*" });
            } else if (request instanceof AliasesRequest) {
                // AliasesRequest is a sub-interface of IndicesRequest.Replaceable
                ((AliasesRequest) request).replaceAliases();
            }

            validateIndexReduction(action, replaceableIndicesRequest, Collections.emptySet());

            return true;
        } else {
            return false;
        }
    }

    public boolean replaceIndices(Action action, Object request, Function<ResolvedIndices, List<String>> replacementFunction,
            ActionRequestInfo actionRequestInfo) {
        if (request instanceof IndicesRequest) {
            if (request instanceof IndicesRequest.Replaceable) {
                IndicesRequest.Replaceable replaceableIndicesRequest = (IndicesRequest.Replaceable) request;

                String[] indices = applyReplacementFunction(action, replaceableIndicesRequest, replacementFunction, actionRequestInfo);

                if (indices.length > 0) {
                    replaceableIndicesRequest.indices(indices);
                    return true;
                } else {
                    return false;
                }
            } else if (request instanceof SingleShardRequest) {
                SingleShardRequest<?> singleShardRequest = (SingleShardRequest<?>) request;

                String[] indices = applyReplacementFunction(action, singleShardRequest, replacementFunction, actionRequestInfo);

                if (indices.length == 1) {
                    singleShardRequest.index(indices[0]);
                    return true;
                } else {
                    return false;
                }
            }
        } else if (request instanceof CompositeIndicesRequest) {
            if (request instanceof MultiSearchRequest) {
                for (SearchRequest searchRequest : ((MultiSearchRequest) request).requests()) {
                    if (!replaceIndices(action, searchRequest, replacementFunction, actionRequestInfo)) {
                        return false;
                    }
                }

                return true;
            } else if (request instanceof MultiTermVectorsRequest) {
                for (TermVectorsRequest termVectorsRequest : ((MultiTermVectorsRequest) request).getRequests()) {
                    if (!replaceIndices(action, termVectorsRequest, replacementFunction, actionRequestInfo)) {
                        return false;
                    }
                }

                return true;
            }
        }

        return false;
    }

    private ActionRequestInfo ensureActionRequestInfo(Action action, IndicesRequest indicesRequest, ActionRequestInfo actionRequestInfo) {
        if (actionRequestInfo != null && actionRequestInfo.isFor(indicesRequest)) {
            return actionRequestInfo;
        } else {
            return new ActionRequestInfo(indicesRequest,
                    action.isDataStreamPrivilege() ? IndicesRequestInfo.Scope.DATA_STREAM : IndicesRequestInfo.Scope.ANY);
        }
    }

    private String[] applyReplacementFunction(Action action, IndicesRequest indicesRequest,
            Function<ResolvedIndices, List<String>> replacementFunction, ActionRequestInfo actionRequestInfo) {
        actionRequestInfo = ensureActionRequestInfo(action, indicesRequest, actionRequestInfo);
        List<String> replacedLocalIndices = new ArrayList<>(replacementFunction.apply(actionRequestInfo.getResolvedIndices()));
        replacedLocalIndices.addAll(actionRequestInfo.getResolvedIndices().getRemoteIndices());
        return replacedLocalIndices.toArray(new String[replacedLocalIndices.size()]);
    }

    private List<String> renamedIndices(final RestoreSnapshotRequest request, final List<String> filteredIndices) {
        try {
            final List<String> renamedIndices = new ArrayList<>();
            for (final String index : filteredIndices) {
                String renamedIndex = index;
                if (request.renameReplacement() != null && request.renamePattern() != null) {
                    renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
                }
                renamedIndices.add(renamedIndex);
            }
            return renamedIndices;
        } catch (PatternSyntaxException e) {
            log.error("Unable to parse the regular expression denoted in 'rename_pattern'. Please correct the pattern and try again.", e);
            throw e;
        }
    }

    private String[] toArray(ImmutableSet<String> set) {
        return set.toArray(new String[set.size()]);
    }

    public class ActionRequestInfo {

        private final boolean unknown;
        private final boolean indexRequest;

        private final ImmutableSet<IndicesRequestInfo> indices;
        private Object sourceRequest;
        private boolean resolvedIndicesInitialized = false;
        private ResolvedIndices mainResolvedIndices;
        private ResolvedIndices allResolvedIndices;
        private ImmutableMap<IndicesRequestInfo.AdditionalInfoRole, ResolvedIndices> additionalResolvedIndices;

        private Boolean containsWildcards;

        ActionRequestInfo(boolean unknown, boolean indexRequest) {
            this.unknown = unknown;
            this.indexRequest = indexRequest;
            this.indices = null;
        }

        ActionRequestInfo(ImmutableSet<IndicesRequestInfo> indices) {
            this.unknown = false;
            this.indexRequest = true;
            this.indices = indices;
        }

        ActionRequestInfo(boolean unknown, boolean indexRequest, ImmutableSet<IndicesRequestInfo> indices) {
            this.unknown = unknown;
            this.indexRequest = indexRequest;
            this.indices = indices;
        }

        ActionRequestInfo(IndicesRequest indices, IndicesRequestInfo.Scope scope) {
            this(ImmutableSet.of(new IndicesRequestInfo(null, indices, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
            this.sourceRequest = indices;
        }

        ActionRequestInfo(Collection<? extends IndicesRequest> indices, IndicesRequestInfo.Scope scope) {
            this(from(indices, scope));
        }

        ActionRequestInfo(String index, IndicesOptions indicesOptions, IndicesRequestInfo.Scope scope) {
            this(ImmutableSet
                    .of(new IndicesRequestInfo(null, index, indicesOptions, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo(String[] indices, IndicesOptions indicesOptions, IndicesRequestInfo.Scope scope) {
            this(ImmutableList.ofArray(indices), indicesOptions, scope);
        }

        ActionRequestInfo(List<String> index, IndicesOptions indicesOptions, IndicesRequestInfo.Scope scope) {
            this(ImmutableSet
                    .of(new IndicesRequestInfo(null, index, indicesOptions, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo with(IndicesRequest indices, IndicesRequestInfo.Scope scope) {
            return new ActionRequestInfo(unknown, indexRequest,
                    this.indices.with(new IndicesRequestInfo(null, indices, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo with(String[] indices, IndicesOptions indicesOptions, IndicesRequestInfo.Scope scope) {
            return new ActionRequestInfo(unknown, indexRequest, this.indices.with(new IndicesRequestInfo(null, ImmutableList.ofArray(indices),
                    indicesOptions, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo additional(IndicesRequestInfo.AdditionalInfoRole role, IndicesRequest indices, IndicesRequestInfo.Scope scope) {
            return new ActionRequestInfo(unknown, indexRequest,
                    this.indices.with(new IndicesRequestInfo(role, indices, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo additional(IndicesRequestInfo.AdditionalInfoRole role, Collection<? extends IndicesRequest> requests,
                IndicesRequestInfo.Scope scope) {
            Meta meta = metaDataSupplier.get();
            SystemIndexAccess systemIndexAccess = systemIndexAccessSupplier.get();
            return new ActionRequestInfo(unknown, indexRequest, this.indices
                    .with(requests.stream().map(r -> new IndicesRequestInfo(role, r, scope, systemIndexAccess, meta)).collect(Collectors.toList())));
        }

        ActionRequestInfo additional(IndicesRequestInfo.AdditionalInfoRole role, ImmutableList<String> indices, IndicesOptions indicesOptions,
                IndicesRequestInfo.Scope scope) {
            return new ActionRequestInfo(unknown, indexRequest, this.indices
                    .with(new IndicesRequestInfo(role, indices, indicesOptions, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo additional(IndicesRequestInfo.AdditionalInfoRole role, String[] indices, IndicesOptions indicesOptions,
                IndicesRequestInfo.Scope scope) {
            return this.additional(role, ImmutableList.ofArray(indices), indicesOptions, scope);
        }

        public boolean isUnknown() {
            return unknown;
        }

        public boolean isIndexRequest() {
            return indexRequest;
        }

        public boolean containsWildcards() {
            if (containsWildcards == null) {
                boolean result = this.indices != null ? this.indices.stream().anyMatch((i) -> i.containsWildcards()) : false;
                this.containsWildcards = result;
                return result;
            } else {
                return containsWildcards;
            }
        }

        /*
        public ActionRequestInfo reducedIndices(ImmutableSet<String> newLocalResolvedIndices) {
            if (!resolvedIndicesInitialized) {
                initResolvedIndices();
                resolvedIndicesInitialized = true;
            }
        
            return new ActionRequestInfo(unknown, indexRequest, indices, resolvedIndices.localIndices(newLocalResolvedIndices),
                    additionalResolvedIndices, newLocalResolvedIndices);
        }
        */

        public ResolvedIndices getResolvedIndices() {
            if (!resolvedIndicesInitialized) {
                initResolvedIndices();
                resolvedIndicesInitialized = true;
            }

            return allResolvedIndices;
        }

        public ResolvedIndices getMainResolvedIndices() {
            if (!resolvedIndicesInitialized) {
                initResolvedIndices();
                resolvedIndicesInitialized = true;
            }

            return mainResolvedIndices;
        }

        public ImmutableMap<IndicesRequestInfo.AdditionalInfoRole, ResolvedIndices> getAdditionalResolvedIndices() {
            if (!resolvedIndicesInitialized) {
                initResolvedIndices();
                resolvedIndicesInitialized = true;
            }

            return additionalResolvedIndices;
        }

        public boolean ignoreUnavailable() {
            if (indices == null || indices.size() == 0) {
                return false;
            }

            for (IndicesRequestInfo index : indices) {
                if (!index.indicesOptions().ignoreUnavailable()) {
                    return false;
                }
            }

            return true;
        }

        private void initResolvedIndices() {
            if (unknown) {
                allResolvedIndices = LOCAL_ALL;
                mainResolvedIndices = LOCAL_ALL;
                additionalResolvedIndices = ImmutableMap.empty();
                return;
            }

            if (!indexRequest) {
                allResolvedIndices = null;
                mainResolvedIndices = null;
                additionalResolvedIndices = ImmutableMap.empty();
                return;
            }

            int numberOfEntries = indices.size();

            if (numberOfEntries == 0) {
                allResolvedIndices = LOCAL_ALL;
                mainResolvedIndices = LOCAL_ALL;
                additionalResolvedIndices = ImmutableMap.empty();
            } else if (numberOfEntries == 1 && indices.only().role == null) {
                mainResolvedIndices = allResolvedIndices = indices.only().resolveIndices();
                additionalResolvedIndices = ImmutableMap.empty();
            } else {
                ResolvedIndices mainResolvedIndices = null;
                ImmutableMap<IndicesRequestInfo.AdditionalInfoRole, ResolvedIndices> additionalResolvedIndicesMap = ImmutableMap.empty();

                for (IndicesRequestInfo info : indices) {
                    ResolvedIndices singleResolved = info.resolveIndices();

                    if (info.role == null) {
                        mainResolvedIndices = singleResolved.with(mainResolvedIndices);
                    } else {
                        additionalResolvedIndicesMap = additionalResolvedIndicesMap.withComputed(info.role,
                                (additionalResolvedIndices) -> singleResolved.with(additionalResolvedIndices));
                    }
                }

                this.mainResolvedIndices = mainResolvedIndices;
                this.additionalResolvedIndices = additionalResolvedIndicesMap;

                if (additionalResolvedIndicesMap.isEmpty()) {
                    this.allResolvedIndices = mainResolvedIndices;
                } else {
                    this.allResolvedIndices = mainResolvedIndices.with(additionalResolvedIndicesMap.values());
                }
            }

        }

        public boolean isFor(Object sourceRequest) {
            return this.sourceRequest == sourceRequest;
        }

        @Override
        public String toString() {
            if (unknown) {
                return "UNKNOWN";
            } else if (!indexRequest) {
                return "CLUSTER_REQUEST";
            } else {
                return "main: " + getMainResolvedIndices() + "; additional: " + getAdditionalResolvedIndices() + "; source: " + this.indices;
            }
        }

        public ImmutableSet<IndicesRequestInfo> getUnresolved() {
            return indices;
        }

    }

    public static class IndicesRequestInfo {

        private final ImmutableList<String> indices;
        private final String[] indicesArray;
        private final IndicesOptions indicesOptions;
        private final boolean allowsRemoteIndices;
        private final boolean includeDataStreams;
        private final IndicesRequestInfo.AdditionalInfoRole role;
        private final boolean expandWildcards;
        private final boolean isAll;
        private final boolean containsWildcards;
        private final boolean containsNegation;
        private final boolean writeRequest;
        private final boolean createIndexRequest;
        private final SystemIndexAccess systemIndexAccess;
        private final Meta indexMetadata;
        private final ImmutableSet<String> remoteIndices;
        private final ImmutableList<String> localIndices;
        private final Scope scope;

        IndicesRequestInfo(IndicesRequestInfo.AdditionalInfoRole role, IndicesRequest indicesRequest, Scope scope,
                SystemIndexAccess systemIndexAccess, Meta indexMetadata) {
            this.indices = indicesRequest.indices() != null ? ImmutableList.ofArray(indicesRequest.indices()) : ImmutableList.empty();
            this.indicesArray = indicesRequest.indices();
            this.indicesOptions = indicesRequest.indicesOptions();
            this.allowsRemoteIndices = indicesRequest instanceof Replaceable ? ((Replaceable) indicesRequest).allowsRemoteIndices() : false;
            this.includeDataStreams = indicesRequest.includeDataStreams();
            this.role = role;
            this.expandWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsHidden()
                    || indicesOptions.expandWildcardsClosed();
            this.localIndices = this.indices.matching((i) -> !i.contains(":"));
            this.remoteIndices = ImmutableSet.of(this.indices.matching((i) -> i.contains(":")));
            this.isAll = this.expandWildcards && this.isAll(localIndices, remoteIndices, indicesRequest);
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(this.indices) : false;
            this.containsNegation = this.containsWildcards && !this.isAll && containsNegation(this.indices);
            this.writeRequest = indicesRequest instanceof DocWriteRequest;
            this.createIndexRequest = indicesRequest instanceof IndexRequest
                    || indicesRequest instanceof org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
            this.indexMetadata = indexMetadata;
            this.scope = scope;
            this.systemIndexAccess = systemIndexAccess;

        }

        IndicesRequestInfo(IndicesRequestInfo.AdditionalInfoRole role, String index, IndicesOptions indicesOptions, Scope scope,
                SystemIndexAccess systemIndexAccess, Meta indexMetadata) {
            this.indices = ImmutableList.of(index);
            this.indicesArray = new String[] { index };
            this.indicesOptions = indicesOptions;
            this.allowsRemoteIndices = true;
            this.includeDataStreams = true;
            this.role = role;
            this.expandWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsHidden()
                    || indicesOptions.expandWildcardsClosed();
            this.localIndices = this.indices.matching((i) -> !i.contains(":"));
            this.remoteIndices = ImmutableSet.of(this.indices.matching((i) -> i.contains(":")));
            this.isAll = this.expandWildcards && this.isAll(localIndices, remoteIndices, null);
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(index) : false;
            this.containsNegation = false;
            this.writeRequest = false;
            this.createIndexRequest = false;
            this.indexMetadata = indexMetadata;

            this.scope = scope;
            this.systemIndexAccess = systemIndexAccess;
        }

        IndicesRequestInfo(IndicesRequestInfo.AdditionalInfoRole role, List<String> indices, IndicesOptions indicesOptions, Scope scope,
                SystemIndexAccess systemIndexAccess, Meta indexMetadata) {
            this.indices = ImmutableList.of(indices);
            this.indicesArray = indices.toArray(new String[indices.size()]);
            this.indicesOptions = indicesOptions;
            this.allowsRemoteIndices = true;
            this.includeDataStreams = true;
            this.role = role;
            this.expandWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsHidden()
                    || indicesOptions.expandWildcardsClosed();
            this.localIndices = this.indices.matching((i) -> !i.contains(":"));
            this.remoteIndices = ImmutableSet.of(this.indices.matching((i) -> i.contains(":")));
            this.isAll = this.expandWildcards && this.isAll(localIndices, remoteIndices, null);
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(this.indices) : false;
            this.containsNegation = this.containsWildcards && !this.isAll && containsNegation(this.indices);
            this.writeRequest = false;
            this.createIndexRequest = false;
            this.indexMetadata = indexMetadata;

            this.scope = scope;
            this.systemIndexAccess = systemIndexAccess;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (allowsRemoteIndices ? 1231 : 1237);
            result = prime * result + (includeDataStreams ? 1231 : 1237);
            result = prime * result + (writeRequest ? 1231 : 1237);
            result = prime * result + (createIndexRequest ? 1231 : 1237);
            result = prime * result + ((indices == null) ? 0 : indices.hashCode());
            result = prime * result + ((indicesOptions == null) ? 0 : indicesOptions.hashCode());
            result = prime * result + ((role == null) ? 0 : role.hashCode());
            result = prime * result + ((scope == null) ? 0 : scope.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof IndicesRequestInfo)) {
                return false;
            }
            IndicesRequestInfo other = (IndicesRequestInfo) obj;
            if (allowsRemoteIndices != other.allowsRemoteIndices) {
                return false;
            }
            if (includeDataStreams != other.includeDataStreams) {
                return false;
            }
            if (writeRequest != other.writeRequest) {
                return false;
            }
            if (createIndexRequest != other.createIndexRequest) {
                return false;
            }
            if (indices == null) {
                if (other.indices != null) {
                    return false;
                }
            } else if (!indices.equals(other.indices)) {
                return false;
            }
            if (indicesOptions == null) {
                if (other.indicesOptions != null) {
                    return false;
                }
            } else if (!indicesOptions.equals(other.indicesOptions)) {
                return false;
            }
            if (role == null) {
                if (other.role != null) {
                    return false;
                }
            } else if (!role.equals(other.role)) {
                return false;
            }

            if (!this.scope.equals(other.scope)) {
                return false;
            }

            return true;
        }

        public boolean isExpandWildcards() {
            return expandWildcards;
        }

        public boolean containsWildcards() {
            return containsWildcards;
        }

        public boolean isAll() {
            return this.isAll;
        }

        private boolean isAll(List<String> localIndices, ImmutableSet<String> remoteIndices, IndicesRequest indicesRequest) {
            if (localIndices.isEmpty() && !remoteIndices.isEmpty()) {
                return false;
            }

            if (indicesRequest instanceof AliasesRequest) {
                AliasesRequest aliasesRequest = (AliasesRequest) indicesRequest;

                if (aliasesRequest.aliases() != null && aliasesRequest.aliases().length != 0) {
                    ImmutableSet<String> aliases = ImmutableSet.ofArray(aliasesRequest.aliases());

                    if (!aliases.contains("*") && !aliases.contains("_all")) {
                        return false;
                    }
                }
            }

            return IndexNameExpressionResolver.isAllIndices(localIndices)
                    || (localIndices.size() == 1 && (localIndices.get(0) == null || localIndices.get(0).equals("*")));
        }

        ResolvedIndices resolveIndices() {
            if (isAll()) {
                return new ResolvedIndices(true, ResolvedIndices.Local.EMPTY, remoteIndices, ImmutableSet.of(this));
            }

            if (localIndices.isEmpty()) {
                return new ResolvedIndices(false, ResolvedIndices.Local.EMPTY, remoteIndices, ImmutableSet.empty());
            } else if (isExpandWildcards() && localIndices.size() == 1 && (localIndices.contains(Metadata.ALL) || localIndices.contains("*"))) {
                // In case of * wildcards, we defer resolution of indices. Chances are that we do not need to resolve the wildcard at all in this case.
                return new ResolvedIndices(true, ResolvedIndices.Local.EMPTY, remoteIndices, ImmutableSet.of(this));
            } else {
                return new ResolvedIndices(false, ResolvedIndices.Local.resolve(this, indexMetadata), remoteIndices, ImmutableSet.empty());
            }
        }

        private ImmutableSet<String> resolveDateMathExpressions() {
            ImmutableSet<String> result = ImmutableSet.empty();

            for (String index : localIndices) {
                result = result.with(DateMathExpressionResolver.resolveExpression(index));
            }

            return result;
        }

        @Override
        public String toString() {
            return "[indices=" + indices + ", indicesOptions=" + indicesOptions + ", allowsRemoteIndices=" + allowsRemoteIndices
                    + ", includeDataStreams=" + includeDataStreams + ", role=" + role + "]";
        }

        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public IndicesRequest.Replaceable asIndicesRequestWithoutRemoteIndices() {
            return new IndicesRequest.Replaceable() {

                @Override
                public String[] indices() {
                    if (remoteIndices.isEmpty()) {
                        return indicesArray;
                    } else {
                        return localIndices.toArray(new String[localIndices.size()]);
                    }
                }

                @Override
                public IndicesOptions indicesOptions() {
                    return indicesOptions;
                }

                @Override
                public boolean includeDataStreams() {
                    return includeDataStreams;
                }

                @Override
                public IndicesRequest indices(String... indices) {
                    return this;
                }

                @Override
                public boolean allowsRemoteIndices() {
                    return allowsRemoteIndices;
                }
            };
        }

        public static enum Scope {

            /**
             * Both indices, aliases and data streams are expected. If an alias containing certain indices is present, the contained indices are skipped, as these are implied by the alias.
             */
            ANY(true, true, true),

            /**
             * Only indices are expected
             */
            INDEX(true, false, false),

            /**
             * Only aliases are expected
             */
            ALIAS(false, true, false),

            /**
             * Only data streams are expected
             */
            DATA_STREAM(false, false, true),

            /**
             * Only indices and data streams are expected (the common denominator of both is that both can be members of indices)
             */
            INDICES_DATA_STREAMS(true, false, true),

            /**
             * Both indices, aliases and data streams are expected. In contrast to ANY, indices are also contained even if they are members of an alias which is also contained.
             */
            ANY_DISTINCT(true, true, true);

            private final boolean includeIndices;
            private final boolean includeAliases;
            private final boolean includeDataStreams;

            private Scope(boolean includeIndices, boolean includeAliases, boolean includeDataStreams) {
                this.includeIndices = includeIndices;
                this.includeAliases = includeAliases;
                this.includeDataStreams = includeDataStreams;
            }

            public boolean includeIndices() {
                return includeIndices;
            }

            public boolean includeAliases() {
                return includeAliases;
            }

            public boolean includeDataStreams() {
                return includeDataStreams;
            }
        }

        public static class AdditionalInfoRole {
            public static final AdditionalInfoRole ALIASES = new AdditionalInfoRole("aliases");
            public static final AdditionalInfoRole RESIZE_TARGET = new AdditionalInfoRole("resize_target",
                    ImmutableSet.ofArray("indices:admin/create"));
            public static final AdditionalInfoRole MANAGE_ALIASES = new AdditionalInfoRole("manage_aliases",
                    ImmutableSet.ofArray("indices:admin/aliases"));
            public static final AdditionalInfoRole DELETE_INDEX = new AdditionalInfoRole("delete_index",
                    ImmutableSet.ofArray("indices:admin/delete"));
            
            private final String id;
            private final ImmutableSet<String> requiredPrivileges;

            public AdditionalInfoRole(String id) {
                this.id = id;
                this.requiredPrivileges = null;
            }

            public AdditionalInfoRole(String id, ImmutableSet<String> requiredPrivileges) {
                this.id = id;
                this.requiredPrivileges = requiredPrivileges;
            }

            public ImmutableSet<Action> getRequiredPrivileges(ImmutableSet<Action> original, Actions actions) {
                if (this.requiredPrivileges == null) {
                    return original;
                } else {
                    return this.requiredPrivileges.map(a -> actions.get(a));
                }
            }

            @Override
            public int hashCode() {
                return id.hashCode();
            }

            @Override
            public boolean equals(Object other) {
                if (this == other) {
                    return true;
                }
                if (!(other instanceof AdditionalInfoRole)) {
                    return false;
                }
                return (((AdditionalInfoRole) other).id.equals(this.id));
            }

            @Override
            public String toString() {
                return id;
            }

        }
    }

    private final ActionRequestInfo UNKNOWN = new ActionRequestInfo(true, false);
    private final ActionRequestInfo CLUSTER_REQUEST = new ActionRequestInfo(false, false);
    private final static ResolvedIndices LOCAL_ALL = new ResolvedIndices(true, ResolvedIndices.Local.EMPTY, ImmutableSet.empty(),
            ImmutableSet.empty());
    private final static ResolvedIndices EMPTY = new ResolvedIndices(false, ResolvedIndices.Local.EMPTY, ImmutableSet.empty(), ImmutableSet.empty());

    public static class ResolvedIndices {

        private final boolean localAll;
        private ImmutableSet<IndicesRequestInfo> deferredRequests;
        private Local localShallow;
        protected final ImmutableSet<String> remoteIndices;

        ResolvedIndices(boolean localAll, Local localShallow, ImmutableSet<String> remoteIndices, ImmutableSet<IndicesRequestInfo> deferredRequests) {
            this.localAll = localAll;
            this.localShallow = localShallow;
            this.remoteIndices = remoteIndices;
            this.deferredRequests = deferredRequests;
        }

        public boolean isLocalAll() {
            return localAll;
        }

        public boolean isLocalIndicesEmpty() {
            return !localAll && getLocal().isEmpty();
        }

        public boolean containsOnlyRemoteIndices() {
            return !this.localAll && this.deferredRequests.isEmpty() && this.localShallow.isEmpty() && !this.remoteIndices.isEmpty();
        }

        ResolvedIndices with(ResolvedIndices other) {
            if (other == null) {
                return this;
            }

            return new ResolvedIndices(this.localAll || other.localAll, this.localShallow.with(other.localShallow),
                    this.remoteIndices.with(other.remoteIndices), this.deferredRequests.with(other.deferredRequests));
        }

        ResolvedIndices with(Collection<ResolvedIndices> others) {
            if (others == null || others.isEmpty()) {
                return this;
            }

            ResolvedIndices result = this;

            for (ResolvedIndices other : others) {
                result = result.with(other);
            }

            return result;
        }

        private void resolveDeferredRequests() {
            Local localShallow = this.localShallow;

            for (IndicesRequestInfo info : this.deferredRequests) {
                localShallow = localShallow.with(Local.resolve(info, info.indexMetadata));
            }

            this.deferredRequests = ImmutableSet.empty();

            this.localShallow = localShallow;
        }

        public Local getLocal() {
            if (!deferredRequests.isEmpty()) {
                resolveDeferredRequests();
            }

            return localShallow;
        }

        public ImmutableSet<String> getRemoteIndices() {
            return remoteIndices;
        }

        public ImmutableSet<String> getLocalAndRemoteIndices() {
            return getLocal().getUnion().map(Meta.IndexLikeObject::name).with(getRemoteIndices());
        }

        public ImmutableSet<String> getLocalSubset(Set<String> superSet) {
            return getLocal().getUnion().map(Meta.IndexLikeObject::name).intersection(superSet).with(remoteIndices);
        }

        public String[] getLocalSubsetAsArray(Set<String> superSet) {
            ImmutableSet<String> result = getLocalSubset(superSet);

            return result.toArray(new String[result.size()]);
        }

        @Override
        public String toString() {
            if (localAll) {
                if (remoteIndices.isEmpty() && !deferredRequests.isEmpty()) {
                    return "local: _all";
                }

                StringBuilder result = new StringBuilder("local: _all");

                if (deferredRequests.isEmpty()) {
                    result.append(" [").append(localShallow.size()).append("]");
                }

                if (!remoteIndices.isEmpty()) {
                    result.append("; remote: ").append(remoteIndices.toShortString());
                }

                return result.toString();
            } else {
                return "local: " + (localShallow != null ? localShallow : "null") + "; remote: "
                        + (remoteIndices != null ? remoteIndices.toShortString() : "null");
            }
        }

        /**
         * Only for testing!
         */
        public static ResolvedIndices of(Meta indexMetadata, String... indices) {
            return new ResolvedIndices(false, Local.resolve(indexMetadata, indices), ImmutableSet.empty(), ImmutableSet.empty());
        }

        /**
         * Only for testing!
         */
        public ResolvedIndices localIndices(String... localIndices) {
            return new ResolvedIndices(false, new Local(ImmutableSet.ofArray(localIndices)), remoteIndices, ImmutableSet.empty());
        }

        /**
         * Only for testing!
         */
        public ResolvedIndices local(Local local) {
            return new ResolvedIndices(false, local, remoteIndices, ImmutableSet.empty());
        }

        public static ResolvedIndices empty() {
            return EMPTY;
        }

        public static class Local {
            private final ImmutableSet<Meta.Index> pureIndices;
            private final ImmutableSet<Meta.Alias> aliases;
            private final ImmutableSet<Meta.DataStream> dataStreams;
            private final ImmutableSet<Meta.NonExistent> nonExistingIndices;
            private final ImmutableSet<String> unionOfAliasesAndDataStreams;
            private final ImmutableSet<Meta.IndexLikeObject> union;
            private String asString;
            private ImmutableSet<String> deepUnion;
            private Boolean containsAliasOrDataStreamMembers;

            Local(ImmutableSet<Meta.Index> pureIndices, ImmutableSet<Meta.Alias> aliases, ImmutableSet<Meta.DataStream> dataStreams,
                    ImmutableSet<Meta.NonExistent> nonExistingIndices) {
                this.pureIndices = pureIndices;
                this.aliases = aliases;
                this.dataStreams = dataStreams;
                this.nonExistingIndices = nonExistingIndices;
                this.unionOfAliasesAndDataStreams = aliases.map(Meta.Alias::name).with(dataStreams.map(Meta.DataStream::name));
                this.union = ImmutableSet.<Meta.IndexLikeObject>of(pureIndices).with(aliases).with(dataStreams).with(this.nonExistingIndices);
            }

            /**
             * Only for testing! TODO remove
             */
            Local(ImmutableSet<String> localIndices) {
                this.pureIndices = ImmutableSet.empty();
                this.aliases = ImmutableSet.empty();
                this.dataStreams = ImmutableSet.empty();
                this.nonExistingIndices = localIndices.map(Meta.NonExistent::of);
                this.unionOfAliasesAndDataStreams = ImmutableSet.empty();
                this.union = ImmutableSet.<Meta.IndexLikeObject>of(this.nonExistingIndices);
            }

            private Local(ImmutableSet<Meta.Index> pureIndices, ImmutableSet<Meta.Alias> aliases, ImmutableSet<Meta.DataStream> dataStreams,
                    ImmutableSet<Meta.NonExistent> nonExistingIndices, ImmutableSet<String> unionOfAliasesAndDataStreams,
                    ImmutableSet<Meta.IndexLikeObject> union) {
                this.pureIndices = pureIndices;
                this.aliases = aliases;
                this.dataStreams = dataStreams;
                this.nonExistingIndices = nonExistingIndices;
                this.unionOfAliasesAndDataStreams = unionOfAliasesAndDataStreams;
                this.union = union;
            }

            boolean isEmpty() {
                return union.isEmpty();
            }

            Local with(Local other) {
                if (this.union.equals(other.union)) {
                    return this;
                }

                if (this.unionOfAliasesAndDataStreams.equals(other.unionOfAliasesAndDataStreams)) {
                    return new Local(this.pureIndices.with(other.pureIndices), this.aliases, this.dataStreams,
                            this.nonExistingIndices.with(other.nonExistingIndices), this.unionOfAliasesAndDataStreams, this.union.with(other.union));
                } else {
                    // Remove entries from pureIndices which are contained in the other object's aliases or data streams (and vice versa)
                    // This ensures the contract that pureIndices only contains indices which are not already indirectly contained in aliases or dataStreams

                    ImmutableSet.Builder<Meta.Index> mergedPureIndices = new ImmutableSet.Builder<>(
                            this.pureIndices.size() + other.pureIndices.size());

                    for (Meta.Index index : this.pureIndices) {
                        if (index.parentDataStreamName() != null && other.dataStreams.contains(index.parentDataStream())) {
                            continue;
                        }

                        if (other.aliases.containsAny(index.parentAliases())) {
                            continue;
                        }

                        mergedPureIndices.add(index);
                    }

                    for (Meta.Index index : other.pureIndices) {
                        if (index.parentDataStreamName() != null && this.dataStreams.contains(index.parentDataStream())) {
                            continue;
                        }

                        if (this.aliases.containsAny(index.parentAliases())) {
                            continue;
                        }

                        mergedPureIndices.add(index);
                    }

                    return new Local(mergedPureIndices.build(), this.aliases.with(other.aliases), this.dataStreams.with(other.dataStreams),
                            this.nonExistingIndices.with(other.nonExistingIndices));
                }
            }

            public int size() {
                return this.union.size();
            }

            public ImmutableSet<Meta.Index> getPureIndices() {
                return pureIndices;
            }

            public ImmutableSet<Meta.Alias> getAliases() {
                return aliases;
            }

            public ImmutableSet<Meta.DataStream> getDataStreams() {
                return dataStreams;
            }

            public ImmutableSet<Meta.IndexCollection> getAliasesAndDataStreams() {
                return ImmutableSet.<Meta.IndexCollection>of(aliases).with(dataStreams);
            }

            public ImmutableSet<Meta.NonExistent> getNonExistingIndices() {
                return nonExistingIndices;
            }

            public ImmutableSet<Meta.IndexLikeObject> getUnion() {
                return union;
            }

            public ImmutableSet<String> getDeepUnion() {
                ImmutableSet<String> result = this.deepUnion;

                if (result == null) {
                    result = this.resolveDeep(aliases).with(resolveDeep(dataStreams)).with(this.union.map(Meta.IndexLikeObject::name));
                    this.deepUnion = result;
                }

                return result;
            }

            /**
             * Resolves the named alias or dataStream to its contained concrete indices. If the named alias or dataStream does not exist, or if it is an index, an empty set is returned.
             */
            /*
            public ImmutableSet<String> resolveDeep(Meta.IndexCollection aliasOrDataStream) {
                Meta.Alias alias = this.aliases.get(aliasOrDataStream);
                if (alias != null) {
                    return alias.resolveDeepToNames();
                }
            
                Meta.DataStream dataStream = this.dataStreams.get(aliasOrDataStream);
                if (dataStream != null) {
                    return dataStream.resolveDeepToNames();
                }
            
                return ImmutableSet.empty();
            }*/

            public ImmutableSet<String> resolveDeep(ImmutableSet<? extends Meta.IndexCollection> aliasesAndDataStreams) {
                // TODO think about moving to meta
                if (aliasesAndDataStreams.size() == 0) {
                    return ImmutableSet.empty();
                }

                if (aliasesAndDataStreams.size() == 1) {
                    return aliasesAndDataStreams.only().resolveDeepToNames();
                }

                ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>(aliasesAndDataStreams.size() * 20);

                for (Meta.IndexCollection object : aliasesAndDataStreams) {
                    result.addAll(object.resolveDeepToNames());
                }

                return result.build();
            }

            public boolean hasAliasOrDataStreamMembers() {
                Boolean result = this.containsAliasOrDataStreamMembers;

                if (result == null) {
                    for (Meta.Index pureIndex : this.pureIndices) {
                        if (!pureIndex.parentAliasNames().isEmpty() || pureIndex.parentDataStreamName() != null) {
                            result = true;
                            break;
                        }
                    }

                    for (Meta.DataStream dataStream : this.dataStreams) {
                        if (!dataStream.parentAliasNames().isEmpty()) {
                            result = true;
                            break;
                        }
                    }

                    if (result == null) {
                        result = false;
                    }

                    this.containsAliasOrDataStreamMembers = result;
                }

                return result;
            }

            boolean hasAliasesOrDataStreams() {
                return !aliases.isEmpty() || !dataStreams.isEmpty();
            }

            public boolean hasAliasesOnly() {
                return !aliases.isEmpty() && this.dataStreams.isEmpty() && this.pureIndices.isEmpty();
            }

            @Override
            public String toString() {
                String result = this.asString;

                if (result != null) {
                    return result;
                }

                StringBuilder resultBuilder = new StringBuilder("{");

                if (!this.pureIndices.isEmpty()) {
                    resultBuilder.append("indices: ").append(this.pureIndices);
                }

                if (!this.aliases.isEmpty()) {
                    if (!this.pureIndices.isEmpty()) {
                        resultBuilder.append("; ");
                    }

                    resultBuilder.append("aliases: ").append(this.aliases);
                }

                if (!this.dataStreams.isEmpty()) {
                    if (!this.pureIndices.isEmpty() || !this.aliases.isEmpty()) {
                        resultBuilder.append("; ");
                    }

                    resultBuilder.append("dataStreams: ").append(this.dataStreams);
                }

                if (!this.nonExistingIndices.isEmpty()) {
                    if (!this.pureIndices.isEmpty() || !this.aliases.isEmpty() || !this.dataStreams.isEmpty()) {
                        resultBuilder.append("; ");
                    }

                    resultBuilder.append("nonExistingIndices: ").append(this.nonExistingIndices);
                }

                resultBuilder.append("}");

                result = resultBuilder.toString();
                this.asString = result;

                return result;
            }

            static Local resolve(IndicesRequestInfo request, Meta indexMetadata) {
                try {
                    if (request.scope == IndicesRequestInfo.Scope.DATA_STREAM) {
                        if (request.isAll) {
                            return new Local(ImmutableSet.empty(), ImmutableSet.empty(), indexMetadata.dataStreams(), ImmutableSet.empty());
                        } else if (request.expandWildcards) {
                            return resolveWithPatterns(request, indexMetadata);
                        } else if (request.createIndexRequest) {
                            return new Local(ImmutableSet.empty(), ImmutableSet.empty(),
                                    ImmutableSet.of(request.resolveDateMathExpressions().map(Meta.DataStream::nonExistent)), ImmutableSet.empty());
                        } else {
                            return resolveDataStreamsWithoutPatterns(request, indexMetadata);
                        }
                    } else if (request.scope == IndicesRequestInfo.Scope.ALIAS) {
                        if (request.isAll) {
                            return new Local(ImmutableSet.empty(), indexMetadata.aliases(), ImmutableSet.empty(), ImmutableSet.empty());
                        } else if (request.expandWildcards) {
                            return resolveWithPatterns(request, indexMetadata);
                        } else if (request.createIndexRequest) {
                            return new Local(ImmutableSet.empty(), ImmutableSet.of(request.resolveDateMathExpressions().map(Meta.Alias::nonExistent)),
                                    ImmutableSet.empty(), ImmutableSet.empty());
                        } else {
                            return resolveAliasesWithoutPatterns(request, indexMetadata);
                        }
                    } else {
                        // other scope

                        if (request.isAll) {
                            return resolveIsAll(request, indexMetadata);
                        } else if (request.expandWildcards) {
                            return resolveWithPatterns(request, indexMetadata);
                            //} else if (request.writeRequest) { TODO
                            //    return request.resolveWriteIndex();
                            //   } else if (request.createIndexRequest) {
                            //     return new Local(ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty(),
                            //             request.resolveDateMathExpressions().map(Meta.NonExistent::of));
                        } else {
                            // No wildcards, no write request, no create index request
                            return resolveWithoutPatterns(request, indexMetadata);
                        }
                    }
                } catch (RuntimeException e) {
                    log.error("Error while resolving " + request, e);
                    throw e;
                }
            }

            /**
             * Only for testing!
             */
            public static Local resolve(Meta indexMetadata, String... indices) {
                return resolveWithoutPatterns(new IndicesRequestInfo(null, ImmutableList.ofArray(indices), EXACT, IndicesRequestInfo.Scope.ANY,
                        SystemIndexAccess.DISALLOWED, indexMetadata), indexMetadata);
            }

            static Local resolveWithPatterns(IndicesRequestInfo request, Meta indexMetadata) {
                Metadata metadata = indexMetadata.esMetadata();
                IndicesRequestInfo.Scope scope = request.scope;
                boolean includeDataStreams = request.includeDataStreams && scope.includeDataStreams;
                boolean includeIndices = scope.includeIndices;
                boolean includeAliases = (scope.includeAliases && !request.indicesOptions.ignoreAliases()) || scope == IndicesRequestInfo.Scope.ALIAS; // An explict ALIAS scope overrides ignoreAliases

                SortedMap<String, IndexAbstraction> indicesLookup = metadata.getIndicesLookup();

                ImmutableSet.Builder<Meta.Index> indices = new ImmutableSet.Builder<>();
                ImmutableSet.Builder<Meta.NonExistent> nonExistingIndices = new ImmutableSet.Builder<>();
                ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();
                ImmutableSet.Builder<Meta.DataStream> dataStreams = new ImmutableSet.Builder<>();
                Set<String> excludeNames = new HashSet<>();

                for (int i = request.localIndices.size() - 1; i >= 0; i--) {
                    String index = request.localIndices.get(i);

                    if (index.startsWith("-")) {
                        index = index.substring(1);
                        index = DateMathExpressionResolver.resolveExpression(index);

                        if (index.contains("*")) {
                            Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index,
                                    request.indicesOptions, includeDataStreams);

                            excludeNames.addAll(matchedAbstractions.keySet());
                        } else {
                            excludeNames.add(index);
                        }
                    } else {
                        index = DateMathExpressionResolver.resolveExpression(index);

                        if (index.contains("*")) {

                            Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index,
                                    request.indicesOptions, includeDataStreams);

                            for (Map.Entry<String, IndexAbstraction> entry : matchedAbstractions.entrySet()) {
                                if (excludeNames.contains(entry.getKey())) {
                                    continue;
                                }

                                IndexAbstraction indexAbstraction = entry.getValue();

                                if (indexAbstraction instanceof DataStream) {
                                    if (includeDataStreams) {
                                        dataStreams.add((Meta.DataStream) indexMetadata.getIndexOrLike(entry.getKey()));
                                    }
                                } else if (indexAbstraction instanceof Alias) {
                                    if (includeAliases) {
                                        aliases.add((Meta.Alias) indexMetadata.getIndexOrLike(entry.getKey()));
                                    }
                                } else {
                                    if (includeIndices) {
                                        Meta.Index indexMeta = (Meta.Index) indexMetadata.getIndexOrLike(entry.getKey());

                                        if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(entry.getKey())) {
                                            indices.add(indexMeta);
                                        }
                                    }
                                }
                            }
                        } else {
                            if (excludeNames.contains(index)) {
                                continue;
                            }

                            Meta.IndexLikeObject indexLikeObject = indexMetadata.getIndexOrLike(index);

                            if (indexLikeObject == null) {
                                if (scope == IndicesRequestInfo.Scope.DATA_STREAM) {
                                    dataStreams.add(Meta.DataStream.nonExistent(index));
                                } else if (scope == IndicesRequestInfo.Scope.ALIAS) {
                                    aliases.add(Meta.Alias.nonExistent(index));
                                } else {
                                    nonExistingIndices.add(Meta.NonExistent.of(index));
                                }
                            } else if (indexLikeObject instanceof Meta.Alias) {
                                if (includeAliases) {
                                    aliases.add((Meta.Alias) indexLikeObject);
                                }
                            } else if (indexLikeObject instanceof Meta.DataStream) {
                                if (includeDataStreams) {
                                    dataStreams.add((Meta.DataStream) indexLikeObject);
                                }
                            } else {
                                if (includeIndices) {
                                    Meta.Index indexMeta = (Meta.Index) indexLikeObject;

                                    if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(indexMeta)) {
                                        indices.add((Meta.Index) indexLikeObject);
                                    }
                                }
                            }
                        }
                    }
                }

                ImmutableSet<Meta.Index> pureIndices = indices.build();

                if ((aliases.size() != 0 || dataStreams.size() != 0) && scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                    // If there are aliases or dataStreams, remove the indices that are part of these aliases or dataStreams

                    pureIndices = pureIndices.matching((index) -> {
                        if (index.parentDataStreamName() != null && dataStreams.contains(index.parentDataStream())) {
                            return false;
                        } else if (aliases.containsAny(index.parentAliases())) {
                            return false;
                        } else {
                            return true;
                        }
                    });
                }

                return new Local(pureIndices, aliases.build(), dataStreams.build(), nonExistingIndices.build());
            }

            static Local resolveIsAll(IndicesRequestInfo request, Meta indexMetadata) {
                IndicesRequestInfo.Scope scope = request.scope;

                boolean includeHidden = request.indicesOptions.expandWildcardsHidden();
                boolean excludeSystem = request.systemIndexAccess.isNotAllowed();
                boolean includeDataStreams = request.includeDataStreams && scope.includeDataStreams;
                boolean includeIndices = scope.includeIndices;
                boolean includeAliases = (scope.includeAliases && !request.indicesOptions.ignoreAliases()) || scope == IndicesRequestInfo.Scope.ALIAS;

                ImmutableSet<Meta.Index> pureIndices;
                ImmutableSet<Meta.Alias> aliases;
                ImmutableSet<Meta.DataStream> dataStreams;
                ImmutableSet<Meta.NonExistent> nonExistingIndices = ImmutableSet.empty();

                if (includeDataStreams && includeAliases && scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                    if (!includeIndices) {
                        pureIndices = ImmutableSet.empty();
                    } else if (!includeHidden) {
                        pureIndices = indexMetadata.nonHiddenIndicesWithoutParents();
                    } else if (excludeSystem) {
                        pureIndices = indexMetadata.nonSystemIndicesWithoutParents();
                    } else {
                        pureIndices = indexMetadata.indicesWithoutParents();
                    }

                    aliases = includeHidden ? indexMetadata.aliases() : indexMetadata.aliases().matching(e -> !e.isHidden());
                    dataStreams = indexMetadata.dataStreams();
                } else {
                    if (!includeIndices) {
                        pureIndices = ImmutableSet.empty();
                    } else if (!includeHidden) {
                        pureIndices = indexMetadata.nonHiddenIndices();
                    } else if (excludeSystem) {
                        pureIndices = indexMetadata.nonSystemIndices();
                    } else {
                        pureIndices = indexMetadata.indices();
                    }

                    aliases = includeAliases ? (includeHidden ? indexMetadata.aliases() : indexMetadata.aliases().matching(e -> !e.isHidden()))
                            : ImmutableSet.empty();
                    dataStreams = includeDataStreams ? indexMetadata.dataStreams() : ImmutableSet.empty();

                    if (scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                        if (!dataStreams.isEmpty()) {
                            pureIndices = pureIndices.matching(e -> e.parentDataStreamName() == null);
                        }

                        if (!aliases.isEmpty()) {
                            pureIndices = pureIndices.matching(e -> e.parentAliasNames().isEmpty());
                        }
                    }
                }

                Predicate<Boolean> excludeStatePredicate = WildcardExpressionResolver.excludeStatePredicate(request.indicesOptions);
                if (excludeStatePredicate != null) {
                    pureIndices = pureIndices.matching(e -> !excludeStatePredicate.test(e.isOpen()));
                }

                return new Local(pureIndices, aliases, dataStreams, nonExistingIndices);
            }

            static Local resolveWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
                IndicesRequestInfo.Scope scope = request.scope;

                ImmutableSet.Builder<Meta.Index> indices = new ImmutableSet.Builder<>();
                ImmutableSet.Builder<Meta.NonExistent> nonExistingIndices = new ImmutableSet.Builder<>();
                ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();
                ImmutableSet.Builder<Meta.DataStream> dataStreams = new ImmutableSet.Builder<>();

                boolean includeDataStreams = request.includeDataStreams && scope.includeDataStreams;
                boolean includeIndices = scope.includeIndices;
                boolean includeAliases = (scope.includeAliases && !request.indicesOptions.ignoreAliases()) || scope == IndicesRequestInfo.Scope.ALIAS;

                for (String index : request.localIndices) {
                    String resolved = DateMathExpressionResolver.resolveExpression(index);

                    Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolved);

                    if (indexLike == null) {
                        nonExistingIndices.add(Meta.NonExistent.of(resolved));
                    } else if (indexLike instanceof Meta.Alias) {
                        if (includeAliases) {
                            aliases.add((Meta.Alias) indexLike);
                        }
                        // TODO check whether we need to add NonExistent elements when we have an alias but we want to ignore it
                    } else if (indexLike instanceof Meta.DataStream) {
                        if (includeDataStreams) {
                            dataStreams.add((Meta.DataStream) indexLike);
                        }
                    } else {
                        if (includeIndices) {
                            indices.add((Meta.Index) indexLike);
                        }
                    }
                }

                ImmutableSet<Meta.Index> pureIndices = indices.build();

                if ((aliases.size() != 0 || dataStreams.size() != 0) && scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                    // If there are aliases or dataStreams, remove the indices that are part of these aliases or dataStreams

                    pureIndices = pureIndices.matching(index -> {
                        if (index.parentDataStreamName() != null && dataStreams.contains(index.parentDataStream())) {
                            return false;
                        } else if (aliases.containsAny(index.parentAliases())) {
                            return false;
                        } else {
                            return true;
                        }
                    });
                }

                return new Local(pureIndices, aliases.build(), dataStreams.build(), nonExistingIndices.build());
            }

            static Local resolveDataStreamsWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
                ImmutableSet.Builder<Meta.DataStream> dataStreams = new ImmutableSet.Builder<>();

                for (String index : request.localIndices) {
                    String resolved = DateMathExpressionResolver.resolveExpression(index);

                    Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolved);

                    if (indexLike == null) {
                        dataStreams.add(Meta.DataStream.nonExistent(resolved));
                    } else if (indexLike instanceof Meta.DataStream) {
                        dataStreams.add((Meta.DataStream) indexLike);
                    }
                }

                return new Local(ImmutableSet.empty(), ImmutableSet.empty(), dataStreams.build(), ImmutableSet.empty());
            }

            static Local resolveAliasesWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
                ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();

                for (String index : request.localIndices) {
                    String resolved = DateMathExpressionResolver.resolveExpression(index);

                    Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolved);

                    if (indexLike == null) {
                        aliases.add(Meta.Alias.nonExistent(resolved));
                    } else if (indexLike instanceof Meta.Alias) {
                        aliases.add((Meta.Alias) indexLike);
                    }
                }

                return new Local(ImmutableSet.empty(), aliases.build(), ImmutableSet.empty(), ImmutableSet.empty());
            }

            static final Local EMPTY = new Local(ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty());

            public ImmutableSet<String> getUnionOfAliasesAndDataStreams() {
                return unionOfAliasesAndDataStreams;
            }

            @Override
            public int hashCode() {
                return union.hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                } else if (obj instanceof Local) {
                    return this.union.equals(((Local) obj).union);
                } else {
                    return false;
                }
            }

        }

    }

    private ImmutableSet<IndicesRequestInfo> from(Collection<? extends IndicesRequest> indicesRequests, IndicesRequestInfo.Scope scope) {
        if (indicesRequests.isEmpty()) {
            return ImmutableSet.empty();
        }

        Meta indexMetadata = metaDataSupplier.get();
        SystemIndexAccess systemIndexAccess = systemIndexAccessSupplier.get();

        IndicesRequest first = null;
        IndicesRequestInfo firstInfo = null;
        ImmutableSet.Builder<IndicesRequestInfo> set = null;

        for (IndicesRequest current : indicesRequests) {
            if (set != null) {
                set.add(new IndicesRequestInfo(null, current, scope, systemIndexAccess, indexMetadata));
            } else if (first == null) {
                first = current;
                firstInfo = new IndicesRequestInfo(null, current, scope, systemIndexAccess, indexMetadata);
            } else if (equals(current, first)) {
                // skip
            } else {
                set = new ImmutableSet.Builder<>(indicesRequests.size());
                set.add(firstInfo);
                set.add(new IndicesRequestInfo(null, current, scope, systemIndexAccess, indexMetadata));
            }
        }

        if (set != null) {
            return set.build();
        } else {
            return ImmutableSet.of(firstInfo);
        }
    }

    private static boolean containsWildcard(Collection<String> indices) {
        return indices == null || indices.stream().anyMatch((i) -> i != null && (i.contains("*") || i.equals("_all")));
    }

    private static boolean containsWildcard(String index) {
        return index == null || index.contains("*");
    }

    private static boolean containsWildcard(IndicesRequest request) {
        String[] indices = request.indices();

        if (indices == null || indices.length == 0) {
            return true;
        }

        if (!request.indicesOptions().expandWildcardsOpen() && !request.indicesOptions().expandWildcardsClosed()) {
            return false;
        }

        for (int i = 0; i < indices.length; i++) {
            if (indices[i].equals("_all") || indices[i].equals("*")) {
                return true;
            }

            if (Regex.isSimpleMatchPattern(indices[i])) {
                return true;
            }
        }

        return false;
    }

    private static boolean containsNegation(ImmutableList<String> indices) {
        return indices != null && indices.forAnyApplies((i) -> i.startsWith("-"));
    }

    private static boolean equals(IndicesRequest a, IndicesRequest b) {
        return Arrays.equals(a.indices(), b.indices()) && Objects.equals(a.indicesOptions(), b.indicesOptions())
                && (a instanceof Replaceable ? ((Replaceable) a).allowsRemoteIndices()
                        : false) == (b instanceof Replaceable ? ((Replaceable) b).allowsRemoteIndices() : false)
                && a.includeDataStreams() == b.includeDataStreams();
    }
}
