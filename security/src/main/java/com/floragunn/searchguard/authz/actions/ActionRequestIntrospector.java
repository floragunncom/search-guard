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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.floragunn.fluent.collections.ImmutableList;
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
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotUtils;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchsupport.action.IndicesOptionsSupport;
import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo.Scope;
import com.floragunn.searchsupport.meta.Meta;

public class ActionRequestIntrospector {

    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';

    private static final IndicesOptions EXACT = IndicesOptionsSupport.EXACT;

    private static final Set<String> NAME_BASED_SHORTCUTS_FOR_CLUSTER_ACTIONS = ImmutableSet.of("indices:data/read/msearch/template",
            "indices:data/read/search/template", "indices:data/read/sql/translate", "indices:data/read/sql", "indices:data/read/sql/close_cursor",
            "cluster:admin/scripts/painless/execute", "indices:admin/template/get", "cluster:admin/component_template/get",
            "indices:admin/index_template/get", "indices:admin/index_template/simulate_index", "indices:admin/index_template/simulate",
            "indices:data/read/close_point_in_time");

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
        if (request instanceof SearchRequest searchRequest && (searchRequest.pointInTimeBuilder() != null)) {
            // In point-in-time queries, wildcards in index names are expanded when the open point-in-time request
            // is sent. Therefore, a list of indices in search requests with PIT can be treated literally.
            BytesReference pointInTimeId = searchRequest.pointInTimeBuilder().getEncodedId();
            String[] indices = SearchContextId.decodeIndices(pointInTimeId);
            return new ActionRequestInfo(indices == null ? ImmutableList.empty() : ImmutableList.ofArray(indices), EXACT,
                    IndicesRequestInfo.Scope.ANY);
        } else if (request instanceof SingleShardRequest) {
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
            if (action.scope() == Action.Scope.DATA_STREAM) {
                return new ActionRequestInfo((IndicesRequest) request, IndicesRequestInfo.Scope.DATA_STREAM);
            } else if (request instanceof AliasesRequest) {
                AliasesRequest aliasesRequest = (AliasesRequest) request;
                IndicesRequest indicesRequest = (IndicesRequest) request;

                // We segment all requests implementing AliasesRequest (which is mostly GetAliasRequest) this way:
                // - The requested indices get into the main ActionRequestInfo object. This can also include data streams.
                // - The requested aliases get into an additional ActionRequestInfo object with role ALIAS

                return new ActionRequestInfo(indicesRequest.indices(), indicesRequest.indicesOptions(), IndicesRequestInfo.Scope.INDICES_DATA_STREAMS)//
                        .additional(Action.AdditionalDimension.ALIASES, aliasesRequest.aliases(),
                                aliasesRequest.expandAliasesWildcards() ? IndicesOptions.lenientExpandHidden() : EXACT,
                                IndicesRequestInfo.Scope.ALIAS);
            } else if (request instanceof IndicesAliasesRequest) {
                IndicesAliasesRequest indicesAliasesRequest = (IndicesAliasesRequest) request;
                ActionRequestInfo result = new ActionRequestInfo(ImmutableSet.empty());

                for (IndicesAliasesRequest.AliasActions aliasAction : indicesAliasesRequest.getAliasActions()) {
                    switch (aliasAction.actionType()) {
                    case ADD:
                        result = result.with(aliasAction, IndicesRequestInfo.Scope.INDICES_DATA_STREAMS) //
                                .additional(Action.AdditionalDimension.ALIASES, aliasAction.aliases(), EXACT, IndicesRequestInfo.Scope.ALIAS);
                        break;
                    case REMOVE:
                        // We do some more heavy lifting here. The wildcard in the index attribute of the AliasActions calls shall only
                        // refer to indices that are member of the aliases. Thus, we resolve early and determine the intersection.
                        IndicesRequestInfo aliasesRequestInfo = new IndicesRequestInfo(Action.AdditionalDimension.ALIASES,
                                ImmutableList.ofArray(aliasAction.aliases()),
                                aliasAction.expandAliasesWildcards() ? IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN : EXACT,
                                IndicesRequestInfo.Scope.ALIAS, systemIndexAccessSupplier.get(), metaDataSupplier.get());
                        ImmutableSet<Meta.Alias> aliases = aliasesRequestInfo.resolveIndices().getLocal().getAliases();

                        IndicesRequestInfo indicesRequestInfo = new IndicesRequestInfo(null, aliasAction,
                                IndicesRequestInfo.Scope.INDICES_DATA_STREAMS, systemIndexAccessSupplier.get(), metaDataSupplier.get());
                        ImmutableSet<Meta.IndexLikeObject> indices = indicesRequestInfo.resolveIndices().getLocal().getUnion();
                        ImmutableSet<Meta.IndexLikeObject> indicesThatAreMembersOfSpecifiedAliases = indices
                                .matching(i -> i.parentAliases().containsAny(aliases));

                        result = result
                                .with(indicesThatAreMembersOfSpecifiedAliases.map(Meta.IndexLikeObject::name), EXACT,
                                        IndicesRequestInfo.Scope.INDICES_DATA_STREAMS) //
                                .additional(aliasesRequestInfo);
                        break;
                    case REMOVE_INDEX:
                        // This is the most weird part of IndicesAliasesRequest: You can delete an index - completely unrelated to aliases.
                        result = result.additional(Action.AdditionalDimension.DELETE_INDEX, aliasAction,
                                IndicesRequestInfo.Scope.INDICES_DATA_STREAMS);
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
                            .additional(Action.AdditionalDimension.MANAGE_ALIASES, ImmutableList.of(createIndexRequest.aliases()).map(a -> a.name()),
                                    EXACT, IndicesRequestInfo.Scope.ALIAS);
                }
            } else if (request instanceof PutMappingRequest) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;

                if (putMappingRequest.getConcreteIndex() != null) {
                    return new ActionRequestInfo(putMappingRequest.getConcreteIndex().getName(), EXACT, IndicesRequestInfo.Scope.ANY);
                } else {
                    return new ActionRequestInfo(putMappingRequest, IndicesRequestInfo.Scope.ANY);
                }
            } else if (request instanceof ResizeRequest) {
                // Note: The targetIndex of ResizeRequest gets special treatment in PrivilegesEvaluator

                // ResizeRequest returns incorrect indicesOptions, so we hardcode them here
                ResizeRequest resizeRequest = (ResizeRequest) request;

                return new ActionRequestInfo(resizeRequest.getSourceIndex(), EXACT, IndicesRequestInfo.Scope.ANY).additional(
                        Action.AdditionalDimension.RESIZE_TARGET, ((ResizeRequest) request).getTargetIndexRequest(), IndicesRequestInfo.Scope.ANY);
            } else if (request instanceof DownsampleAction.Request downsampleRequest) {

                return new ActionRequestInfo(downsampleRequest.getSourceIndex(), downsampleRequest.indicesOptions(), IndicesRequestInfo.Scope.ANY)
                        .additional(Action.AdditionalDimension.DOWNSAMPLE_TARGET, ImmutableList.of(downsampleRequest.getTargetIndex()), EXACT,
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
                return unknownActionRequest();
            }

        } else if (request instanceof RestoreSnapshotRequest) {

            if (!isLocalNodeElectedMaster.getAsBoolean()) {
                return unknownActionRequest();
            }

            RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;
            SnapshotInfo snapshotInfo = this.getSnapshotInfoFunction.apply(restoreRequest);

            if (snapshotInfo == null) {
                log.warn("snapshot repository '" + restoreRequest.repository() + "', snapshot '" + restoreRequest.snapshot() + "' not found");
                return unknownActionRequest();
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
        } else if (request instanceof ClearScrollRequest) {
            return CLUSTER_REQUEST;
        } else if (request instanceof SearchScrollRequest) {
            return CLUSTER_REQUEST;
        } else {
            if (action.isIndexLikePrivilege()) {
                log.warn("Unknown action request: {}", request.getClass().getName());
                return unknownActionRequest();
            } else {
                log.debug("Unknown action request: {}", request.getClass().getName());
                return CLUSTER_REQUEST;
            }
        }
    }

    public boolean isReduceIndicesAvailable(Action action, Object request) {
        return request instanceof AnalyzeAction.Request || request instanceof IndicesRequest.Replaceable;
    }

    public PrivilegesEvaluationResult reduceIndices(Action action, Object request, ImmutableSet<String> keepIndices,
            ImmutableMap<Action.AdditionalDimension, ImmutableSet<String>> additionalKeepIndices, ActionRequestInfo actionRequestInfo)
            throws PrivilegesEvaluationException {

        if (log.isTraceEnabled()) {
            log.trace("Reducing indices of {} to {}", request, keepIndices);
        }

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

                ImmutableSet<String> keepAliases = additionalKeepIndices.get(Action.AdditionalDimension.ALIASES);
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
        } else {
            log.debug("Cannot reduce the indices of {} because the request does not implement a suitable interface", request);
            return PrivilegesEvaluationResult.INSUFFICIENT;
        }

    }

    private void validateIndexReduction(Action action, Object request, Set<String> keepIndices) throws PrivilegesEvaluationException {
        ActionRequestInfo newInfo = getActionRequestInfo(action, request);

        if (log.isDebugEnabled()) {
            log.debug("Reduced request to:\n{}\n{}", request, newInfo);
        }

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
                    action.scope() == Action.Scope.DATA_STREAM ? IndicesRequestInfo.Scope.DATA_STREAM : IndicesRequestInfo.Scope.ANY);
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
        private ImmutableMap<Action.AdditionalDimension, ResolvedIndices> additionalResolvedIndices;

        private Boolean containsWildcards;

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

        ActionRequestInfo with(Collection<String> indices, IndicesOptions indicesOptions, IndicesRequestInfo.Scope scope) {
            return new ActionRequestInfo(unknown, indexRequest, this.indices.with(new IndicesRequestInfo(null, ImmutableList.of(indices),
                    indicesOptions, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo additional(Action.AdditionalDimension role, IndicesRequest indices, IndicesRequestInfo.Scope scope) {
            return new ActionRequestInfo(unknown, indexRequest,
                    this.indices.with(new IndicesRequestInfo(role, indices, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo additional(Action.AdditionalDimension role, Collection<? extends IndicesRequest> requests, IndicesRequestInfo.Scope scope) {
            Meta meta = metaDataSupplier.get();
            SystemIndexAccess systemIndexAccess = systemIndexAccessSupplier.get();
            return new ActionRequestInfo(unknown, indexRequest, this.indices
                    .with(requests.stream().map(r -> new IndicesRequestInfo(role, r, scope, systemIndexAccess, meta)).collect(Collectors.toList())));
        }

        ActionRequestInfo additional(Action.AdditionalDimension role, ImmutableList<String> indices, IndicesOptions indicesOptions,
                IndicesRequestInfo.Scope scope) {
            return new ActionRequestInfo(unknown, indexRequest, this.indices
                    .with(new IndicesRequestInfo(role, indices, indicesOptions, scope, systemIndexAccessSupplier.get(), metaDataSupplier.get())));
        }

        ActionRequestInfo additional(Action.AdditionalDimension role, String[] indices, IndicesOptions indicesOptions,
                IndicesRequestInfo.Scope scope) {
            return this.additional(role, ImmutableList.ofArray(indices), indicesOptions, scope);
        }

        ActionRequestInfo additional(IndicesRequestInfo indexRequestInfo) {
            return new ActionRequestInfo(unknown, indexRequest, this.indices.with(indexRequestInfo));
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

        public ImmutableMap<Action.AdditionalDimension, ResolvedIndices> getAdditionalResolvedIndices() {
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
            if (unknown || !indexRequest) {
                mainResolvedIndices = allResolvedIndices = localAll();
                additionalResolvedIndices = ImmutableMap.empty();
                return;
            }

            int numberOfEntries = indices.size();

            if (numberOfEntries == 0) {
                mainResolvedIndices = allResolvedIndices = localAll();
                additionalResolvedIndices = ImmutableMap.empty();
            } else if (numberOfEntries == 1 && indices.only().role == null) {
                mainResolvedIndices = allResolvedIndices = indices.only().resolveIndices();
                additionalResolvedIndices = ImmutableMap.empty();
            } else {
                ResolvedIndices mainResolvedIndices = ResolvedIndices.EMPTY;
                ImmutableMap<Action.AdditionalDimension, ResolvedIndices> additionalResolvedIndicesMap = ImmutableMap.empty();

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

    static class IndicesRequestInfo {

        private final ImmutableList<String> indices;
        private final String[] indicesArray;
        private final IndicesOptions indicesOptions;
        private final boolean allowsRemoteIndices;
        final boolean includeDataStreams;
        private final Action.AdditionalDimension role;
        final boolean expandWildcards;
        final boolean isAll;
        private final boolean containsWildcards;
        private final boolean writeRequest;
        final boolean createIndexRequest;
        final SystemIndexAccess systemIndexAccess;
        final Meta indexMetadata;
        private final ImmutableSet<String> remoteIndices;
        final ImmutableList<String> localIndices;
        final Scope scope;
        private final boolean negationOnlyEffectiveForIndices;

        IndicesRequestInfo(Action.AdditionalDimension role, IndicesRequest indicesRequest, Scope scope, SystemIndexAccess systemIndexAccess,
                Meta indexMetadata) {
            this.indices = indicesRequest.indices() != null ? ImmutableList.ofArray(indicesRequest.indices()) : ImmutableList.empty();
            this.indicesArray = indicesRequest.indices();
            this.indicesOptions = indicesRequest.indicesOptions();
            this.allowsRemoteIndices = indicesRequest instanceof Replaceable ? ((Replaceable) indicesRequest).allowsRemoteIndices() : false;
            this.includeDataStreams = indicesRequest.includeDataStreams();
            this.role = role;
            this.expandWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsHidden()
                    || indicesOptions.expandWildcardsClosed();
            this.localIndices = this.indices.matching(Predicate.not(ActionRequestIntrospector::isRemoteIndex));
            this.remoteIndices = ImmutableSet.of(this.indices.matching(ActionRequestIntrospector::isRemoteIndex));
            this.isAll = this.expandWildcards && this.isAll(localIndices, remoteIndices, indicesRequest);
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(this.indices) : false;
            this.writeRequest = indicesRequest instanceof DocWriteRequest;
            this.createIndexRequest = indicesRequest instanceof IndexRequest
                    || indicesRequest instanceof org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
            this.indexMetadata = indexMetadata;
            this.scope = scope;
            this.systemIndexAccess = systemIndexAccess;
            this.negationOnlyEffectiveForIndices = scope != Scope.DATA_STREAM && scope != Scope.ALIAS;
        }

        IndicesRequestInfo(Action.AdditionalDimension role, String index, IndicesOptions indicesOptions, Scope scope,
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
            this.writeRequest = false;
            this.createIndexRequest = false;
            this.indexMetadata = indexMetadata;

            this.scope = scope;
            this.systemIndexAccess = systemIndexAccess;
            this.negationOnlyEffectiveForIndices = scope != Scope.DATA_STREAM && scope != Scope.ALIAS;
        }

        IndicesRequestInfo(Action.AdditionalDimension role, List<String> indices, IndicesOptions indicesOptions, Scope scope,
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
            this.writeRequest = false;
            this.createIndexRequest = false;
            this.indexMetadata = indexMetadata;

            this.scope = scope;
            this.systemIndexAccess = systemIndexAccess;
            this.negationOnlyEffectiveForIndices = scope != Scope.DATA_STREAM && scope != Scope.ALIAS;
        }

        IndicesRequestInfo(List<String> indices, IndicesOptions indicesOptions, Scope scope, SystemIndexAccess systemIndexAccess,
                Meta indexMetadata) {
            this(null, indices, indicesOptions, scope, systemIndexAccess, indexMetadata);
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

        @Override
        public String toString() {
            return "[indices=" + indices + ", indicesOptions=" + indicesOptions + ", allowsRemoteIndices=" + allowsRemoteIndices
                    + ", includeDataStreams=" + includeDataStreams + ", role=" + role + "]";
        }

        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        boolean isNegationOnlyEffectiveForIndices() {
            return negationOnlyEffectiveForIndices;
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

            final boolean includeIndices;
            final boolean includeAliases;
            final boolean includeDataStreams;

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
    }

    private ActionRequestInfo unknownActionRequest() {
        return new ActionRequestInfo(true, true, ImmutableSet.of(localAllIndicesRequestInfo()));
    }

    private ResolvedIndices localAll() {
        return new ResolvedIndices(true, ResolvedIndices.Local.EMPTY, ImmutableSet.empty(), ImmutableSet.of(localAllIndicesRequestInfo()));
    }

    private IndicesRequestInfo localAllIndicesRequestInfo() {
        return new IndicesRequestInfo(ImmutableList.of("*"), IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED, Scope.ANY, systemIndexAccessSupplier.get(),
                metaDataSupplier.get());
    }

    private final ActionRequestInfo CLUSTER_REQUEST = new ActionRequestInfo(false, false, null);

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

    static boolean containsWildcard(Collection<String> indices) {
        return indices == null || indices.stream().anyMatch((i) -> i != null && (i.contains("*") || i.equals("_all")));
    }

    static boolean containsWildcard(String index) {
        return index == null || index.contains("*") || index.equals("_all");
    }

    private static boolean equals(IndicesRequest a, IndicesRequest b) {
        return Arrays.equals(a.indices(), b.indices()) && Objects.equals(a.indicesOptions(), b.indicesOptions())
                && (a instanceof Replaceable ? ((Replaceable) a).allowsRemoteIndices()
                        : false) == (b instanceof Replaceable ? ((Replaceable) b).allowsRemoteIndices() : false)
                && a.includeDataStreams() == b.includeDataStreams();
    }

    static boolean isRemoteIndex(String indexName) {
        int firstIndex = indexName.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR);
        int lastIndex = indexName.lastIndexOf(REMOTE_CLUSTER_INDEX_SEPARATOR);

        // If both are same and not -1, there's exactly one colon
        return (firstIndex != -1) && (firstIndex == lastIndex);
    }
}
