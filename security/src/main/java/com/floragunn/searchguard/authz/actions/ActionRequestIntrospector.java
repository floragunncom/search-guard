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
import java.util.function.Function;
import java.util.regex.PatternSyntaxException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.IndicesRequest.Replaceable;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstraction.Alias;
import org.elasticsearch.cluster.metadata.IndexAbstraction.ConcreteIndex;
import org.elasticsearch.cluster.metadata.IndexAbstraction.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotUtils;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.support.SnapshotRestoreHelper;
import com.floragunn.searchsupport.queries.DateMathExpressionResolver;
import com.floragunn.searchsupport.queries.WildcardExpressionResolver;

public class ActionRequestIntrospector {

    private static final IndicesOptions EXACT = new IndicesOptions(EnumSet.noneOf(IndicesOptions.Option.class),
            EnumSet.noneOf(IndicesOptions.WildcardStates.class));

    private static final Set<String> NAME_BASED_SHORTCUTS_FOR_CLUSTER_ACTIONS = ImmutableSet.of("indices:data/read/msearch/template",
            "indices:data/read/search/template", "indices:data/read/sql/translate", "indices:data/read/sql", "indices:data/read/sql/close_cursor",
            "cluster:admin/scripts/painless/execute");

    private final static Logger log = LogManager.getLogger(ActionRequestIntrospector.class);
    private final IndexNameExpressionResolver resolver;
    private final ClusterService clusterService;
    private final ClusterInfoHolder clusterInfoHolder;
    private final GuiceDependencies guiceDependencies;

    public ActionRequestIntrospector(IndexNameExpressionResolver resolver, ClusterService clusterService, ClusterInfoHolder clusterInfoHolder,
            GuiceDependencies guiceDependencies) {
        super();
        this.resolver = resolver;
        this.clusterService = clusterService;
        this.clusterInfoHolder = clusterInfoHolder;
        this.guiceDependencies = guiceDependencies;
    }

    public ActionRequestInfo getActionRequestInfo(String action, Object request) {

        if (NAME_BASED_SHORTCUTS_FOR_CLUSTER_ACTIONS.contains(action)) {
            return CLUSTER_REQUEST;
        }

        if (request instanceof SingleShardRequest) {
            // SingleShardRequest can reference exactly one index or no indices at all (which might be a bit surprising)
            SingleShardRequest<?> singleShardRequest = (SingleShardRequest<?>) request;
            
            if (singleShardRequest.index() != null) {
                return new ActionRequestInfo(singleShardRequest.index(), SingleShardRequest.INDICES_OPTIONS);
            } else {
                // Actions which can have a null index:
                // - AnalyzeAction.Request
                // - PainlessExecuteAction ("cluster:admin/scripts/painless/execute"): This is a cluster action, so index information does not matter here
                // Here, we assume that the request references all indices. However, this is not really true for the AnalyzeAction.Request, which indeed references no index at all in this case.
                // We have in reduceIndices() a special case for AnalyzeAction.Request which takes care that the user just needs to have the privilege for any index.

                return new ActionRequestInfo("*", IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
            }            
        } else if (request instanceof IndicesRequest) {
            if (request instanceof PutMappingRequest) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;

                if (putMappingRequest.getConcreteIndex() != null) {
                    return new ActionRequestInfo(putMappingRequest.getConcreteIndex().getName(), EXACT);
                } else {
                    return new ActionRequestInfo(putMappingRequest);
                }
            } else if (request instanceof FieldCapabilitiesIndexRequest) {
                // FieldCapabilitiesIndexRequest implements IndicesRequest. However,  this delegates to the original indices specified in the FieldCapabilitiesIndexRequest.
                // On the level of FieldCapabilitiesIndexRequest, it is sufficient to only consider the index stored in the index attribute. 

                return new ActionRequestInfo(((FieldCapabilitiesIndexRequest) request).index(), EXACT);
            } else if (request instanceof ResizeRequest) {
                // Note: The targetIndex of ResizeRequest gets special treatment in PrivilegesEvaluator

                // ResizeRequest returns incorrect indicesOptions, so we hardcode them here
                ResizeRequest resizeRequest = (ResizeRequest) request;

                return new ActionRequestInfo(resizeRequest.getSourceIndex(), EXACT).additional("target",
                        ((ResizeRequest) request).getTargetIndexRequest());
            } else {
                // request instanceof IndicesRequest
                return new ActionRequestInfo((IndicesRequest) request);
            }
        } else if (request instanceof CompositeIndicesRequest) {

            if (request instanceof BulkRequest) {
                return new ActionRequestInfo(((BulkRequest) request).requests());
            } else if (request instanceof MultiGetRequest) {
                return new ActionRequestInfo(((MultiGetRequest) request).getItems());
            } else if (request instanceof MultiSearchRequest) {
                return new ActionRequestInfo(((MultiSearchRequest) request).requests());
            } else if (request instanceof MultiTermVectorsRequest) {
                return new ActionRequestInfo(((MultiTermVectorsRequest) request).getRequests());
            } else if (request instanceof ReindexRequest) {
                return CLUSTER_REQUEST;
            } else {
                log.warn("Unknown action request: {} ", request.getClass().getName());
                return UNKNOWN;
            }

        } else if (request instanceof RestoreSnapshotRequest) {

            if (clusterInfoHolder.isLocalNodeElectedMaster() == Boolean.FALSE) {
                return UNKNOWN;
            }

            RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;
            SnapshotInfo snapshotInfo = SnapshotRestoreHelper.getSnapshotInfo(restoreRequest, guiceDependencies.getRepositoriesService());

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

                return new ActionRequestInfo(renamedTargetIndices, EXACT);
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
            if (action.startsWith("index:")) {
                log.warn("Unknown action request: {}", request.getClass().getName());
            } else {
                log.debug("Unknown action request: {}", request.getClass().getName());
            }
            return UNKNOWN;
        }
    }

    public PrivilegesEvaluationResult reduceIndices(String action, Object request, Set<String> keepIndices, ActionRequestInfo actionRequestInfo)
            throws PrivilegesEvaluationException {

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

            ResolvedIndices resolvedIndices = getResolvedIndices(replaceableIndicesRequest, actionRequestInfo);
            ImmutableSet<String> actualIndices = resolvedIndices.getLocal().getUnion();

            if (keepIndices.containsAll(actualIndices)) {
                return PrivilegesEvaluationResult.OK;
            }

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

        return PrivilegesEvaluationResult.INSUFFICIENT;
    }

    private void validateIndexReduction(String action, Object request, Set<String> keepIndices) throws PrivilegesEvaluationException {
        ActionRequestInfo newInfo = getActionRequestInfo(action, request);

        if (!keepIndices.containsAll(newInfo.getResolvedIndices().getLocal().getUnion())) {
            throw new PrivilegesEvaluationException(
                    "Indices were not properly reduced: " + request + "/" + newInfo.getResolvedIndices() + "; keep: " + keepIndices);
        }
    }

    public boolean forceEmptyResult(Object request) throws PrivilegesEvaluationException {
        if (request instanceof IndicesRequest.Replaceable) {
            IndicesRequest.Replaceable replaceableIndicesRequest = (IndicesRequest.Replaceable) request;

            if (replaceableIndicesRequest.indicesOptions().expandWildcardsOpen()
                    || replaceableIndicesRequest.indicesOptions().expandWildcardsClosed()) {
                replaceableIndicesRequest.indices(new String[] { ".force_no_index*", "-*" });
            } else {
                replaceableIndicesRequest.indices(new String[0]);
            }

            validateIndexReduction("", replaceableIndicesRequest, Collections.emptySet());

            return true;
        } else {
            return false;
        }
    }

    public boolean replaceIndices(Object request, Function<ResolvedIndices, List<String>> replacementFunction, ActionRequestInfo actionRequestInfo) {
        if (request instanceof IndicesRequest) {
            if (request instanceof IndicesRequest.Replaceable) {
                IndicesRequest.Replaceable replaceableIndicesRequest = (IndicesRequest.Replaceable) request;

                String[] indices = applyReplacementFunction(replaceableIndicesRequest, replacementFunction, actionRequestInfo);

                if (indices.length > 0) {
                    replaceableIndicesRequest.indices(indices);
                    return true;
                } else {
                    return false;
                }
            } else if (request instanceof SingleShardRequest) {
                SingleShardRequest<?> singleShardRequest = (SingleShardRequest<?>) request;

                String[] indices = applyReplacementFunction(singleShardRequest, replacementFunction, actionRequestInfo);

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
                    if (!replaceIndices(searchRequest, replacementFunction, actionRequestInfo)) {
                        return false;
                    }
                }

                return true;
            } else if (request instanceof MultiTermVectorsRequest) {
                for (TermVectorsRequest termVectorsRequest : ((MultiTermVectorsRequest) request).getRequests()) {
                    if (!replaceIndices(termVectorsRequest, replacementFunction, actionRequestInfo)) {
                        return false;
                    }
                }

                return true;
            }
        }

        return false;
    }

    private ResolvedIndices getResolvedIndices(IndicesRequest indicesRequest, ActionRequestInfo actionRequestInfo) {
        if (actionRequestInfo != null && actionRequestInfo.isFor(indicesRequest)) {
            return actionRequestInfo.getResolvedIndices();
        } else {
            return new ActionRequestInfo(indicesRequest).getResolvedIndices();
        }
    }

    private String[] applyReplacementFunction(IndicesRequest indicesRequest, Function<ResolvedIndices, List<String>> replacementFunction,
            ActionRequestInfo actionRequestInfo) {
        ResolvedIndices resolvedIndices = getResolvedIndices(indicesRequest, actionRequestInfo);
        List<String> replacedLocalIndices = new ArrayList<>(replacementFunction.apply(resolvedIndices));
        replacedLocalIndices.addAll(resolvedIndices.getRemoteIndices());
        return replacedLocalIndices.toArray(new String[replacedLocalIndices.size()]);
    }

    public ActionRequestInfo create(String index, IndicesOptions indicesOptions) {
        return new ActionRequestInfo(ImmutableSet.of(new IndicesRequestInfo(null, index, indicesOptions, clusterService.state())));
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
        private ResolvedIndices resolvedIndices;
        private ImmutableMap<String, ResolvedIndices> additionalResolvedIndices;

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

        ActionRequestInfo(IndicesRequest indices) {
            this(ImmutableSet.of(new IndicesRequestInfo(null, indices, clusterService.state())));
            this.sourceRequest = indices;
        }

        ActionRequestInfo(Collection<? extends IndicesRequest> indices) {
            this(from(indices));
        }

        ActionRequestInfo(String index, IndicesOptions indicesOptions) {
            this(ImmutableSet.of(new IndicesRequestInfo(null, index, indicesOptions, clusterService.state())));
        }

        ActionRequestInfo(List<String> index, IndicesOptions indicesOptions) {
            this(ImmutableSet.of(new IndicesRequestInfo(null, index, indicesOptions, clusterService.state())));
        }

        ActionRequestInfo(boolean unknown, boolean indexRequest, ImmutableSet<IndicesRequestInfo> indices, ResolvedIndices resolvedIndices,
                ImmutableMap<String, ResolvedIndices> additionalResolvedIndices, Object sourceRequest) {
            this.unknown = unknown;
            this.indexRequest = indexRequest;
            this.indices = indices;
            this.resolvedIndices = resolvedIndices;
            this.resolvedIndicesInitialized = true;
            this.additionalResolvedIndices = additionalResolvedIndices;
            this.sourceRequest = sourceRequest;
        }

        ActionRequestInfo additional(String role, IndicesRequest indices) {
            return new ActionRequestInfo(unknown, indexRequest, this.indices.with(new IndicesRequestInfo(role, indices, clusterService.state())));
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

            return resolvedIndices;
        }

        public ImmutableMap<String, ResolvedIndices> getAdditionalResolvedIndices() {
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
                resolvedIndices = LOCAL_ALL;
                additionalResolvedIndices = ImmutableMap.empty();
                return;
            }

            if (!indexRequest) {
                resolvedIndices = null;
                additionalResolvedIndices = ImmutableMap.empty();
                return;
            }

            int numberOfEntries = indices.size();

            if (numberOfEntries == 0) {
                resolvedIndices = LOCAL_ALL;
                additionalResolvedIndices = ImmutableMap.empty();
            } else if (numberOfEntries == 1 && indices.only().role == null) {
                resolvedIndices = indices.only().resolveIndices();
                additionalResolvedIndices = ImmutableMap.empty();
            } else {
                ResolvedIndices resolvedIndices = null;
                ImmutableMap<String, ResolvedIndices> additionalResolvedIndicesMap = ImmutableMap.empty();

                for (IndicesRequestInfo info : indices) {
                    ResolvedIndices singleResolved = info.resolveIndices();

                    if (info.role == null) {
                        resolvedIndices = singleResolved.with(resolvedIndices);
                    } else {
                        additionalResolvedIndicesMap = additionalResolvedIndicesMap.withComputed(info.role,
                                (additionalResolvedIndices) -> singleResolved.with(additionalResolvedIndices));
                    }
                }

                this.resolvedIndices = resolvedIndices;
                this.additionalResolvedIndices = additionalResolvedIndicesMap;
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
                return "indices: " + getResolvedIndices() + "; additional: " + getAdditionalResolvedIndices() + "; source: " + this.indices
                        + "; containsWildcards: " + containsWildcards;
            }
        }

        public ImmutableSet<IndicesRequestInfo> getUnresolved() {
            return indices;
        }

    }

    public class IndicesRequestInfo {

        private final ImmutableList<String> indices;
        private final String[] indicesArray;
        private final IndicesOptions indicesOptions;
        private final boolean allowsRemoteIndices;
        private final boolean includeDataStreams;
        private final String role;
        private final boolean expandWildcards;
        private final boolean isAll;
        private final boolean containsWildcards;
        private final boolean containsNegation;
        private final boolean writeRequest;
        private final boolean createIndexRequest;
        private final ClusterState clusterState;
        private final ImmutableSet<String> remoteIndices;
        private final ImmutableList<String> localIndices;

        IndicesRequestInfo(String role, IndicesRequest indicesRequest, ClusterState clusterState) {
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
            this.isAll = this.expandWildcards && this.isAll(localIndices, remoteIndices);
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(this.indices) : false;
            this.containsNegation = this.containsWildcards && !this.isAll && containsNegation(this.indices);
            this.writeRequest = indicesRequest instanceof DocWriteRequest;
            this.createIndexRequest = indicesRequest instanceof IndexRequest
                    || indicesRequest instanceof org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
            this.clusterState = clusterState;
        }

        IndicesRequestInfo(String role, String index, IndicesOptions indicesOptions, ClusterState clusterState) {
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
            this.isAll = this.expandWildcards && this.isAll(localIndices, remoteIndices);
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(index) : false;
            this.containsNegation = false;
            this.writeRequest = false;
            this.createIndexRequest = false;
            this.clusterState = clusterState;
        }

        IndicesRequestInfo(String role, List<String> indices, IndicesOptions indicesOptions, ClusterState clusterState) {
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
            this.isAll = this.expandWildcards && this.isAll(localIndices, remoteIndices);
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(this.indices) : false;
            this.containsNegation = this.containsWildcards && !this.isAll && containsNegation(this.indices);
            this.writeRequest = false;
            this.createIndexRequest = false;
            this.clusterState = clusterState;

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

        private boolean isAll(List<String> localIndices, ImmutableSet<String> remoteIndices) {
            if (localIndices.isEmpty() && !remoteIndices.isEmpty()) {
                return false;
            }

            return IndexNameExpressionResolver.isAllIndices(localIndices)
                    || (localIndices.size() == 1 && (localIndices.get(0) == null || localIndices.get(0).equals("*")));
        }

        ResolvedIndices resolveIndices() {
            if (isAll()) {
                return new ResolvedIndices(true, ResolvedIndices.Local.EMPTY, remoteIndices, ImmutableSet.of(this));
            }

            if (localIndices.size() == 0) {
                return new ResolvedIndices(false, ResolvedIndices.Local.EMPTY, remoteIndices, ImmutableSet.empty());
            } else if (isExpandWildcards() && localIndices.size() == 1 && (localIndices.contains(Metadata.ALL) || localIndices.contains("*"))) {
                // In case of * wildcards, we defer resolution of indices. Chances are that we do not need to resolve the wildcard at all in this case.
                return new ResolvedIndices(true, ResolvedIndices.Local.EMPTY, remoteIndices, ImmutableSet.of(this));
            } else {
                return new ResolvedIndices(false, ResolvedIndices.Local.resolve(this, clusterService.state().metadata()), remoteIndices,
                        ImmutableSet.empty());
            }
        }

        private ImmutableSet<String> resolveDateMathExpressions() {
            ImmutableSet<String> result = ImmutableSet.empty();

            for (String index : localIndices) {
                result = result.with(resolver.resolveDateMathExpression(index));
            }

            return result;
        }

        private ImmutableSet<String> resolveWriteIndex() {
            ImmutableSet<String> result = ImmutableSet.empty();

            for (String index : localIndices) {
                Index concreteIndex = resolver.concreteWriteIndex(clusterService.state(), indicesOptions, index, true, includeDataStreams);

                if (concreteIndex != null) {
                    result = result.with(concreteIndex.getName());
                } else {
                    result = result.with(resolver.resolveDateMathExpression(index));
                }
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

        public ClusterState getClusterState() {
            return clusterState;
        }

    }

    private final ActionRequestInfo UNKNOWN = new ActionRequestInfo(true, false);
    private final ActionRequestInfo CLUSTER_REQUEST = new ActionRequestInfo(true, false);
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

        private void resolveDeferredRequests() {
            Local localShallow = this.localShallow;

            for (IndicesRequestInfo info : this.deferredRequests) {
                localShallow = localShallow.with(Local.resolve(info, info.clusterState.metadata()));
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
            return getLocal().getUnion().with(getRemoteIndices());
        }

        public ImmutableSet<String> getLocalSubset(Set<String> superSet) {
            return getLocal().getUnion().intersection(superSet).with(remoteIndices);
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
            private final ImmutableMap<String, ConcreteIndex> pureIndices;
            private final ImmutableMap<String, Alias> aliases;
            private final ImmutableMap<String, DataStream> dataStreams;
            private final ImmutableSet<String> nonExistingIndices;
            private final ImmutableSet<String> unionOfAliasesAndDataStreams;
            private final ImmutableSet<String> union;
            private String asString;
            private ImmutableSet<String> deepUnion;

            /**
             * Public only for testing!
             */
            public Local(ImmutableMap<String, ConcreteIndex> pureIndices, ImmutableMap<String, Alias> aliases,
                    ImmutableMap<String, DataStream> dataStreams, ImmutableSet<String> nonExistingIndices) {
                this.pureIndices = pureIndices;
                this.aliases = aliases;
                this.dataStreams = dataStreams;
                this.nonExistingIndices = nonExistingIndices;
                this.unionOfAliasesAndDataStreams = aliases.keySet().with(dataStreams.keySet());
                this.union = pureIndices.keySet().with(nonExistingIndices).with(this.unionOfAliasesAndDataStreams);
            }

            /**
             * Only for testing!
             */
            Local(ImmutableSet<String> localIndices) {
                this.pureIndices = ImmutableMap.empty();
                this.aliases = ImmutableMap.empty();
                this.dataStreams = ImmutableMap.empty();
                this.nonExistingIndices = localIndices;
                this.unionOfAliasesAndDataStreams = ImmutableSet.empty();
                this.union = localIndices;
            }

            private Local(ImmutableMap<String, ConcreteIndex> pureIndices, ImmutableMap<String, Alias> aliases,
                    ImmutableMap<String, DataStream> dataStreams, ImmutableSet<String> nonExistingIndices,
                    ImmutableSet<String> unionOfAliasesAndDataStreams, ImmutableSet<String> union) {
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

                    ImmutableMap.Builder<String, ConcreteIndex> mergedPureIndices = new ImmutableMap.Builder<>(
                            this.pureIndices.size() + other.pureIndices.size());

                    for (ConcreteIndex index : this.pureIndices.values()) {
                        if (index.getParentDataStream() != null && other.dataStreams.containsKey(index.getParentDataStream().getName())) {
                            continue;
                        }

                        if (index.getAliases() != null && other.unionOfAliasesAndDataStreams.containsAny(index.getAliases())) {
                            continue;
                        }

                        mergedPureIndices.put(index.getName(), index);
                    }

                    for (ConcreteIndex index : other.pureIndices.values()) {
                        if (index.getParentDataStream() != null && this.dataStreams.containsKey(index.getParentDataStream().getName())) {
                            continue;
                        }

                        if (index.getAliases() != null && this.unionOfAliasesAndDataStreams.containsAny(index.getAliases())) {
                            continue;
                        }

                        mergedPureIndices.put(index.getName(), index);
                    }

                    return new Local(mergedPureIndices.build(), this.aliases.with(other.aliases), this.dataStreams.with(other.dataStreams),
                            this.nonExistingIndices.with(other.nonExistingIndices));
                }
            }

            public int size() {
                return this.union.size();
            }

            public ImmutableMap<String, ConcreteIndex> getPureIndices() {
                return pureIndices;
            }

            public ImmutableMap<String, Alias> getAliases() {
                return aliases;
            }

            public ImmutableMap<String, DataStream> getDataStreams() {
                return dataStreams;
            }

            public ImmutableSet<String> getNonExistingIndices() {
                return nonExistingIndices;
            }

            public ImmutableSet<String> getUnion() {
                return union;
            }

            public ImmutableSet<String> getDeepUnion() {
                ImmutableSet<String> result = this.deepUnion;

                if (result == null) {
                    result = this.resolveDeep(unionOfAliasesAndDataStreams).with(this.union);
                    this.deepUnion = result;
                }

                return result;
            }

            /**
             * Resolves the named alias or dataStream to its contained concrete indices. If the named alias or dataStream does not exist, or if it is an index, an empty set is returned.
             */
            public Set<String> resolveDeep(String aliasOrDataStream) {
                Alias alias = this.aliases.get(aliasOrDataStream);
                if (alias != null) {
                    ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>(alias.getIndices().size());

                    for (Index index : alias.getIndices()) {
                        result.add(index.getName());
                    }

                    return result.build();
                }

                DataStream dataStream = this.dataStreams.get(aliasOrDataStream);
                if (dataStream != null) {
                    ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>(dataStream.getIndices().size());

                    for (Index index : dataStream.getIndices()) {
                        result.add(index.getName());
                    }

                    return result.build();
                }

                return ImmutableSet.empty();
            }

            public ImmutableSet<String> resolveDeep(ImmutableSet<String> aliasesAndDataStreams) {
                ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>(aliasesAndDataStreams.size() * 20);

                for (String name : aliasesAndDataStreams) {

                    Alias alias = this.aliases.get(name);
                    if (alias != null) {
                        for (Index index : alias.getIndices()) {
                            result.add(index.getName());
                        }
                        continue;
                    }

                    DataStream dataStream = this.dataStreams.get(name);
                    if (dataStream != null) {
                        for (Index index : dataStream.getIndices()) {
                            result.add(index.getName());
                        }
                    }
                }

                return result.build();
            }

            boolean hasAliasesOrDataStreams() {
                return !aliases.isEmpty() || !dataStreams.isEmpty();
            }

            @Override
            public String toString() {
                String result = this.asString;

                if (result != null) {
                    return result;
                }

                StringBuilder resultBuilder = new StringBuilder("{");

                if (!this.pureIndices.isEmpty()) {
                    resultBuilder.append("indices: ").append(this.pureIndices.keySet());
                }

                if (!this.aliases.isEmpty()) {
                    if (!this.pureIndices.isEmpty()) {
                        resultBuilder.append("; ");
                    }

                    resultBuilder.append("aliases: ").append(this.aliases.keySet());
                }

                if (!this.dataStreams.isEmpty()) {
                    if (!this.pureIndices.isEmpty() || !this.aliases.isEmpty()) {
                        resultBuilder.append("; ");
                    }

                    resultBuilder.append("dataStreams: ").append(this.dataStreams.keySet());
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

            static Local resolve(IndicesRequestInfo request, Metadata metadata) {
                try {
                    if (request.isAll) {
                        return resolveIsAll(request, metadata);
                    } else if (request.expandWildcards) {
                        return resolveWithPatterns(request, metadata);
                        //} else if (request.writeRequest) { TODO
                        //    return request.resolveWriteIndex();
                    } else if (request.createIndexRequest) {
                        return new Local(ImmutableMap.empty(), ImmutableMap.empty(), ImmutableMap.empty(), request.resolveDateMathExpressions());
                    } else {
                        // No wildcards, no write request, no create index request
                        return resolveWithoutPatterns(request, metadata);
                    }
                } catch (RuntimeException e) {
                    log.error("Error while resolving " + request, e);
                    throw e;
                }
            }

            static Local resolveWithPatterns(IndicesRequestInfo request, Metadata metadata) {
                SortedMap<String, IndexAbstraction> indicesLookup = metadata.getIndicesLookup();

                ImmutableMap.Builder<String, ConcreteIndex> indices = new ImmutableMap.Builder<>();
                ImmutableSet.Builder<String> nonExistingIndices = new ImmutableSet.Builder<>();
                ImmutableMap.Builder<String, Alias> aliases = new ImmutableMap.Builder<>();
                ImmutableMap.Builder<String, DataStream> dataStreams = new ImmutableMap.Builder<>();
                Set<String> excludeNames = new HashSet<>();

                for (int i = request.localIndices.size() - 1; i >= 0; i--) {
                    String index = request.localIndices.get(i);

                    if (index.startsWith("-")) {
                        index = index.substring(1);
                        index = DateMathExpressionResolver.resolveExpression(index);

                        if (index.contains("*")) {
                            Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index,
                                    request.indicesOptions, request.includeDataStreams);

                            excludeNames.addAll(matchedAbstractions.keySet());
                        } else {
                            excludeNames.add(index);
                        }
                    } else {
                        index = DateMathExpressionResolver.resolveExpression(index);

                        if (index.contains("*")) {

                            Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index,
                                    request.indicesOptions, request.includeDataStreams);

                            for (Map.Entry<String, IndexAbstraction> entry : matchedAbstractions.entrySet()) {
                                if (excludeNames.contains(entry.getKey())) {
                                    continue;
                                }

                                IndexAbstraction indexAbstraction = entry.getValue();

                                if (indexAbstraction instanceof Alias) {
                                    aliases.put(entry.getKey(), (Alias) indexAbstraction);
                                } else if (indexAbstraction instanceof DataStream) {
                                    dataStreams.put(entry.getKey(), (DataStream) indexAbstraction);
                                } else {
                                    indices.put(entry.getKey(), (ConcreteIndex) indexAbstraction);
                                }
                            }
                        } else {
                            if (excludeNames.contains(index)) {
                                continue;
                            }

                            IndexAbstraction indexAbstraction = indicesLookup.get(index);

                            if (indexAbstraction == null) {
                                nonExistingIndices.add(index);
                            } else if (indexAbstraction instanceof Alias) {
                                if (!request.indicesOptions.ignoreAliases()) {
                                    aliases.put(index, (Alias) indexAbstraction);
                                }
                            } else if (indexAbstraction instanceof DataStream) {
                                if (request.includeDataStreams) {
                                    dataStreams.put(index, (DataStream) indexAbstraction);
                                }
                            } else {
                                indices.put(index, (ConcreteIndex) indexAbstraction);
                            }
                        }
                    }
                }

                ImmutableMap<String, ConcreteIndex> pureIndices = indices.build();

                if (aliases.size() != 0 || dataStreams.size() != 0) {
                    ImmutableMap.Builder<String, ConcreteIndex> filteredPureIndices = new ImmutableMap.Builder<>(pureIndices.size());

                    for (ConcreteIndex index : pureIndices.values()) {
                        if (index.getParentDataStream() != null && dataStreams.contains(index.getParentDataStream().getName())) {
                            continue;
                        }

                        if (index.getAliases() != null && aliases.containsAnyKey(index.getAliases())) {
                            continue;
                        }

                        filteredPureIndices.put(index.getName(), index);
                    }

                    pureIndices = filteredPureIndices.build();
                }

                return new Local(pureIndices, aliases.build(), dataStreams.build(), nonExistingIndices.build());
            }

            static Local resolveIsAll(IndicesRequestInfo request, Metadata metadata) {
                SortedMap<String, IndexAbstraction> indicesLookup = metadata.getIndicesLookup();

                ImmutableMap.Builder<String, ConcreteIndex> indices = new ImmutableMap.Builder<>();
                ImmutableMap.Builder<String, Alias> aliases = new ImmutableMap.Builder<>();
                ImmutableMap.Builder<String, DataStream> dataStreams = new ImmutableMap.Builder<>();

                IndexMetadata.State excludeState = WildcardExpressionResolver.excludeState(request.indicesOptions);

                for (IndexAbstraction indexAbstraction : indicesLookup.values()) {
                    if (indexAbstraction instanceof Alias) {
                        aliases.put(indexAbstraction.getName(), (Alias) indexAbstraction);
                    } else if (indexAbstraction instanceof DataStream) {
                        if (request.includeDataStreams) {
                            dataStreams.put(indexAbstraction.getName(), (DataStream) indexAbstraction);
                        }
                    } else {
                        ConcreteIndex concreteIndex = (ConcreteIndex) indexAbstraction;
                        IndexMetadata indexMetadata = metadata.index(indexAbstraction.getName());

                        if ((!request.includeDataStreams || concreteIndex.getParentDataStream() == null)
                                && (concreteIndex.getAliases() == null || concreteIndex.getAliases().isEmpty())
                                && (excludeState == null || indexMetadata.getState() != excludeState)
                                && (!indexMetadata.isHidden() || request.indicesOptions.expandWildcardsHidden())) {
                            indices.put(concreteIndex.getName(), concreteIndex);
                        }
                    }
                }

                return new Local(indices.build(), aliases.build(), dataStreams.build(), ImmutableSet.empty());
            }

            static Local resolveWithoutPatterns(IndicesRequestInfo request, Metadata metadata) {
                ImmutableMap.Builder<String, ConcreteIndex> indices = new ImmutableMap.Builder<>();
                ImmutableSet.Builder<String> nonExistingIndices = new ImmutableSet.Builder<>();
                ImmutableMap.Builder<String, Alias> aliases = new ImmutableMap.Builder<>();
                ImmutableMap.Builder<String, DataStream> dataStreams = new ImmutableMap.Builder<>();
                Map<String, IndexAbstraction> indicesLookup = metadata.getIndicesLookup();

                for (String index : request.localIndices) {
                    String resolved = DateMathExpressionResolver.resolveExpression(index);

                    IndexAbstraction indexAbstraction = indicesLookup.get(resolved);

                    if (indexAbstraction == null) {
                        nonExistingIndices.add(resolved);
                    } else if (indexAbstraction instanceof Alias) {
                        if (!request.indicesOptions.ignoreAliases()) {
                            aliases.put(indexAbstraction.getName(), (Alias) indexAbstraction);
                        }
                    } else if (indexAbstraction instanceof DataStream) {
                        if (request.includeDataStreams) {
                            dataStreams.put(indexAbstraction.getName(), (DataStream) indexAbstraction);
                        }
                    } else {
                        indices.put(indexAbstraction.getName(), (ConcreteIndex) indexAbstraction);
                    }
                }

                ImmutableMap<String, ConcreteIndex> pureIndices = indices.build();

                if (aliases.size() != 0 || dataStreams.size() != 0) {
                    ImmutableMap.Builder<String, ConcreteIndex> filteredPureIndices = new ImmutableMap.Builder<>(pureIndices.size());

                    for (ConcreteIndex index : pureIndices.values()) {
                        if (index.getParentDataStream() != null && dataStreams.contains(index.getParentDataStream().getName())) {
                            continue;
                        }

                        if (index.getAliases() != null && aliases.containsAnyKey(index.getAliases())) {
                            continue;
                        }

                        filteredPureIndices.put(index.getName(), index);
                    }

                    pureIndices = filteredPureIndices.build();
                }

                return new Local(pureIndices, aliases.build(), dataStreams.build(), nonExistingIndices.build());
            }

            static final Local EMPTY = new Local(ImmutableMap.empty(), ImmutableMap.empty(), ImmutableMap.empty(), ImmutableSet.empty());

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

    private ImmutableSet<IndicesRequestInfo> from(Collection<? extends IndicesRequest> indicesRequests) {
        if (indicesRequests.isEmpty()) {
            return ImmutableSet.empty();
        }

        ClusterState state = clusterService.state();

        IndicesRequest first = null;
        IndicesRequestInfo firstInfo = null;
        ImmutableSet.Builder<IndicesRequestInfo> set = null;

        for (IndicesRequest current : indicesRequests) {
            if (set != null) {
                set.add(new IndicesRequestInfo(null, current, state));
            } else if (first == null) {
                first = current;
                firstInfo = new IndicesRequestInfo(null, current, state);
            } else if (equals(current, first)) {
                // skip
            } else {
                set = new ImmutableSet.Builder<>(indicesRequests.size());
                set.add(firstInfo);
                set.add(new IndicesRequestInfo(null, current, state));
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
