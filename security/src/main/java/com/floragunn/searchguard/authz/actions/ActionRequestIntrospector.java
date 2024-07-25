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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.floragunn.fluent.collections.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.IndicesRequest.Replaceable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.rest.root.MainRequest;
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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotUtils;
import org.elasticsearch.transport.RemoteClusterService;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.support.SnapshotRestoreHelper;

import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS;

public class ActionRequestIntrospector {

    private static final IndicesOptions EXACT = new IndicesOptions(
            new IndicesOptions.ConcreteTargetOptions(false),
            IndicesOptions.WildcardOptions.builder()
                    .resolveAliases(true)
                    .matchClosed(false)
                    .includeHidden(false)
                    .allowEmptyExpressions(false)
                    .matchOpen(false)
                    .build(),
            IndicesOptions.GeneralOptions.builder()
                    .allowClosedIndices(true)
                    .allowAliasToMultipleIndices(true)
                    .ignoreThrottled(false)
                    .build()
    );

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
        if (request instanceof SearchRequest searchRequest && (searchRequest.pointInTimeBuilder() != null)) {
            // In point-in-time queries, wildcards in index names are expanded when the open point-in-time request
            // is sent. Therefore, a list of indices in search requests with PIT can be treated literally.
            String pointInTimeId = searchRequest.pointInTimeBuilder().getEncodedId();
            String[] indices = SearchContextId.decodeIndices(pointInTimeId);
            return new ActionRequestInfo(indices == null ? ImmutableList.empty() : ImmutableList.ofArray(indices), EXACT);
        } else if (request instanceof SingleShardRequest) {
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
            } else if (request instanceof FieldCapabilitiesRequest) {
                // FieldCapabilitiesIndexRequest implements IndicesRequest. However,  this delegates to the original indices specified in the FieldCapabilitiesIndexRequest.
                // On the level of FieldCapabilitiesIndexRequest, it is sufficient to only consider the index stored in the index attribute. 

                return new ActionRequestInfo(((FieldCapabilitiesRequest) request));
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
                log.warn("Unknown action request: " + request.getClass().getName());
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
                log.warn("Unknown action request: " + request.getClass().getName());
            } else {
                log.debug("Unknown action request: " + request.getClass().getName());                
            }
            return UNKNOWN;
        }
    }

    public PrivilegesEvaluationResult reduceIndices(String action, Object request, Set<String> keepIndices,
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

            ResolvedIndices resolvedIndices = getResolvedIndices(replaceableIndicesRequest, actionRequestInfo);
            ImmutableSet<String> actualIndices = resolvedIndices.getLocalIndices();

            if (keepIndices.containsAll(actualIndices)) {
                return PrivilegesEvaluationResult.OK;
            }

            if (!replaceableIndicesRequest.indicesOptions().ignoreUnavailable() && !containsWildcard(replaceableIndicesRequest)) {
                return PrivilegesEvaluationResult.INSUFFICIENT;
            }

            ImmutableSet<String> newIndices = actualIndices.intersection(keepIndices).with(resolvedIndices.getRemoteIndices());

            if (log.isTraceEnabled()) {
                log.trace("reduceIndicesForIgnoreUnavailable: keep: " + keepIndices + " actual: " + actualIndices + "; newIndices: " + newIndices);
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

        if (!keepIndices.containsAll(newInfo.getResolvedIndices().getLocalIndices())) {
            throw new PrivilegesEvaluationException(
                    "Indices were not properly reduced: " + request + "/" + newInfo.getResolvedIndices() + "; keep: " + keepIndices);
        }
    }

    public boolean forceEmptyResult(Object request) throws PrivilegesEvaluationException {
        if (request instanceof IndicesRequest.Replaceable) {
            IndicesRequest.Replaceable replaceableIndicesRequest = (IndicesRequest.Replaceable) request;

            if (replaceableIndicesRequest.indicesOptions().expandWildcardsOpen() || replaceableIndicesRequest.indicesOptions().expandWildcardsClosed()) {
                replaceableIndicesRequest.indices(new String [] {".force_no_index*", "-*"});
            } else {
                replaceableIndicesRequest.indices(new String [0]);
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
        return new ActionRequestInfo(ImmutableSet.of(new IndicesRequestInfo(null, index, indicesOptions)));
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
            log.error("Unable to parse the regular expression denoted in 'rename_pattern'. Please correct the pattern an try again.", e);
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
            this(ImmutableSet.of(new IndicesRequestInfo(null, indices)));
            this.sourceRequest = indices;
        }

        ActionRequestInfo(Collection<? extends IndicesRequest> indices) {
            this(from(indices));
        }

        ActionRequestInfo(String index, IndicesOptions indicesOptions) {
            this(ImmutableSet.of(new IndicesRequestInfo(null, index, indicesOptions)));
        }

        ActionRequestInfo(List<String> index, IndicesOptions indicesOptions) {
            this(ImmutableSet.of(new IndicesRequestInfo(null, index, indicesOptions)));
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
            return new ActionRequestInfo(unknown, indexRequest, this.indices.with(new IndicesRequestInfo(role, indices)));
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

        public ActionRequestInfo reducedIndices(ImmutableSet<String> newLocalResolvedIndices) {
            if (!resolvedIndicesInitialized) {
                initResolvedIndices();
                resolvedIndicesInitialized = true;
            }

            return new ActionRequestInfo(unknown, indexRequest, indices, resolvedIndices.localIndices(newLocalResolvedIndices),
                    additionalResolvedIndices, newLocalResolvedIndices);
        }

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

        private final List<String> indices;
        private final String[] indicesArray;
        private final IndicesOptions indicesOptions;
        private final boolean allowsRemoteIndices;
        private final boolean includeDataStreams;
        private final String role;
        private final boolean expandWildcards;
        private final boolean isAll;
        private final boolean containsWildcards;
        private final boolean writeRequest;
        private final boolean createIndexRequest;
        private ImmutableSet<String> remoteIndices;
        private List<String> localIndices;

        IndicesRequestInfo(String role, IndicesRequest indicesRequest) {
            this.indices = indicesRequest.indices() != null ? Arrays.asList(indicesRequest.indices()) : Collections.emptyList();
            this.indicesArray = indicesRequest.indices();
            this.indicesOptions = indicesRequest.indicesOptions();
            this.allowsRemoteIndices = indicesRequest instanceof Replaceable ? ((Replaceable) indicesRequest).allowsRemoteIndices() : false;
            this.includeDataStreams = indicesRequest.includeDataStreams();
            this.role = role;
            this.expandWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsHidden()
                    || indicesOptions.expandWildcardsClosed();
            this.isAll = this.expandWildcards
                    ? IndexNameExpressionResolver.isAllIndices(indices) || (indices.size() == 1 && indices.iterator().next().equals("*"))
                    : false;
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(this.indices) : false;
            this.writeRequest = indicesRequest instanceof DocWriteRequest;
            this.createIndexRequest = indicesRequest instanceof IndexRequest
                    || indicesRequest instanceof org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
        }

        IndicesRequestInfo(String role, String index, IndicesOptions indicesOptions) {
            this.indices = Collections.singletonList(index);
            this.indicesArray = new String[] { index };
            this.indicesOptions = indicesOptions;
            this.allowsRemoteIndices = true;
            this.includeDataStreams = true;
            this.role = role;
            this.expandWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsHidden()
                    || indicesOptions.expandWildcardsClosed();
            this.isAll = this.expandWildcards ? IndexNameExpressionResolver.isAllIndices(indices) || (index.equals("*")) : false;
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(index) : false;
            this.writeRequest = false;
            this.createIndexRequest = false;

        }

        IndicesRequestInfo(String role, List<String> indices, IndicesOptions indicesOptions) {
            this.indices = indices;
            this.indicesArray = indices.toArray(new String[indices.size()]);
            this.indicesOptions = indicesOptions;
            this.allowsRemoteIndices = true;
            this.includeDataStreams = true;
            this.role = role;
            this.expandWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsHidden()
                    || indicesOptions.expandWildcardsClosed();
            this.isAll = this.expandWildcards
                    ? IndexNameExpressionResolver.isAllIndices(indices) || (indices.size() == 1 && indices.get(0).equals("*"))
                    : false;
            this.containsWildcards = this.expandWildcards ? this.isAll || containsWildcard(this.indices) : false;
            this.writeRequest = false;
            this.createIndexRequest = false;
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
            if (expandWildcards) {
                return IndexNameExpressionResolver.isAllIndices(indices) || (indices.size() == 1 && indices.get(0).equals("*"));
            } else {
                return false;
            }
        }

        ResolvedIndices resolveIndices() {
            if (isAll()) {
                return new ResolvedIndices(true, ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.of(this));
            }

            checkForRemoteIndices();

            if (localIndices.size() == 0) {
                return new ResolvedIndices(false, ImmutableSet.empty(), remoteIndices, ImmutableSet.empty());
            } else if (isExpandWildcards() && localIndices.size() == 1 && (localIndices.contains(Metadata.ALL) || localIndices.contains("*"))) {
                // In case of * wildcards, we defer resolution of indices. Chances are that we do not need to resolve the wildcard at all in this case.
                return new ResolvedIndices(true, ImmutableSet.empty(), remoteIndices, ImmutableSet.of(this));
            } else {
                return new ResolvedIndices(false, resolveIndicesNow(), remoteIndices, ImmutableSet.empty());
            }
        }

        ImmutableSet<String> resolveIndicesNow() {
            try {
                checkForRemoteIndices();

                if (expandWildcards) {
                    try {
                        return ImmutableSet.ofArray(resolver.concreteIndexNames(clusterService.state(), allowNoIndices(indicesOptions),
                                this.asIndicesRequestWithoutRemoteIndices()));
                    } catch (IndexNotFoundException | IndexClosedException | InvalidIndexNameException e) {
                        // For some reason, concreteIndexNames() also throws IndexNotFoundException in some cases when ALLOW_NO_INDICES is specified. 
                        // We catch this and just return the raw index names as fallback

                        if (log.isTraceEnabled()) {
                            log.trace(
                                    "Exception in resolveIndicesNow(). This is expected due to weird implementation choices in concreteIndexNames(). Recovering: "
                                            + this,
                                    e);
                        }

                        return resolveWithoutWildcards();
                    }
                } else if (this.writeRequest) {
                    return resolveWriteIndex();
                } else if (this.createIndexRequest) {
                    return resolveDateMathExpressions();
                } else {
                    // No wildcards, no write request, no create index request
                    return resolveWithoutWildcards();
                }
            } catch (RuntimeException e) {
                log.error("Error in resolveIndicesNow() for " + this, e);
                throw e;
            }
        }

        private IndicesOptions allowNoIndices(IndicesOptions indicesOptions) {
            if (indicesOptions.allowNoIndices()) {
                return indicesOptions;
            } else {
                IndicesOptions.Builder builder = IndicesOptions.builder(indicesOptions);
                builder.wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions())
                        .allowEmptyExpressions(true).build());
                return builder.build();
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
                Index concreteIndex = resolver.concreteWriteIndex(clusterService.state(), ignoreUnavailable(indicesOptions), index, true, includeDataStreams);

                if (concreteIndex != null) {
                    result = result.with(concreteIndex.getName());
                } else {
                    result = result.with(resolver.resolveDateMathExpression(index));
                }
            }

            return result;
        }

        private ImmutableSet<String> resolveWithoutWildcards() {
            ImmutableSet<String> result = ImmutableSet.empty();
            ClusterState state = clusterService.state();
            Map<String, IndexAbstraction> indicesLookup = state.metadata().getIndicesLookup();

            for (String index : localIndices) {
                String resolved = resolver.resolveDateMathExpression(index);                
                IndexAbstraction indexAbstraction = indicesLookup.get(resolved);

                if (indexAbstraction == null) {
                    result = result.with(resolved);
                    continue;
                }

                if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && indicesOptions.ignoreAliases()) {
                    continue;
                }

                if (indexAbstraction.isDataStreamRelated() && !includeDataStreams) {
                    continue;
                }

                result = result.with(indexAbstraction.getIndices().stream().map(Index::getName).collect(Collectors.toList()));
            }

            return result;
        }

        private void checkForRemoteIndices() {
            if (this.remoteIndices == null || this.localIndices == null) {
                RemoteClusterService remoteClusterService = guiceDependencies.getTransportService().getRemoteClusterService();

                if (allowsRemoteIndices) {
                    Map<String, OriginalIndices> groupedIndices = remoteClusterService.groupIndices(indicesOptions, indicesArray);

                    OriginalIndices localOriginalIndices = groupedIndices.get(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);

                    localIndices = localOriginalIndices != null ? Arrays.asList(localOriginalIndices.indices()) : Collections.emptyList();
                    remoteIndices = buildRemoteIndicesSet(groupedIndices);
                } else {
                    localIndices = indices;
                    remoteIndices = ImmutableSet.empty();
                }

            }
        }

        private ImmutableSet<String> buildRemoteIndicesSet(Map<String, OriginalIndices> groupedIndices) {
            if (groupedIndices.size() == 1 && groupedIndices.containsKey(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY)) {
                return ImmutableSet.empty();
            }

            Set<String> result = new HashSet<>();

            for (Map.Entry<String, OriginalIndices> entry : groupedIndices.entrySet()) {
                if (!entry.getKey().equals(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY)) {
                    for (String index : entry.getValue().indices()) {
                        result.add(entry.getKey() + ":" + index);
                    }
                }
            }

            return ImmutableSet.of(result);
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
        private IndicesOptions ignoreUnavailable(IndicesOptions indicesOptions) {
            if (indicesOptions.ignoreUnavailable()) {
                return indicesOptions;
            } else {
                IndicesOptions.Builder builder = IndicesOptions.builder(indicesOptions);
                builder.concreteTargetOptions(ALLOW_UNAVAILABLE_TARGETS);
                return builder.build();
            }
        }
    }

    private final ActionRequestInfo UNKNOWN = new ActionRequestInfo(true, false);
    private final ActionRequestInfo CLUSTER_REQUEST = new ActionRequestInfo(true, false);
    private final static ResolvedIndices LOCAL_ALL = new ResolvedIndices(true, ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty());
    private final static ResolvedIndices EMPTY = new ResolvedIndices(false, ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty());

    public static class ResolvedIndices {
                
        private final boolean localAll;
        private ImmutableSet<IndicesRequestInfo> deferredRequests;
        private ImmutableSet<String> localIndices;
        protected final ImmutableSet<String> remoteIndices;

        ResolvedIndices(boolean localAll, ImmutableSet<String> localIndices, ImmutableSet<String> remoteIndices,
                ImmutableSet<IndicesRequestInfo> deferredRequests) {
            this.localAll = localAll;
            this.localIndices = localIndices;
            this.remoteIndices = remoteIndices;
            this.deferredRequests = deferredRequests;
        }

        public boolean isLocalAll() {
            return localAll;
        }

        public boolean isLocalIndicesEmpty() {
            return !localAll && getLocalIndices().isEmpty();
        }

        public boolean containsOnlyRemoteIndices() {
            return !this.localAll && this.deferredRequests.isEmpty() && this.localIndices.isEmpty() && !this.remoteIndices.isEmpty();
        }

        ResolvedIndices with(ResolvedIndices other) {
            if (other == null) {
                return this;
            }

            return new ResolvedIndices(this.localAll || other.localAll, this.localIndices.with(other.localIndices),
                    this.remoteIndices.with(other.remoteIndices), this.deferredRequests.with(other.deferredRequests));
        }

        public ImmutableSet<String> getLocalIndices() {
            if (deferredRequests.isEmpty()) {
                return localIndices;
            } else {
                ImmutableSet<String> localIndices = this.localIndices;

                for (IndicesRequestInfo info : deferredRequests) {
                    localIndices = localIndices.with(info.resolveIndicesNow());
                }

                this.localIndices = localIndices;
                this.deferredRequests = ImmutableSet.empty();

                return localIndices;
            }
        }

        public ImmutableSet<String> getRemoteIndices() {
            return remoteIndices;
        }

        public ImmutableSet<String> getLocalAndRemoteIndices() {
            return getLocalIndices().with(getRemoteIndices());
        }

        public ImmutableSet<String> getLocalSubset(Set<String> superSet) {
            return getLocalIndices().intersection(superSet).with(remoteIndices);
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
                    result.append(" [").append(localIndices.size()).append("]");
                }

                if (!remoteIndices.isEmpty()) {
                    result.append("; remote: ").append(remoteIndices.toShortString());
                }

                return result.toString();
            } else {
                return "local: " + (localIndices != null ? localIndices.toShortString() : "null") + "; remote: "
                        + (remoteIndices != null ? remoteIndices.toShortString() : "null");
            }
        }

        public ResolvedIndices localIndices(ImmutableSet<String> localIndices) {
            return new ResolvedIndices(false, localIndices, remoteIndices, ImmutableSet.empty());
        }
        
        public ResolvedIndices localIndices(String... localIndices) {
            return localIndices(ImmutableSet.ofArray(localIndices));
        }
        
        public static ResolvedIndices empty() {
            return EMPTY;
        }
    }

    private ImmutableSet<IndicesRequestInfo> from(Collection<? extends IndicesRequest> indicesRequests) {
        if (indicesRequests.isEmpty()) {
            return ImmutableSet.empty();
        }

        IndicesRequest first = null;
        IndicesRequestInfo firstInfo = null;
        Set<IndicesRequestInfo> set = null;

        for (IndicesRequest current : indicesRequests) {
            if (set != null) {
                set.add(new IndicesRequestInfo(null, current));
            } else if (first == null) {
                first = current;
                firstInfo = new IndicesRequestInfo(null, current);
            } else if (equals(current, first)) {
                // skip
            } else {
                set = new HashSet<>(indicesRequests.size());
                set.add(firstInfo);
                set.add(new IndicesRequestInfo(null, current));
            }
        }

        if (set != null) {
            return ImmutableSet.of(set);
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

        if (indices == null || indices.length == 0)  {
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

    private static boolean equals(IndicesRequest a, IndicesRequest b) {
        return Arrays.equals(a.indices(), b.indices()) && Objects.equals(a.indicesOptions(), b.indicesOptions())
                && (a instanceof Replaceable ? ((Replaceable) a).allowsRemoteIndices()
                        : false) == (b instanceof Replaceable ? ((Replaceable) b).allowsRemoteIndices() : false)
                && a.includeDataStreams() == b.includeDataStreams();
    }
}
