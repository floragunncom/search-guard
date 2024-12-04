/*
 * Copyright 2020-2022 floragunn GmbH
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

package com.floragunn.searchguard.configuration;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.CountAggregation;
import com.floragunn.searchsupport.indices.IndexMapping;
import com.google.common.collect.ImmutableMap;

public class ProtectedConfigIndexService implements ComponentStateProvider {
    private final static Logger log = LogManager.getLogger(ProtectedConfigIndexService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ProtectedIndices protectedIndices;

    private final ComponentState componentState = new ComponentState(100, null, "protected_config_index_service");
    private final CountAggregation flushPendingIndicesCount = new CountAggregation();

    private final Set<ConfigIndexState> pendingIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<ConfigIndexState> completedIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final AtomicBoolean ready = new AtomicBoolean(false);

    private final static ImmutableMap<String, Object> INDEX_SETTINGS = ImmutableMap.of("index.number_of_shards", 1, "index.auto_expand_replicas",
            "0-all", "index.hidden", true);

    public ProtectedConfigIndexService(Client client, ClusterService clusterService, ThreadPool threadPool, ProtectedIndices protectedIndices) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.protectedIndices = protectedIndices;

        clusterService.addListener(clusterStateListener);

        this.componentState.addMetrics("flush_pending_indices", flushPendingIndicesCount);
    }

    public ComponentState createIndex(ConfigIndex configIndex) {
        ConfigIndexState configIndexState = new ConfigIndexState(configIndex);

        synchronized (componentState) {
            componentState.addPart(configIndexState.moduleState);
        }

        protectedIndices.add(configIndex.getName());

        if (!ready.get()) {
            pendingIndices.add(configIndexState);
        } else {
            createIndexNow(configIndexState, clusterService.state());
        }

        return configIndexState.moduleState;
    }

    public DocNode flushPendingIndices() {
        return flushPendingIndices(this.clusterService.state());
    }

    public DocNode flushPendingIndices(ClusterState clusterState) {
        try {
            if (this.pendingIndices.isEmpty()) {
                componentState.setInitialized();
                return DocNode.of("info", "completed");
            }

            flushPendingIndicesCount.increment();

            Set<ConfigIndexState> pendingIndices = new HashSet<>(this.pendingIndices);

            this.pendingIndices.removeAll(pendingIndices);
            Map<String, String> result = new HashMap<>();

            for (ConfigIndexState configIndex : pendingIndices) {
                String configIndexResult = createIndexNow(configIndex, clusterState);
                result.put(configIndex.getName(), configIndexResult);
            }

            return DocNode.wrap(result);
        } catch (Exception e) {
            log.error("Error in flushPendingIndices()", e);
            componentState.addLastException("flushPendingIndices", e);
            componentState.setFailed(e);
            return DocNode.of("error", e.getMessage());
        }
    }

    public void onNodeStart() {
        ready.set(true);

        threadPool.generic().execute(() -> checkClusterState(clusterService.state()));
    }

    private void checkClusterState(ClusterState clusterState) {
        try {
            if (!ready.get()) {
                componentState.setState(State.INITIALIZING, "waiting_for_node_started");
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("checkClusterState()\npendingIndices: " + pendingIndices);
            }

            if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                componentState.setState(State.INITIALIZING, "waiting_for_state_recovery");
                log.trace("State not yet recovered. Waiting more.");
                return;
            }
            
            componentState.setState(State.INITIALIZING, "waiting_for_master");

            if (clusterState.nodes().isLocalNodeElectedMaster() || clusterState.nodes().getMasterNode() != null) {
                flushPendingIndices(clusterState);
            }
        } catch (Exception e) {
            log.error("Error in checkClusterState()", e);
            componentState.addLastException("checkClusterState", e);
            componentState.setFailed(e);
        }

        if (!this.pendingIndices.isEmpty()) {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(30), threadPool.generic(),
                    () -> checkClusterState(clusterService.state()));
        }
    }

    private String createIndexNow(ConfigIndexState configIndex, ClusterState clusterState) {

        try {
            if (log.isTraceEnabled()) {
                log.trace("createIndexNow(" + configIndex + ")");
            }

            if (completedIndices.contains(configIndex)) {
                if (log.isTraceEnabled()) {
                    log.trace(configIndex + " is already completed");
                }
                return "completed";
            }

            IndexAbstraction indexAbstraction;
            if ((indexAbstraction = clusterState.getMetadata().getIndicesLookup().get(configIndex.getName())) != null) {
                final IndexMetadata indexMetadata = clusterState.getMetadata().index(indexAbstraction.getWriteIndex());
                if (log.isTraceEnabled()) {
                    log.trace(configIndex + " does already exist.");
                }

                //if the index is not hidden we make it hidden
                if (!indexMetadata.isHidden()) {

                    if (log.isInfoEnabled()) {
                        log.info("Index settings for " + configIndex.getName() + " needs to be updated");
                    }

                    client.admin().indices().updateSettings(
                            new UpdateSettingsRequest(indexMetadata.getIndex().getName())
                                    .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build()),
                            new ActionListener<AcknowledgedResponse>() {

                                @Override
                                public void onResponse(AcknowledgedResponse response) {
                                    log.info("Settings update for " + configIndex + " successful");
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    log.error("Settings update failed for " + configIndex, e);
                                }
                            });
                }

                if (configIndex.mappingUpdates.size() != 0) {
                    int mappingVersion = getMappingVersion(configIndex, clusterState);

                    if (log.isTraceEnabled()) {
                        log.trace("Mapping version of index: " + mappingVersion);
                    }

                    SortedMap<Integer, Map<String, Object>> availableUpdates = configIndex.mappingUpdates.tailMap(mappingVersion);

                    if (availableUpdates.size() != 0) {
                        Integer patchFrom = availableUpdates.firstKey();

                        Map<String, Object> patch = configIndex.mappingUpdates.get(patchFrom);

                        if (log.isInfoEnabled()) {
                            log.info("Updating mapping of index " + configIndex.getName() + " from version " + mappingVersion + " to version "
                                    + configIndex.mappingVersion);
                        }

                        configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "mapping_update");

                        PutMappingRequest putMappingRequest = new PutMappingRequest(configIndex.getName()).source(patch);

                        client.admin().indices().putMapping(putMappingRequest, new ActionListener<AcknowledgedResponse>() {

                            @Override
                            public void onResponse(AcknowledgedResponse response) {
                                configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "mapping_updated");
                                completedIndices.add(configIndex);
                                configIndex.setCreated(true);

                                if (configIndex.getListener() != null) {
                                    configIndex.waitForYellowStatus();
                                } else {
                                    configIndex.moduleState.setInitialized();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.error("Mapping update failed for " + configIndex, e);
                                configIndex.setFailed(e);
                                configIndex.moduleState.setState(ComponentState.State.FAILED, "mapping_update_failed");

                            }
                        });

                        return "mapping_updated";
                    }
                }

                completedIndices.add(configIndex);
                configIndex.setCreated(true);

                if (configIndex.getListener() != null) {
                    configIndex.waitForYellowStatus();
                } else {
                    configIndex.moduleState.setInitialized();
                }

                return "exists";
            }

            //index does not exist so we will create it

            if (!clusterState.nodes().isLocalNodeElectedMaster()) {
                pendingIndices.add(configIndex);
                configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "waiting_for_master");
                return "waiting_for_master";
            }

            CreateIndexRequest request = new CreateIndexRequest(configIndex.getName());

            if (configIndex.getMapping() != null) {
                request.mapping(configIndex.getMapping());
            }

            request.settings(INDEX_SETTINGS);

            if (log.isDebugEnabled()) {
                log.debug("Creating index " + request.index());
            }

            completedIndices.add(configIndex);
            configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "creating");

            CreateIndexResponse createIndexResponse = client.admin().indices().create(request).actionGet();
            configIndex.setCreated(true);

            if (createIndexResponse.isAcknowledged()) {
                if (log.isDebugEnabled()) {
                    log.debug("Created " + configIndex + ": " + Strings.toString(createIndexResponse));
                }

                if (configIndex.getListener() != null) {
                    configIndex.waitForYellowStatus();
                } else {
                    configIndex.moduleState.setInitialized();
                }

                return "created";
            } else {
                throw new Exception("Index creation was not acknowledged");
            }

        } catch (ResourceAlreadyExistsException e) {
            configIndex.setCreated(true);

            if (configIndex.getListener() != null) {
                configIndex.waitForYellowStatus();
            } else {
                configIndex.moduleState.setInitialized();
            }

            return "created_by_other_node";
        } catch (Exception e) {
            pendingIndices.add(configIndex);
            log.error("Error while creating index " + configIndex, e);
            configIndex.moduleState.addLastException("createIndexNow", e);
            return "error";
        }
    }

    private int getMappingVersion(ConfigIndexState configIndex, ClusterState clusterState) {
        IndexMetadata index = clusterState.getMetadata().index(clusterState.getMetadata().getIndicesLookup().get(configIndex.getName()).getWriteIndex());
        MappingMetadata mapping = index.mapping();

        if (mapping == null) {
            return 0;
        }

        Object meta = mapping.getSourceAsMap().get("_meta");

        if (!(meta instanceof Map)) {
            return 0;
        }

        Object version = ((Map<?, ?>) meta).get("version");

        if (version instanceof Number) {
            return ((Number) version).intValue();
        } else {
            return 0;
        }

    }

    private final ClusterStateListener clusterStateListener = new ClusterStateListener() {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            threadPool.generic().execute(() -> checkClusterState(event.state()));
        }
    };

    private class ConfigIndexState {
        private final String name;
        private final Map<String, Object> mapping;
        private final int mappingVersion;
        private final SortedMap<Integer, Map<String, Object>> mappingUpdates;
        private final IndexReadyListener listener;
        private final String[] allIndices;
        private final ComponentState moduleState;
        private volatile long createdAt;

        ConfigIndexState(ConfigIndex configIndex) {
            this.name = configIndex.name;
            this.mapping = configIndex.mapping;
            this.mappingVersion = configIndex.mappingVersion;
            this.listener = configIndex.listener;
            this.moduleState = new ComponentState(5, "index", configIndex.name);
            this.mappingUpdates = configIndex.mappingUpdates;

            if (configIndex.indexDependencies == null || configIndex.indexDependencies.length == 0) {
                allIndices = new String[] { name };
            } else {
                allIndices = new String[configIndex.indexDependencies.length + 1];
                allIndices[0] = name;
                System.arraycopy(configIndex.indexDependencies, 0, allIndices, 1, configIndex.indexDependencies.length);
            }
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }

        @Override
        public String toString() {
            return "ConfigIndex [name=" + name + "]";
        }

        public void setFailed(Exception failed) {
            this.moduleState.setFailed(failed);
        }

        public void setCreated(boolean created) {
            if (created) {
                this.moduleState.setInitialized();
                this.createdAt = System.currentTimeMillis();
            }
        }

        public IndexReadyListener getListener() {
            return listener;
        }

        public void waitForYellowStatus() {
            if (log.isTraceEnabled()) {
                log.trace("waitForYellowStatus(" + this + ")");
            }

            this.moduleState.setState(ComponentState.State.INITIALIZING, "waiting_for_yellow_status");
            this.moduleState.startNextTry();

            TimeValue masterNodeTimeout = new TimeValue(30, TimeUnit.SECONDS);
            client.admin().cluster().health(new ClusterHealthRequest(masterNodeTimeout, allIndices).waitForYellowStatus().timeout(TimeValue.timeValueMinutes(5)),
                    new ActionListener<ClusterHealthResponse>() {

                        @Override
                        public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                            if (clusterHealthResponse.getStatus() == ClusterHealthStatus.YELLOW
                                    || clusterHealthResponse.getStatus() == ClusterHealthStatus.GREEN) {

                                if (log.isDebugEnabled()) {
                                    log.debug(ConfigIndexState.this + " reached status " + Strings.toString(clusterHealthResponse));
                                }

                                threadPool.generic().submit(() -> tryOnIndexReady());

                                return;
                            }

                            if (isTimedOut()) {
                                moduleState.setFailed("Index " + name + " is has not become ready. Giving up");
                                moduleState.setDetailJson(Strings.toString(clusterHealthResponse));
                                log.error("Index " + name + " is has not become ready:\n" + clusterHealthResponse + "\nGiving up.");
                                return;
                            }

                            if (isLate()) {
                                log.error("Index " + name + " is not yet ready:\n" + clusterHealthResponse + "\nRetrying.");
                                moduleState.setDetailJson(Strings.toString(clusterHealthResponse));
                            } else if (log.isTraceEnabled()) {
                                log.trace("Index " + name + " is not yet ready:\n" + clusterHealthResponse + "\nRetrying.");
                            }

                            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), threadPool.generic(),
                                    () -> waitForYellowStatus());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (isTimedOut()) {
                                log.error("Index " + name + " is has not become ready. Giving up.", e);
                                moduleState.setFailed(e);
                                return;
                            }

                            if (isLate()) {
                                log.warn("Index " + name + " is not yet ready. Retrying.", e);
                                moduleState.addLastException("waiting_for_yellow_status", e);
                            } else if (log.isTraceEnabled()) {
                                log.trace("Index " + name + " is not yet ready. Retrying.", e);
                            }

                            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), threadPool.generic(),
                                    () -> waitForYellowStatus());
                        }
                    });
        }

        private void tryOnIndexReady() {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("tryOnIndexReady(" + this + ")");
                }

                this.moduleState.setState(ComponentState.State.INITIALIZING, "final_probe");
                this.moduleState.startNextTry();

                listener.onIndexReady(new FailureListener() {

                    @Override
                    public void onFailure(Exception e) {
                        if (isTimedOut()) {
                            log.error("Initialization for " + name + " failed. Giving up.", e);
                            moduleState.setFailed(e);
                            return;
                        }

                        if (isLate()) {
                            log.warn("Initialization for " + name + " not yet successful. Retrying.", e);
                        } else if (log.isTraceEnabled()) {
                            log.trace("Initialization for " + name + " not yet successful. Retrying.", e);
                        }

                        threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), threadPool.generic(), () -> tryOnIndexReady());

                    }

                    @Override
                    public void onSuccess() {
                        moduleState.setInitialized();
                    }

                });

            } catch (Exception e) {
                log.error("Error in onIndexReady of " + this, e);
            }
        }

        private boolean isTimedOut() {
            return System.currentTimeMillis() > (createdAt + 24 * 60 * 60 * 1000);
        }

        private boolean isLate() {
            return System.currentTimeMillis() > (createdAt + 60 * 1000);
        }

    }

    public static class ConfigIndex {
        private String name;
        private Map<String, Object> mapping;
        private int mappingVersion;
        private IndexReadyListener listener;
        private String[] indexDependencies = new String[0];
        private SortedMap<Integer, Map<String, Object>> mappingUpdates = new TreeMap<>();

        public ConfigIndex(String name) {
            this.name = name;
        }

        public ConfigIndex mapping(IndexMapping mapping) {
            this.mapping = mapping.toDocNode().toMap();
            return this;
        }

        public ConfigIndex mapping(Map<String, Object> mapping) {
            this.mapping = mapping;
            this.mappingVersion = 1;
            return this;
        }

        public ConfigIndex mapping(Map<String, Object> mapping, int mappingVersion) {
            this.mapping = new HashMap<>(mapping);
            this.mappingVersion = mappingVersion;

            this.mapping.put("_meta", ImmutableMap.of("version", mappingVersion));

            return this;
        }

        public ConfigIndex mappingUpdate(int fromVersion, Map<String, Object> mappingDelta) {
            if (this.mappingVersion == 0) {
                throw new IllegalStateException("A mapping needs to be defined first");
            }

            mappingDelta = new HashMap<>(mappingDelta);
            mappingDelta.put("_meta", ImmutableMap.of("version", this.mappingVersion));
            this.mappingUpdates.put(fromVersion, mappingDelta);
            return this;
        }

        public ConfigIndex onIndexReady(IndexReadyListener listener) {
            this.listener = listener;
            return this;
        }

        public ConfigIndex dependsOnIndices(String... indexDependencies) {
            if (indexDependencies != null) {
                this.indexDependencies = indexDependencies;
            }
            return this;
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }

        public SortedMap<Integer, Map<String, Object>> getMappingUpdates() {
            return mappingUpdates;
        }

    }

    @FunctionalInterface
    public static interface IndexReadyListener {
        void onIndexReady(FailureListener failureListener);
    }

    public static interface FailureListener {
        void onSuccess();

        void onFailure(Exception e);
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public static class TriggerConfigIndexCreationAction extends ActionType<TriggerConfigIndexCreationAction.Response> {

        public static final TriggerConfigIndexCreationAction INSTANCE = new TriggerConfigIndexCreationAction();
        public static final String NAME = "cluster:admin:searchguard:internal/indices/create";

        public static final RestApi REST_API = new RestApi()//
                .responseHeaders(SearchGuardVersion.header())//
                .handlesPost("/_searchguard/internal/indices/create").with(TriggerConfigIndexCreationAction.INSTANCE, (params, body) -> new Request(), Response::status)//
                .name("/_searchguard/internal/indices/create");

        protected TriggerConfigIndexCreationAction() {
            super(NAME);
        }

        public static class Request extends BaseNodesRequest<Request> {

            Request() {
                super(new String[0]);
            }

            Request(Collection<DiscoveryNode> concreteNodes) {
                super(concreteNodes.toArray(new DiscoveryNode[concreteNodes.size()]));
            }
        }

        public static class Response extends BaseNodesResponse<NodeResponse> implements ToXContentObject {

            public Response(StreamInput in) throws IOException {
                super(in);
            }

            public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
                super(clusterName, nodes, failures);
            }

            @Override
            public List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
                return in.readCollectionAsList(NodeResponse::new);
            }

            @Override
            public void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
                out.writeCollection(nodes);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("nodes", getNodesMap());

                if (hasFailures()) {
                    builder.field("failures");
                    builder.startArray();
                    for (FailedNodeException failure : failures()) {
                        builder.startObject();
                        failure.toXContent(builder, params);
                        builder.endObject();
                    }
                    builder.endArray();
                }

                builder.endObject();
                return builder;
            }

            public RestStatus status() {
                return RestStatus.OK;
            }
        }

        public static class NodeRequest extends TransportRequest {

            public NodeRequest(StreamInput in) throws IOException {
                super(in);
            }

            public NodeRequest() {
                super();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }

        public static class NodeResponse extends BaseNodeResponse implements ToXContentObject {

            private final DocNode result;

            public NodeResponse(StreamInput in) throws IOException {
                super(in);
                this.result = DocNode.wrap(in.readGenericMap());
            }

            public NodeResponse(DiscoveryNode node, DocNode result) {
                super(node);
                this.result = result;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeGenericMap(result.toMap());
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.map(result.toMap());
                return builder;
            }

        }

        public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {

            private final ProtectedConfigIndexService protectedConfigIndexService;

            @Inject
            public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                    ActionFilters actionFilters, ProtectedConfigIndexService protectedConfigIndexService) {
                super(TriggerConfigIndexCreationAction.NAME, clusterService, transportService, actionFilters,
                        NodeRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));

                this.protectedConfigIndexService = protectedConfigIndexService;
            }

            @Override
            protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
                return new NodeResponse(in);
            }

            @Override
            protected Response newResponse(Request request, List<NodeResponse> responses, List<FailedNodeException> failures) {
                return new Response(this.clusterService.getClusterName(), responses, failures);
            }

            @Override
            protected NodeResponse nodeOperation(NodeRequest request, Task task) {
                return new NodeResponse(clusterService.localNode(), protectedConfigIndexService.flushPendingIndices());
            }

            @Override
            protected NodeRequest newNodeRequest(Request request) {
                return new NodeRequest();
            }
        }

    }

}
