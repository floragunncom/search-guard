/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.modules.api;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.PartsStats;
import com.floragunn.searchsupport.cstate.metrics.Measurement;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class GetComponentStateAction extends ActionType<GetComponentStateAction.Response> {

    private static final Logger log = LogManager.getLogger(GetComponentStateAction.class);

    public static final GetComponentStateAction INSTANCE = new GetComponentStateAction();
    public static final String NAME = "cluster:admin/searchguard/components/state";

    protected GetComponentStateAction() {
        super(NAME, Response::new);
    }

    public static class Request extends BaseNodesRequest<Request> {

        private String moduleId;
        private boolean verbose;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.moduleId = in.readOptionalString();
            this.verbose = in.readBoolean();
        }

        public Request(String moduleId) {
            super(new String[0]);
            this.moduleId = moduleId;
        }

        public Request(String moduleId, boolean verbose) {
            super(new String[0]);
            this.moduleId = moduleId;
            this.verbose = verbose;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(moduleId);
            out.writeBoolean(verbose);
        }

        public boolean isVerbose() {
            return verbose;
        }

        public void setVerbose(boolean verbose) {
            this.verbose = verbose;
        }
    }

    public static class NodeRequest extends BaseNodesRequest {

        Request request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new Request(in);
        }

        public NodeRequest(Request request) {
            super((String[]) null);
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements ToXContentObject, Document<Response> {

        private String message;
        private Health health;
        private List<ComponentState> mergedComponentState;

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

        public List<ComponentState> getMergedComponentState() {
            initMergedComponentState();

            return this.mergedComponentState;
        }

        public String getMessage() {
            initMergedComponentState();

            return this.message;
        }

        public Map<String, Set<String>> getComponentsGroupedByLicense() {
            Map<String, Set<String>> result = new LinkedHashMap<String, Set<String>>();

            for (ComponentState componentState : getMergedComponentState()) {
                getComponentsGroupedByLicense(componentState, result);
            }

            return result;
        }

        private static void getComponentsGroupedByLicense(ComponentState componentState, Map<String, Set<String>> result) {
            String requiredLicense = componentState.getLicenseRequiredInfo();

            if (!"no".equals(requiredLicense)) {
                result.computeIfAbsent(requiredLicense, (k) -> new HashSet<>()).add(componentState.getTypeAndName());
            }

            if (componentState.getParts() != null) {
                for (ComponentState part : componentState.getParts()) {
                    getComponentsGroupedByLicense(part, result);
                }
            }
        }

        private void initMergedComponentState() {
            if (this.mergedComponentState != null) {
                return;
            }

            Map<String, ComponentState> map = new HashMap<>();
            List<String> infoMessages = new ArrayList<>();

            int total = 0;
            int completelyFailed = 0;
            int partiallyFailed = 0;
            int versionMismatch = 0;
            Set<String> partiallyFailedMessages = new HashSet<>();
            Set<String> versionMismatchMessages = new HashSet<>();
            ComponentState configRepoComponentState = null;

            for (NodeResponse nodeResponse : getNodes()) {
                try {
                    for (ComponentState componentState : nodeResponse.getStates()) {

                        ComponentState mergedComponentState = map.get(componentState.getKey());

                        if (mergedComponentState == null) {
                            mergedComponentState = new ComponentState(componentState.getSortPrio(), componentState.getType(),
                                    componentState.getName());
                            map.put(mergedComponentState.getKey(), mergedComponentState);
                        }

                        componentState.setNodeId(nodeResponse.getNode().getId());
                        componentState.setNodeName(nodeResponse.getNode().getName());

                        mergedComponentState.addPart(componentState);

                        if (!componentState.getMetrics().isEmpty()) {
                            for (Map.Entry<String, Measurement<?>> entry : componentState.getMetrics().entrySet()) {
                                Measurement<?> existing = mergedComponentState.getMetrics().get(entry.getKey());

                                if (existing == null) {
                                    mergedComponentState.addMetrics(entry.getKey(), entry.getValue().clone());
                                } else {
                                    existing.addToThis(entry.getValue());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("Error while processing nodeResponse " + nodeResponse, e);
                    infoMessages.add("Response from " + nodeResponse + " could not be processed: " + e.getMessage());
                }
            }

            List<ComponentState> result = new ArrayList<>(map.values());
            result.sort((s1, s2) -> s1.getSortingKey().compareTo(s2.getSortingKey()));

            for (ComponentState mergedComponentState : result) {
                total++;

                if ("config_repository".equals(mergedComponentState.getName())) {
                    configRepoComponentState = mergedComponentState;
                }

                PartsStats partsStats = mergedComponentState.updateStateFromParts();

                if (partsStats.getFailed() >= partsStats.getMandatory()) {
                    mergedComponentState.setMessage("Initialization failed for all nodes");
                    completelyFailed++;
                } else if (partsStats.getFailed() == 1) {
                    ComponentState failed = mergedComponentState.findPart((s) -> s.isFailed());
                    mergedComponentState.setMessage("Initialization failed for node " + failed.getNodeId());
                    partiallyFailedMessages.add("Initialization failed for node " + failed.getNodeId());
                    partiallyFailed++;
                } else if (partsStats.getFailed() != 0) {
                    partiallyFailed++;
                    mergedComponentState.setMessage("Initialization failed for " + partsStats.getFailed() + " nodes");
                    partiallyFailedMessages.add("Initialization failed for " + partsStats.getFailed() + " nodes");
                }

                Map<String, Long> countedVersions = mergedComponentState.getParts().stream()
                        .collect(groupingBy((state) -> state.getJarVersion() != null ? state.getJarVersion() : "unknown", counting()));

                if (countedVersions.size() > 1) {
                    versionMismatch++;
                    Optional<Map.Entry<String, Long>> maxEntry = countedVersions.entrySet().stream().max(comparing(Map.Entry::getValue));
                    String maxVersion = maxEntry.get().getKey();

                    Multimap<String, String> versionToNodeMap = ArrayListMultimap.create();

                    for (ComponentState part : mergedComponentState.getParts()) {
                        versionToNodeMap.put(part.getJarVersion(), part.getNodeName());
                    }

                    if (countedVersions.size() == 2 && versionToNodeMap.size() == 1) {
                        Map.Entry<String, String> other = versionToNodeMap.entries().iterator().next();
                        mergedComponentState.addDetail(
                                "Version mismatch: Most nodes use " + maxVersion + "; however, node " + other.getValue() + " uses " + other.getKey());
                        versionMismatchMessages.add(
                                "Version mismatch: Most nodes use " + maxVersion + "; however, node " + other.getValue() + " uses " + other.getKey());
                    } else if (countedVersions.size() == 2) {
                        String otherVersion = versionToNodeMap.entries().iterator().next().getKey();
                        Collection<String> nodes = versionToNodeMap.get(otherVersion);

                        if (nodes.size() <= 4) {
                            mergedComponentState.addDetail("Version mismatch: Most nodes use " + maxVersion + "; however, version " + otherVersion
                                    + " is used by nodes " + String.join(", ", nodes));
                            versionMismatchMessages.add("Version mismatch: Most nodes use " + maxVersion + "; however, version " + otherVersion
                                    + " is used by nodes " + String.join(", ", nodes));
                        } else {
                            mergedComponentState.addDetail("Version mismatch: Most nodes use " + maxVersion + "; however, version " + otherVersion
                                    + " is used by " + nodes.size() + "nodes. See below for details.");
                            versionMismatchMessages.add("Version mismatch: Most nodes use " + maxVersion + "; however, version " + otherVersion
                                    + " is used by " + nodes.size() + "nodes. See below for details.");
                        }
                    } else {
                        mergedComponentState.addDetail("Version mismatch: Most nodes use " + maxVersion + "; however, " + countedVersions.size()
                                + " other versions are also in use. See below for details.");
                        versionMismatchMessages.add("Version mismatch: Most nodes use " + maxVersion + "; however, " + countedVersions.size()
                                + " other versions are also in use. See below for details.");
                    }
                }

                Instant minStart = mergedComponentState.getMinStartForInitializingState();

                if (minStart != null && minStart.plus(5, ChronoUnit.MINUTES).isBefore(Instant.now())) {
                    long diff = (Instant.now().toEpochMilli() - minStart.toEpochMilli()) / 1000 / 60;
                    mergedComponentState.addDetail("A component is in state 'initializing' since " + diff + " minutes. See below for details.");
                }
            }

            this.mergedComponentState = result;

            if (configRepoComponentState != null && !configRepoComponentState.isInitialized()) {
                if (configRepoComponentState.isFailed()) {
                    message = "Search Guard configuration could not be initialized";
                } else {
                    message = "Search Guard configuration has not been initialized, yet";
                }
                health = Health.RED;
            } else if (total == 0) {
                message = "No components found";
                health = Health.RED;
            } else if (completelyFailed == 0 && partiallyFailed == 0) {
                health = Health.GREEN;
            } else if (completelyFailed == total) {
                message = "All components have failed on all nodes";
                health = Health.RED;
            } else if (completelyFailed > 0) {
                message = "Some components have failed on all nodes; see below for details";
                health = Health.YELLOW;
            } else if (partiallyFailed > 0) {
                if (partiallyFailedMessages.size() == 1) {
                    message = partiallyFailedMessages.iterator().next() + "; see below for details";
                } else {
                    message = "Components have failed on various nodes; see below for detail";
                }

                health = Health.YELLOW;
            } else if (versionMismatch > 0) {
                if (versionMismatchMessages.size() == 1) {
                    message = versionMismatchMessages.iterator().next();
                } else {
                    message = "Several version mismatches were detected; see below for details";
                }

                health = Health.YELLOW;
            }
        }

        @Override
        public Map<String, Object> toBasicObject() {
            return OrderedImmutableMap.ofNonNull("health", getHealth(), "message", getMessage(), "components", getMergedComponentState());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.value(toDeepBasicObject());
            return builder;
        }

        public RestStatus status() {
            if (getMergedComponentState().size() != 0) {
                return RestStatus.OK;
            } else {
                return RestStatus.NOT_FOUND;
            }
        }

        public Health getHealth() {
            initMergedComponentState();

            return health;
        }

    }

    public static class NodeResponse extends BaseNodeResponse {

        private String message;
        private String detailJson;
        private List<ComponentState> states;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            message = in.readOptionalString();
            detailJson = in.readOptionalString();

            try {
                List<DocNode> stateNodes = DocNode.parse(Format.SMILE).from(in.readByteArray()).toListOfNodes();
                ArrayList<ComponentState> states = new ArrayList<>(stateNodes.size());

                for (DocNode stateNode : stateNodes) {
                    try {
                        states.add(new ComponentState(stateNode));
                    } catch (Exception e) {
                        log.error("Error while parsing state " + stateNode, e);
                    }
                }
                
                this.states = states;
            } catch (Exception e) {
                log.error("Error while parsing states", e);
                this.states = Collections.emptyList();
            }
        }

        public NodeResponse(DiscoveryNode node, List<ComponentState> states, String message, String detailJson) {
            super(node);
            this.states = states;
            this.message = message;
            this.detailJson = detailJson;
        }

        public NodeResponse(DiscoveryNode node, List<ComponentState> states) {
            this(node, states, null, null);
        }

        public String getMessage() {
            return message;
        }

        public String getDetailJson() {
            return detailJson;
        }

        public List<ComponentState> getStates() {
            return states;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(message);
            out.writeOptionalString(detailJson);
            out.writeByteArray(DocWriter.smile().writeAsBytes(states));
        }
    }

    public static enum Health {
        GREEN, YELLOW, RED;
    }

    public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse> {

        private SearchGuardModulesRegistry modulesRegistry;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                ActionFilters actionFilters, SearchGuardModulesRegistry modulesRegistry) {
            super(GetComponentStateAction.NAME, threadPool, clusterService, transportService, actionFilters, Request::new, NodeRequest::new,
                    threadPool.executor(ThreadPool.Names.MANAGEMENT));
            this.modulesRegistry = modulesRegistry;
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
            if (request.request.moduleId != null && !request.request.moduleId.equals("_all")) {
                ComponentState componentState = modulesRegistry.getComponentState(request.request.moduleId);

                if (componentState != null) {
                    return new NodeResponse(clusterService.localNode(), Collections.singletonList(componentState));
                } else {
                    return new NodeResponse(clusterService.localNode(), Collections.emptyList());
                }

            } else {
                return new NodeResponse(clusterService.localNode(), modulesRegistry.getComponentStates());
            }
        }

        @Override
        protected NodeRequest newNodeRequest(Request request) {
            return new NodeRequest(request);
        }

    }

}
