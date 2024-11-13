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

package com.floragunn.searchguard;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchsupport.action.RestApi;

public class SearchGuardCapabilities {
    private static final Logger log = LogManager.getLogger(SearchGuardCapabilities.class);

    private final Client nodeClient;
    private final Capabilities local;
    private volatile Capabilities clusterWide;
    private final ConcurrentHashMap<String, Capabilities> byNode = new ConcurrentHashMap<>();

    SearchGuardCapabilities(Collection<SearchGuardModule> modules, ClusterService clusterService, Client nodeClient) {
        ImmutableSet<String> capabilities = ImmutableSet
                .of(modules.stream().flatMap((m) -> m.getCapabilities().stream()).collect(Collectors.toSet()));
        ImmutableSet<String> uiCapabilities = ImmutableSet
                .of(modules.stream().flatMap((m) -> m.getUiCapabilities().stream()).collect(Collectors.toSet()));
        ImmutableSet<String> publicCapabilites = ImmutableSet
                .of(modules.stream().flatMap((m) -> m.getPublicCapabilities().stream()).collect(Collectors.toSet()));

        this.clusterWide = this.local = new Capabilities(SearchGuardVersion.getVersion(), capabilities, uiCapabilities, publicCapabilites);
        this.nodeClient = nodeClient;

        if (clusterService != null) {
            clusterService.addListener(clusterStateListener);
        }
    }

    public boolean hasCapability(String capability) {
        return clusterWide.hasCapability(capability);
    }

    public ImmutableSet<String> getCapabilities() {
        return clusterWide.getCapabilities();
    }

    public ImmutableSet<String> getUiCapabilities() {
        return clusterWide.getUiCapabilities();
    }

    public ImmutableSet<String> getPublicCapabilites() {
        return clusterWide.getPublicCapabilites();
    }

    private void calculateIntersectionOfCapabilities() {
        ImmutableMap<String, Capabilities> copy = ImmutableMap.of(byNode);
        ImmutableSet<String> nodes = copy.keySet();

        if (this.local.allCapabilities.isEmpty()) {
            this.clusterWide = Capabilities.EMPTY;
            return;
        }

        if (nodes.isEmpty()) {
            // We are the only node in the cluster
            Capabilities oldClusterWide = this.clusterWide;
            Capabilities newClusterWide = this.local;
            this.clusterWide = newClusterWide;
            log.info("Updated capabilities: " + newClusterWide + "\nOld: " + oldClusterWide);
            return;
        }

        CheckTable<String, String> capabilities = !this.local.capabilities.isEmpty() ? CheckTable.create(nodes, this.local.capabilities) : null;
        CheckTable<String, String> uiCapabilities = !this.local.uiCapabilities.isEmpty() ? CheckTable.create(nodes, this.local.uiCapabilities) : null;
        CheckTable<String, String> publicCapabilities = !this.local.publicCapabilities.isEmpty()
                ? CheckTable.create(nodes, this.local.publicCapabilities)
                : null;

        Set<String> versions = new TreeSet<String>();
        versions.add(this.local.searchGuardVersion);

        for (Map.Entry<String, Capabilities> entry : copy.entrySet()) {
            versions.add(entry.getValue().searchGuardVersion != null ? entry.getValue().searchGuardVersion : "_unknown");

            if (capabilities != null) {
                capabilities.checkIf(entry.getKey(), (c) -> entry.getValue().capabilities.contains(c));
            }

            if (uiCapabilities != null) {
                uiCapabilities.checkIf(entry.getKey(), (c) -> entry.getValue().uiCapabilities.contains(c));
            }

            if (publicCapabilities != null) {
                publicCapabilities.checkIf(entry.getKey(), (c) -> entry.getValue().publicCapabilities.contains(c));
            }
        }

        Capabilities oldClusterWide = this.clusterWide;
        Capabilities newClusterWide = new Capabilities(String.join(", ", versions),
                capabilities != null ? capabilities.getCompleteColumns() : ImmutableSet.empty(),
                uiCapabilities != null ? uiCapabilities.getCompleteColumns() : ImmutableSet.empty(),
                publicCapabilities != null ? publicCapabilities.getCompleteColumns() : ImmutableSet.empty());

        if (oldClusterWide == null) {
            log.info("Initial capabilities: " + newClusterWide);
            this.clusterWide = newClusterWide;
        } else if (!oldClusterWide.equals(newClusterWide)) {
            log.info("Updated capabilities: " + newClusterWide + "\nOld: " + oldClusterWide);
            this.clusterWide = newClusterWide;
        } else {
            log.debug("Capabilities did not change");
        }
    }

    static class Capabilities implements Writeable, Document<Capabilities> {
        static final Capabilities EMPTY = new Capabilities(null, ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty());

        /**
         * Capabilities which are assumed for older versions of Search Guard which do not support the capabilities API
         */
        static final Capabilities LEGACY_ASSUMED_CAPABILITIES = new Capabilities("_unknown",
                ImmutableSet.of("auth_tokens", "fe_multi_tenancy", "signals"), ImmutableSet.empty(), ImmutableSet.empty());

        final String searchGuardVersion;
        final ImmutableSet<String> capabilities;
        final ImmutableSet<String> uiCapabilities;
        final ImmutableSet<String> publicCapabilities;
        final ImmutableSet<String> allCapabilities;

        Capabilities(String searchGuardVersion, ImmutableSet<String> capabilities, ImmutableSet<String> uiCapabilities,
                ImmutableSet<String> publicCapabilites) {
            this.searchGuardVersion = searchGuardVersion;
            this.capabilities = capabilities;
            this.uiCapabilities = uiCapabilities;
            this.publicCapabilities = publicCapabilites;
            this.allCapabilities = capabilities.with(uiCapabilities).with(publicCapabilites);
        }

        Capabilities(StreamInput in) throws IOException {
            this.searchGuardVersion = in.readOptionalString();
            this.capabilities = ImmutableSet.of(in.readCollectionAsSet(StreamInput::readString));
            this.uiCapabilities = ImmutableSet.of(in.readCollectionAsSet(StreamInput::readString));
            this.publicCapabilities = ImmutableSet.of(in.readCollectionAsSet(StreamInput::readString));
            this.allCapabilities = capabilities.with(uiCapabilities).with(publicCapabilities);
        }

        boolean hasCapability(String capability) {
            return allCapabilities.contains(capability);
        }

        ImmutableSet<String> getCapabilities() {
            return capabilities;
        }

        ImmutableSet<String> getUiCapabilities() {
            return uiCapabilities;
        }

        ImmutableSet<String> getPublicCapabilites() {
            return publicCapabilities;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(searchGuardVersion);
            out.writeCollection(capabilities, StreamOutput::writeString);
            out.writeCollection(uiCapabilities, StreamOutput::writeString);
            out.writeCollection(publicCapabilities, StreamOutput::writeString);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((capabilities == null) ? 0 : capabilities.hashCode());
            result = prime * result + ((publicCapabilities == null) ? 0 : publicCapabilities.hashCode());
            result = prime * result + ((uiCapabilities == null) ? 0 : uiCapabilities.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Capabilities)) {
                return false;
            }
            Capabilities other = (Capabilities) obj;
            return capabilities.equals(other.capabilities) && publicCapabilities.equals(other.publicCapabilities)
                    && uiCapabilities.equals(other.uiCapabilities);
        }

        @Override
        public String toString() {
            return "Capabilities [searchGuardVersion=" + searchGuardVersion + ", capabilities=" + capabilities + ", uiCapabilities=" + uiCapabilities
                    + ", publicCapabilities=" + publicCapabilities + "]";
        }

        @Override
        public Map<String, Object> toBasicObject() {
            return ImmutableMap.of("version", searchGuardVersion, "capabilities", capabilities, "ui_capabilities", uiCapabilities,
                    "public_capabilities", publicCapabilities);
        }

    }

    private final ClusterStateListener clusterStateListener = new ClusterStateListener() {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!event.nodesChanged()) {
                return;
            }

            DiscoveryNodes.Delta nodesDelta = event.nodesDelta();

            if (nodesDelta.removed()) {
                for (DiscoveryNode node : nodesDelta.removedNodes()) {
                    byNode.remove(node.getId());
                }

                calculateIntersectionOfCapabilities();
            }

            if (nodesDelta.added()) {
                nodeClient.execute(GetCapabilitiesAction.INSTANCE, new GetCapabilitiesAction.Request(nodesDelta.addedNodes()),
                        new ActionListener<GetCapabilitiesAction.Response>() {

                            @Override
                            public void onResponse(GetCapabilitiesAction.Response response) {
                                try {
                                    for (GetCapabilitiesAction.NodeResponse nodeResponse : response.getNodes()) {
                                        byNode.put(nodeResponse.getNode().getId(), nodeResponse.getCapabilities());
                                    }

                                    for (FailedNodeException failure : response.failures()) {
                                        if (failure.getCause() instanceof RemoteTransportException
                                                && failure.getCause().getCause() instanceof ActionNotFoundTransportException) {
                                            log.debug("Remote node does not know the GetCapabilitiesAction. Assuming fallback capabilities", failure);

                                            byNode.put(failure.nodeId(), Capabilities.LEGACY_ASSUMED_CAPABILITIES);
                                        } else {
                                            log.warn("Got failure from node when retrieving capabilities", failure);

                                            byNode.remove(failure.nodeId());
                                        }
                                    }

                                    calculateIntersectionOfCapabilities();
                                } catch (Exception e) {
                                    log.error("Error while updating capabilities: " + response, e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.error("Error when retrieving capabilities", e);
                            }
                        });
            }
        }
    };

    public static class GetCapabilitiesAction extends ActionType<GetCapabilitiesAction.Response> {

        public static final GetCapabilitiesAction INSTANCE = new GetCapabilitiesAction();
        public static final String NAME = "cluster:admin/searchguard/capabilities/cluster_wide/get";

        public static final RestApi REST_API = new RestApi()//
                .responseHeaders(SearchGuardVersion.header())//
                .handlesGet("/_searchguard/capabilities/cluster_wide").with(GetCapabilitiesAction.INSTANCE, (params, body) -> new Request(), Response::status)//
                .name("/_searchguard/capabilities/cluster_wide");

        protected GetCapabilitiesAction() {
            super(NAME, Response::new);
        }

        public static class Request extends BaseNodesRequest<Request> {

            Request() {
                super(new String[0]);
            }

            Request(StreamInput in) throws IOException {
                super(in);
            }

            Request(Collection<DiscoveryNode> concreteNodes) {
                super(concreteNodes.toArray(new DiscoveryNode[concreteNodes.size()]));
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {
                super.writeTo(out);
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

        public static class NodeRequest extends BaseNodesRequest {

            private Request request;

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

        public static class NodeResponse extends BaseNodeResponse implements ToXContentObject {

            private final Capabilities capabilities;

            public NodeResponse(StreamInput in) throws IOException {
                super(in);
                this.capabilities = new Capabilities(in);
            }

            public NodeResponse(DiscoveryNode node, Capabilities capabilities) {
                super(node);
                this.capabilities = capabilities;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                this.capabilities.writeTo(out);
            }

            public Capabilities getCapabilities() {
                return capabilities;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.map(capabilities.toBasicObject());
                return builder;
            }

        }

        public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse> {

            private final SearchGuardCapabilities capabilities;

            @Inject
            public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                    ActionFilters actionFilters, SearchGuardCapabilities capabilities) {
                super(GetCapabilitiesAction.NAME, threadPool, clusterService, transportService, actionFilters, Request::new, NodeRequest::new,
                        threadPool.executor(ThreadPool.Names.MANAGEMENT));

                this.capabilities = capabilities;
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
                return new NodeResponse(clusterService.localNode(), capabilities.local);
            }

            @Override
            protected NodeRequest newNodeRequest(Request request) {
                return new NodeRequest(request);
            }
        }

    }

}
