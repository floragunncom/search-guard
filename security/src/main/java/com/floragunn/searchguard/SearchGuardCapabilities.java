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
import java.util.concurrent.atomic.AtomicLong;
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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.RestApi;

public class SearchGuardCapabilities {
    private static final Logger log = LogManager.getLogger(SearchGuardCapabilities.class);

    /**
     * ES|QL queries are authorized by the nodes which execute them, as the indices a query reads are not known on the
     * coordinating node. Nodes of an older version do not do that, thus ES|QL must be refused while such nodes are part
     * of the cluster. See SearchGuardFilter.
     * <p>
     * <b>Upgrade order for cross cluster ES|QL:</b> capabilities are only known for the nodes of the local cluster, as
     * there is no mechanism to retrieve them from a remote cluster. The check in SearchGuardFilter therefore does not
     * cover remote clusters: a cross cluster ES|QL query which is coordinated by an upgraded cluster is accepted, while
     * the part which runs on a remote cluster is authorized by that remote cluster alone - and a remote cluster running
     * an older version of Search Guard does not authorize it at all. Remote clusters must consequently be upgraded
     * before the clusters which query them. As long as a remote cluster runs an older version, the indices which an
     * ES|QL query reads on it are not checked against the user's roles, so either the queried indices must be
     * unrestricted on that cluster, or the privilege for the top level action indices:data/read/esql must be withheld
     * on the coordinating cluster until the remote cluster is upgraded.
     */
    public static final String ESQL_DATA_NODE_AUTHZ = "esql_data_node_authz";

    /**
     * Capabilities of the Search Guard core, i.e. capabilities which are not provided by a SearchGuardModule.
     */
    static final ImmutableSet<String> CORE_CAPABILITIES = ImmutableSet.of(ESQL_DATA_NODE_AUTHZ);

    /**
     * Minimum delay between two attempts to retrieve the capabilities of nodes whose capabilities are unknown. Such an
     * attempt is triggered by hasCapability(), which is called while a request is being authorized, thus it is rate
     * limited.
     */
    private static final long RETRIEVAL_RETRY_DELAY_MILLIS = 5000;

    private final Client nodeClient;
    private final Capabilities local;
    private volatile Capabilities clusterWide;
    private final ConcurrentHashMap<String, Capabilities> byNode = new ConcurrentHashMap<>();
    private final ClusterService clusterService;
    private final AtomicLong lastRetrievalAttempt = new AtomicLong(0);

    SearchGuardCapabilities(Collection<SearchGuardModule> modules, ClusterService clusterService, Client nodeClient) {
        ImmutableSet<String> capabilities = CORE_CAPABILITIES
                .with(modules.stream().flatMap((m) -> m.getCapabilities().stream()).collect(Collectors.toSet()));
        ImmutableSet<String> uiCapabilities = ImmutableSet
                .of(modules.stream().flatMap((m) -> m.getUiCapabilities().stream()).collect(Collectors.toSet()));
        ImmutableSet<String> publicCapabilites = ImmutableSet
                .of(modules.stream().flatMap((m) -> m.getPublicCapabilities().stream()).collect(Collectors.toSet()));

        this.clusterWide = this.local = new Capabilities(SearchGuardVersion.getVersion(), capabilities, uiCapabilities, publicCapabilites);
        // The capabilities are also retrieved while a request of a user is being processed, thus the request must not be
        // authorized against that user's privileges.
        this.nodeClient = PrivilegedConfigClient.adapt(nodeClient);
        this.clusterService = clusterService;

        if (clusterService != null) {
            clusterService.addListener(clusterStateListener);
        }
    }

    /**
     * Returns true if all nodes of this cluster support the given capability. Used to decide whether a feature which
     * requires the support of all nodes may be used; thus, this returns false whenever the capabilities of a node are
     * unknown - which is the case right after the start of a node, and when the capabilities of a node could not be
     * retrieved.
     */
    public boolean hasCapability(String capability) {
        return clusterWide.hasCapability(capability) && areCapabilitiesKnownForAllNodes();
    }

    /**
     * The capabilities are retrieved from the other nodes asynchronously when they join. Until the answer of a node has
     * arrived, that node is not represented in the intersection, so the intersection alone would wrongly report the
     * capabilities of this node as cluster wide.
     */
    private boolean areCapabilitiesKnownForAllNodes() {
        ImmutableList<DiscoveryNode> unknownNodes = getNodesWithUnknownCapabilities();

        if (unknownNodes.isEmpty()) {
            return true;
        }

        if (log.isDebugEnabled()) {
            log.debug("The capabilities of the nodes {} are not known yet", unknownNodes);
        }

        retrieveCapabilitiesOfUnknownNodes(unknownNodes);

        return false;
    }

    private ImmutableList<DiscoveryNode> getNodesWithUnknownCapabilities() {
        if (clusterService == null) {
            return ImmutableList.empty();
        }

        String localNodeId = clusterService.localNode().getId();
        ImmutableList.Builder<DiscoveryNode> result = new ImmutableList.Builder<>();

        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (!localNodeId.equals(node.getId()) && !byNode.containsKey(node.getId())) {
                result.add(node);
            }
        }

        return result.build();
    }

    /**
     * Names of the nodes of this cluster whose capabilities are unknown. Only for diagnostic messages: a feature which
     * requires the support of all nodes is unavailable while this is not empty.
     */
    public ImmutableList<String> getNamesOfNodesWithUnknownCapabilities() {
        return getNodesWithUnknownCapabilities().map(DiscoveryNode::getName);
    }

    /**
     * Retrieves the capabilities of nodes which did not answer when they joined the cluster. Without this, a single
     * failed retrieval - a node which has just started might not answer yet - would leave that node unknown until the
     * next change of the cluster membership, and thus features which require the support of all nodes unavailable.
     * Rate limited, as this is reached from hasCapability() while requests are being authorized.
     */
    private void retrieveCapabilitiesOfUnknownNodes(ImmutableList<DiscoveryNode> nodes) {
        long now = System.currentTimeMillis();
        long lastAttempt = lastRetrievalAttempt.get();

        if (now - lastAttempt < RETRIEVAL_RETRY_DELAY_MILLIS || !lastRetrievalAttempt.compareAndSet(lastAttempt, now)) {
            return;
        }

        log.info("Retrieving the capabilities of the nodes {} again", nodes);

        retrieveCapabilities(nodes);
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
                retrieveCapabilities(nodesDelta.addedNodes());
            }
        }
    };

    /**
     * Retrieves the capabilities of the given nodes and updates the intersection. Nodes which answer with a failure stay
     * unknown, so that the retrieval is attempted again, see retrieveCapabilitiesOfUnknownNodes(). The only exception is
     * a node which does not know the action at all: that is an older version of Search Guard, for which the legacy
     * capabilities are assumed.
     */
    private void retrieveCapabilities(Collection<DiscoveryNode> nodes) {
        nodeClient.execute(GetCapabilitiesAction.INSTANCE, new GetCapabilitiesAction.Request(nodes),
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

    public static class GetCapabilitiesAction extends ActionType<GetCapabilitiesAction.Response> {

        public static final GetCapabilitiesAction INSTANCE = new GetCapabilitiesAction();
        public static final String NAME = "cluster:admin/searchguard/capabilities/cluster_wide/get";

        public static final RestApi REST_API = new RestApi()//
                .responseHeaders(SearchGuardVersion.header())//
                .handlesGet("/_searchguard/capabilities/cluster_wide").with(GetCapabilitiesAction.INSTANCE, (params, body) -> new Request(), Response::status)//
                .name("/_searchguard/capabilities/cluster_wide");

        protected GetCapabilitiesAction() {
            super(NAME);
        }

        public static class Request extends BaseNodesRequest {

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

        public static class NodeRequest extends AbstractTransportRequest {

            public NodeRequest(StreamInput in) throws IOException {
                super(in);
            }

            public NodeRequest(Request request) {
                super();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
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

        public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {

            private final SearchGuardCapabilities capabilities;

            @Inject
            public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                    ActionFilters actionFilters, SearchGuardCapabilities capabilities) {
                super(GetCapabilitiesAction.NAME, clusterService, transportService, actionFilters, NodeRequest::new,
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
