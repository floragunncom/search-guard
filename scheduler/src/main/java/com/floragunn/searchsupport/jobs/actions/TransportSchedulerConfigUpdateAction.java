/*
 * Copyright 2015-2019 floragunn GmbH
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

package com.floragunn.searchsupport.jobs.actions;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchsupport.jobs.core.IndexJobStateStore;

public class TransportSchedulerConfigUpdateAction extends
        TransportNodesAction<SchedulerConfigUpdateRequest, SchedulerConfigUpdateResponse, TransportSchedulerConfigUpdateAction.NodeRequest, TransportSchedulerConfigUpdateAction.NodeResponse> {

    private final static Logger log = LogManager.getLogger(TransportSchedulerConfigUpdateAction.class);

    @Inject
    public TransportSchedulerConfigUpdateAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
            final TransportService transportService, final ActionFilters actionFilters) {
        super(SchedulerConfigUpdateAction.NAME, threadPool, clusterService, transportService, actionFilters, SchedulerConfigUpdateRequest::new,
                TransportSchedulerConfigUpdateAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT);

    }

    @Override
    protected SchedulerConfigUpdateResponse newResponse(SchedulerConfigUpdateRequest request, List<NodeResponse> responses,
            List<FailedNodeException> failures) {
        return new SchedulerConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request, Task task) {
        DiscoveryNode localNode = clusterService.localNode();

        try {
            IndexJobStateStore<?> jobStore = IndexJobStateStore.getInstanceBySchedulerName(localNode.getId(), request.request.getSchedulerName());

            if (jobStore == null) {
                log.warn("A job store for scheduler name " + request.request.getSchedulerName() + " does not exist (" + localNode.getId() + ")");
                return new NodeResponse(localNode, NodeResponse.Status.NO_SUCH_JOB_STORE,
                        "A job store for scheduler name " + request.request.getSchedulerName() + " does not exist");
            }

            String nodeId = jobStore.getNodeId();

            if (nodeId != null && !localNode.getId().equals(nodeId)) {
                // This may happen if there are several nodes per JVM (e.g., unit tests)
                log.error("The scheduler with the name " + request.request.getSchedulerName() + " is not configured for this node: "
                        + localNode.getId() + " vs " + jobStore.getNodeId());

                return new NodeResponse(localNode, NodeResponse.Status.NO_SUCH_JOB_STORE,
                        "The scheduler with the name " + request.request.getSchedulerName() + " is not configured for this node: " + localNode.getId()
                                + " vs " + jobStore.getNodeId());
            }

            String status = jobStore.updateJobs();

            return new NodeResponse(localNode, NodeResponse.Status.SUCCESS, status);
        } catch (Exception e) {
            log.error("Error while updating jobs", e);
            return new NodeResponse(localNode, NodeResponse.Status.EXCEPTION, e.toString());
        }
    }

    public static class NodeRequest extends BaseNodesRequest {

        SchedulerConfigUpdateRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new SchedulerConfigUpdateRequest(in);
        }

        public NodeRequest(final SchedulerConfigUpdateRequest request) {
            super((String[]) null);
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private Status status;
        private String message;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            status = in.readEnum(Status.class);
            message = in.readOptionalString();
        }

        public NodeResponse(final DiscoveryNode node, Status status, String message) {
            super(node);
            this.status = status;
            this.message = message;
        }

        public static TransportSchedulerConfigUpdateAction.NodeResponse readNodeResponse(StreamInput in) throws IOException {
            TransportSchedulerConfigUpdateAction.NodeResponse result = new TransportSchedulerConfigUpdateAction.NodeResponse(in);
            return result;
        }

        public String getMessage() {
            return message;
        }

        public Status getStatus() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeEnum(status);
            out.writeOptionalString(message);
        }

        @Override
        public String toString() {
            return "NodeResponse [status=" + status + ", message=" + message + "]";
        }

        public static enum Status {
            SUCCESS, NO_SUCH_JOB_STORE, EXCEPTION
        }
    }

    @Override
    protected NodeRequest newNodeRequest(SchedulerConfigUpdateRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

}
