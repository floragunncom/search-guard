/*
 * Copyright 2019-2022 floragunn GmbH
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

package com.floragunn.signals.actions.watch.ack;

import java.io.IOException;
import java.util.ArrayList;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.signals.NoSuchActionException;
import com.floragunn.signals.NoSuchWatchOnThisNodeException;
import com.floragunn.signals.NotAcknowledgeableException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;

public class TransportAckWatchAction
        extends TransportNodesAction<AckWatchRequest, AckWatchResponse, TransportAckWatchAction.NodeRequest, TransportAckWatchAction.NodeResponse> {

    private final static Logger log = LogManager.getLogger(TransportAckWatchAction.class);

    private final Signals signals;
    private final ThreadPool threadPool;

    @Inject
    public TransportAckWatchAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
            final TransportService transportService, final ActionFilters actionFilters, final Signals signals) {
        super(AckWatchAction.NAME, clusterService, transportService, actionFilters,
                TransportAckWatchAction.NodeRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));

        this.signals = signals;
        this.threadPool = threadPool;
    }

    @Override
    protected AckWatchResponse newResponse(AckWatchRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
        return new AckWatchResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request, Task task) {

        try {
            DiscoveryNode localNode = clusterService.localNode();
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                return new NodeResponse(localNode, AckWatchResponse.Status.UNAUTHORIZED, "Request did not contain user");
            }

            SignalsTenant signalsTenant = signals.getTenant(user);

            if (signalsTenant == null) {
                return new NodeResponse(localNode, AckWatchResponse.Status.NO_SUCH_TENANT, "No such tenant: " + user.getRequestedTenant());
            }

            if (request.watchId == null) {
                throw new IllegalArgumentException("request.watchId is null");
            }

            if (!signalsTenant.runsWatchLocally(request.watchId)) {
                return new NodeResponse(localNode, AckWatchResponse.Status.NO_SUCH_WATCH, "This node does not run " + request.watchId);
            }

            if (request.actionId != null) {
                try {
                    if (request.ack) {
                        signalsTenant.ack(request.watchId, request.actionId, user);
                        return new NodeResponse(localNode, AckWatchResponse.Status.SUCCESS, "Acknowledged");
                    } else {
                        signalsTenant.unack(request.watchId, request.actionId, user);
                        return new NodeResponse(localNode, AckWatchResponse.Status.SUCCESS, "Un-acknowledged");
                    }
                } catch (IllegalStateException e) {
                    return new NodeResponse(localNode, AckWatchResponse.Status.ILLEGAL_STATE, e.getMessage());
                }
            } else {
                if (request.ack) {
                    List<String> ackedActions = new ArrayList<>(signalsTenant.ack(request.watchId, user).keySet());

                    if (ackedActions.size() == 0) {
                        return new NodeResponse(localNode, AckWatchResponse.Status.ILLEGAL_STATE, "No actions are in an acknowlegable state");
                    } else {
                        return new NodeResponse(localNode, AckWatchResponse.Status.SUCCESS, "Acknowledged: " + ackedActions);
                    }
                } else {
                    List<String> unackedActions = signalsTenant.unack(request.watchId, user);

                    if (unackedActions.size() == 0) {
                        return new NodeResponse(localNode, AckWatchResponse.Status.ILLEGAL_STATE, "No actions are in an un-acknowlegable state");
                    } else {
                        return new NodeResponse(localNode, AckWatchResponse.Status.SUCCESS, "Un-acknowledged: " + unackedActions);
                    }
                }
            }
        } catch (NoSuchWatchOnThisNodeException e) {
            // Note: We checked before signalsTenant.runsWatchLocally: If we get this exception anyway, this can only mean one thing:
            return new NodeResponse(clusterService.localNode(), AckWatchResponse.Status.ILLEGAL_STATE, "The watch has not been initialized yet");
        } catch (NoSuchActionException e) {
            return new NodeResponse(clusterService.localNode(), AckWatchResponse.Status.NO_SUCH_ACTION, e.getMessage());
        } catch (NotAcknowledgeableException e) {
            return new NodeResponse(clusterService.localNode(), AckWatchResponse.Status.NOT_ACKNOWLEDGEABLE, e.getMessage());            
        } catch (Exception e) {
            log.error("Error while acknowledging " + request, e);
            return new NodeResponse(clusterService.localNode(), AckWatchResponse.Status.EXCEPTION, e.toString());
        }
    }

    public static class NodeRequest extends TransportRequest {

        private String watchId;
        private String actionId;
        private boolean ack;

        public NodeRequest(final AckWatchRequest request) {
            super();
            this.watchId = request.getWatchId();
            this.actionId = request.getActionId();
            this.ack = request.isAck();
        }

        public NodeRequest(final StreamInput in) throws IOException {
            super(in);
            this.watchId = in.readString();
            this.ack = in.readBoolean();
            this.actionId = in.readOptionalString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(watchId);
            out.writeBoolean(ack);
            out.writeOptionalString(actionId);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private AckWatchResponse.Status status;
        private String message;

        public NodeResponse(final DiscoveryNode node, AckWatchResponse.Status status, String message) {
            super(node);
            this.status = status;
            this.message = message;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            status = in.readEnum(AckWatchResponse.Status.class);
            message = in.readOptionalString();
        }

        public static TransportAckWatchAction.NodeResponse readNodeResponse(StreamInput in) throws IOException {
            TransportAckWatchAction.NodeResponse result = new TransportAckWatchAction.NodeResponse(in);
            return result;
        }

        public String getMessage() {
            return message;
        }

        public AckWatchResponse.Status getStatus() {
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

    }

    @Override
    protected NodeRequest newNodeRequest(AckWatchRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

}
