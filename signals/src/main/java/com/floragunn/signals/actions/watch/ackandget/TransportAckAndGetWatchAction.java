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

package com.floragunn.signals.actions.watch.ackandget;

import static com.floragunn.signals.actions.watch.ackandget.AckAndGetWatchResponse.Status.NO_SUCH_ACTION;
import static com.floragunn.signals.actions.watch.ackandget.AckAndGetWatchResponse.Status.NO_SUCH_TENANT;
import static com.floragunn.signals.actions.watch.ackandget.AckAndGetWatchResponse.Status.NO_SUCH_WATCH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
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
import com.floragunn.signals.actions.watch.ackandget.Acknowledgement.AcknowledgementReader;
import com.floragunn.signals.actions.watch.ackandget.Acknowledgement.AcknowledgementWriter;
import com.floragunn.signals.watch.common.Ack;
import com.floragunn.signals.watch.state.ActionState;
import com.floragunn.signals.watch.state.WatchState;

public class TransportAckAndGetWatchAction
        extends TransportNodesAction<AckAndGetWatchRequest, AckAndGetWatchResponse, TransportAckAndGetWatchAction.NodeRequest, TransportAckAndGetWatchAction.NodeResponse, Void> {

    private final static Logger log = LogManager.getLogger(TransportAckAndGetWatchAction.class);

    private final Signals signals;
    private final ThreadPool threadPool;

    @Inject
    public TransportAckAndGetWatchAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
            final TransportService transportService, final ActionFilters actionFilters, final Signals signals) {
        super(
            AckAndGetWatchAction.NAME, clusterService, transportService, actionFilters,
                TransportAckAndGetWatchAction.NodeRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));

        this.signals = signals;
        this.threadPool = threadPool;
    }

    @Override
    protected AckAndGetWatchResponse newResponse(AckAndGetWatchRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
        return new AckAndGetWatchResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request, Task task) {

        try {
            DiscoveryNode localNode = clusterService.localNode();
            ThreadContext threadContext = threadPool.getThreadContext();

            User user = threadContext.getTransient(ConfigConstants.SG_USER);

            if (user == null) {
                return new NodeResponse(localNode, AckAndGetWatchResponse.Status.UNAUTHORIZED, "Request did not contain user");
            }

            SignalsTenant signalsTenant = signals.getTenant(user);

            if (signalsTenant == null) {
                return new NodeResponse(localNode, NO_SUCH_TENANT, "No such tenant: " + user.getRequestedTenant());
            }

            if (request.watchId == null) {
                throw new IllegalArgumentException("request.watchId is null");
            }

            if (!signalsTenant.runsWatchLocally(request.watchId)) {
                return new NodeResponse(localNode, NO_SUCH_WATCH, "This node does not run " + request.watchId);
            }

            if (request.actionId != null) {
                String actionId = request.actionId;
                if(signalsTenant.getWatchState(request.watchId).isActionMissing(actionId)){
                    String message = String.format("Watch %s does not contain action %s", request.watchId, actionId);
                    return new NodeResponse(localNode, NO_SUCH_ACTION, message);
                }
                try {
                    if (request.ack) {
                        WatchState watchState = signalsTenant.ack(request.watchId, actionId, user);
                        ActionState actionState = watchState.getActionState(actionId);
                        Ack acked = actionState.getAcked();
                        Acknowledgement acknowledgement = new Acknowledgement(acked.getOn(), acked.getBy(), actionId);
                        return new NodeResponse(localNode, AckAndGetWatchResponse.Status.SUCCESS, "Acknowledged", acknowledgement);
                    } else {
                        signalsTenant.unack(request.watchId, actionId, user);
                        List<String> unackedActionIds = Collections.singletonList(actionId);
                        return new NodeResponse(localNode, AckAndGetWatchResponse.Status.SUCCESS, "Un-acknowledged", unackedActionIds);
                    }
                } catch (IllegalStateException e) {
                    return new NodeResponse(localNode, AckAndGetWatchResponse.Status.ILLEGAL_STATE, e.getMessage());
                }
            } else {
                if (request.ack) {
                    Map<String, Ack> ackedActions = signalsTenant.ack(request.watchId, user);

                    if (ackedActions.size() == 0) {
                        return new NodeResponse(localNode, AckAndGetWatchResponse.Status.ILLEGAL_STATE, "No actions are in an acknowlegable state");
                    } else {
                        String message = "Acknowledged: " + new ArrayList<>(ackedActions.keySet());
                        Acknowledgement[] acknowledgements = ackedActions.entrySet()
                                .stream()
                                .map(entry -> new Acknowledgement(entry.getValue().getOn(), entry.getValue().getBy(), entry.getKey()))
                                .toArray(size -> new Acknowledgement[size]);
                        return new NodeResponse(localNode, AckAndGetWatchResponse.Status.SUCCESS, message, acknowledgements);
                    }
                } else {
                    List<String> unackedActions = signalsTenant.unack(request.watchId, user);

                    if (unackedActions.size() == 0) {
                        return new NodeResponse(localNode, AckAndGetWatchResponse.Status.ILLEGAL_STATE, "No actions are in an un-acknowlegable state");
                    } else {
                        return new NodeResponse(localNode, AckAndGetWatchResponse.Status.SUCCESS, "Un-acknowledged: " + unackedActions, unackedActions);
                    }
                }
            }
        } catch (NoSuchWatchOnThisNodeException e) {
            // Note: We checked before signalsTenant.runsWatchLocally: If we get this exception anyway, this can only mean one thing:
            return new NodeResponse(clusterService.localNode(), AckAndGetWatchResponse.Status.ILLEGAL_STATE, "The watch has not been initialized yet");
        } catch (NoSuchActionException e) {
            return new NodeResponse(clusterService.localNode(), NO_SUCH_ACTION, e.getMessage());
        } catch (NotAcknowledgeableException e) {
            return new NodeResponse(clusterService.localNode(), AckAndGetWatchResponse.Status.NOT_ACKNOWLEDGEABLE, e.getMessage());
        } catch (Exception e) {
            log.error("Error while acknowledging " + request, e);
            return new NodeResponse(clusterService.localNode(), AckAndGetWatchResponse.Status.EXCEPTION, e.toString());
        }
    }

    public static class NodeRequest extends TransportRequest {

        private String watchId;
        private String actionId;
        private boolean ack;

        public NodeRequest() {
            super();
        }

        public NodeRequest(final AckAndGetWatchRequest request) {
            super();
            this.watchId = request.getWatchId();
            this.ack = request.isAck();
            this.actionId = request.getActionId();
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

        private AckAndGetWatchResponse.Status status;
        private String message;

        private Acknowledgement[] acknowledgements;
        private String[] unackedActionIds;

        public NodeResponse(final DiscoveryNode node, AckAndGetWatchResponse.Status status, String message, Acknowledgement[] acknowledgements) {
            super(node);
            this.status = status;
            this.message = message;
            this.acknowledgements = acknowledgements;
            this.unackedActionIds = new String[0];
        }

        public NodeResponse(final DiscoveryNode node, AckAndGetWatchResponse.Status status, String message, List<String> unackedActionIds) {
            super(node);
            this.status = status;
            this.message = message;
            this.acknowledgements = new Acknowledgement[0];
            this.unackedActionIds = Objects.requireNonNull(unackedActionIds, "Unacked action ids are required.").stream()
                .toArray(size -> new String[size]);
        }

        NodeResponse(final DiscoveryNode node, AckAndGetWatchResponse.Status status, String message, Acknowledgement acknowledgement) {
            super(node);
            this.status = status;
            this.message = message;
            this.acknowledgements = new Acknowledgement[]{acknowledgement};
            this.unackedActionIds = new String[0];
        }

        public NodeResponse(DiscoveryNode node, AckAndGetWatchResponse.Status status, String message){
            this(node, status, message, new Acknowledgement[0]);
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            status = in.readEnum(AckAndGetWatchResponse.Status.class);
            message = in.readOptionalString();
            this.acknowledgements = in.readArray(new AcknowledgementReader(), size -> new Acknowledgement[size]);
            this.unackedActionIds = in.readArray(stream -> stream.readString(), size -> new String[size]);
        }

        public static TransportAckAndGetWatchAction.NodeResponse readNodeResponse(StreamInput in) throws IOException {
            TransportAckAndGetWatchAction.NodeResponse result = new TransportAckAndGetWatchAction.NodeResponse(in);
            return result;
        }

        public String getMessage() {
            return message;
        }

        public Acknowledgement[] getAcknowledgements() {
            return acknowledgements;
        }

        public String[] getUnackedActionIds() {
            return unackedActionIds;
        }

        public AckAndGetWatchResponse.Status getStatus() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeEnum(status);
            out.writeOptionalString(message);
            out.writeArray(new AcknowledgementWriter(), acknowledgements);
            out.writeArray((stream, element) -> stream.writeString(element), unackedActionIds);
        }

        @Override
        public String toString() {
            return "NodeResponse [status=" + status + ", message=" + message + "]";
        }

    }

    @Override
    protected NodeRequest newNodeRequest(AckAndGetWatchRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

}
