package com.floragunn.aim.api.internal;

import com.floragunn.fluent.collections.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.impl.DirectSchedulerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

//TODO This is a workaround since AIM can not use com.floragunn.searchsupport.jobs.actions.CheckForExecutingTriggerAction next to signals because signals would also register the handler under the same name
public class InternalSchedulerAPI {
    public static final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> HANDLERS = ImmutableList
            .of(new ActionPlugin.ActionHandler<>(CheckExecutingTriggers.INSTANCE, CheckExecutingTriggers.Handler.class));

    private static TriggerKey readTriggerKey(StreamInput input) throws IOException {
        String keyString = input.readString();
        int dot = keyString.indexOf('.');
        return dot == -1 ? new TriggerKey(keyString) : new TriggerKey(keyString.substring(dot + 1), keyString.substring(0, dot));
    }

    private static void writeTriggerKey(StreamOutput output, TriggerKey triggerKey) throws IOException {
        String group = triggerKey.getGroup();
        if (group == null || group.isEmpty()) {
            output.writeString(triggerKey.getName());
        } else {
            output.writeString(group + "." + triggerKey.getName());
        }
    }

    public static class CheckExecutingTriggers extends ActionType<CheckExecutingTriggers.Response> {
        public static final String NAME = "cluster:admin:searchguard:aim:internal:scheduler:triggers/get";
        public static final CheckExecutingTriggers INSTANCE = new CheckExecutingTriggers();

        private CheckExecutingTriggers() {
            super(NAME);
        }

        public static class Request extends BaseNodesRequest<Request> {
            private final String schedulerName;
            private final Set<TriggerKey> triggerKeys;

            public Request(String schedulerName, Set<TriggerKey> triggerKeys) {
                super(new String[] {});
                this.schedulerName = schedulerName;
                this.triggerKeys = triggerKeys;
            }

            public Request(StreamInput in) throws IOException {
                super(in);
                schedulerName = in.readString();
                triggerKeys = in.readCollectionAsSet(InternalSchedulerAPI::readTriggerKey);
            }

            @Override
            public void writeTo(StreamOutput output) throws IOException {
                super.writeTo(output);
                output.writeString(schedulerName);
                output.writeCollection(triggerKeys, InternalSchedulerAPI::writeTriggerKey);
            }

            @Override
            public ActionRequestValidationException validate() {
                if (schedulerName == null || schedulerName.isEmpty()) {
                    return new ActionRequestValidationException();
                }
                return null;
            }

            public String getSchedulerName() {
                return schedulerName;
            }

            public Set<TriggerKey> getTriggerKeys() {
                return triggerKeys;
            }

            public static class Node extends BaseNodesRequest<Node> {
                private final Request request;

                public Node(Request request) {
                    super(new String[] {});
                    this.request = request;
                }

                public Node(StreamInput in) throws IOException {
                    super(in);
                    request = new Request(in);
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    super.writeTo(out);
                    request.writeTo(out);
                }

                public Request getRequest() {
                    return request;
                }
            }
        }

        public static class Response extends BaseNodesResponse<Response.Node> {
            public Response(ClusterName clusterName, List<Response.Node> nodes, List<FailedNodeException> failures) {
                super(clusterName, nodes, failures);
            }

            public Response(StreamInput input) throws IOException {
                super(input);
            }

            @Override
            public List<Node> readNodesFrom(StreamInput input) throws IOException {
                return input.readCollectionAsList(Node::new);
            }

            @Override
            public void writeNodesTo(StreamOutput output, List<Node> nodes) throws IOException {
                output.writeCollection(nodes);
            }

            public Set<TriggerKey> getTriggerKeys() {
                return getNodes().stream().map(Node::getTriggerKeys).flatMap(Set::stream).collect(Collectors.toSet());
            }

            public static class Node extends BaseNodeResponse {
                private final Set<TriggerKey> triggerKeys;

                public Node(DiscoveryNode node, Set<TriggerKey> triggerKeys) {
                    super(node);
                    this.triggerKeys = triggerKeys;
                }

                public Node(StreamInput in) throws IOException {
                    super(in);
                    triggerKeys = in.readCollectionAsSet(InternalSchedulerAPI::readTriggerKey);
                }

                @Override
                public void writeTo(StreamOutput output) throws IOException {
                    super.writeTo(output);
                    output.writeCollection(triggerKeys, InternalSchedulerAPI::writeTriggerKey);
                }

                public Set<TriggerKey> getTriggerKeys() {
                    return triggerKeys;
                }
            }
        }

        public static class Handler extends TransportNodesAction<Request, Response, Request.Node, Response.Node> {
            private static final Logger LOG = LogManager.getLogger(CheckExecutingTriggers.Handler.class);

            @Inject
            public Handler(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
                super(NAME, clusterService, transportService, actionFilters, Request.Node::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));
            }

            @Override
            protected Response newResponse(Request request, List<Response.Node> nodes, List<FailedNodeException> failures) {
                return new Response(clusterService.getClusterName(), nodes, failures);
            }

            @Override
            protected Request.Node newNodeRequest(Request request) {
                return new Request.Node(request);
            }

            @Override
            protected Response.Node newNodeResponse(StreamInput input, DiscoveryNode node) throws IOException {
                return new Response.Node(input);
            }

            @Override
            protected Response.Node nodeOperation(Request.Node request, Task task) {
                DiscoveryNode node = clusterService.localNode();
                try {
                    Scheduler scheduler = DirectSchedulerFactory.getInstance().getScheduler(request.getRequest().getSchedulerName());
                    if (scheduler == null) {
                        return new Response.Node(node, new HashSet<>());
                    }
                    Set<TriggerKey> result = new HashSet<>();
                    for (JobExecutionContext jobExecutionContext : scheduler.getCurrentlyExecutingJobs()) {
                        if (jobExecutionContext.getTrigger() != null
                                && request.getRequest().getTriggerKeys().contains(jobExecutionContext.getTrigger().getKey())) {
                            result.add(jobExecutionContext.getTrigger().getKey());
                        }
                    }
                    return new Response.Node(node, result);
                } catch (Exception e) {
                    LOG.error("Failed to retrieve running triggers", e);
                    return new Response.Node(node, new HashSet<>());
                }
            }
        }
    }
}
