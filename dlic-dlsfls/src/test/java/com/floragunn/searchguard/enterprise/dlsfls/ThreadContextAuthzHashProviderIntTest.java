/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.dlsfls;

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.SimpleRestHandler;

public class ThreadContextAuthzHashProviderIntTest {

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User DLS_USER_1A = new TestSgConfig.User("dls_user_1a")
            .roles(new Role("role").indexPermissions("*").dls(DocNode.of("prefix.dept.value", "dept_a")).on("index_*").clusterPermissions("*"));

    static final TestSgConfig.User DLS_USER_1B = new TestSgConfig.User("dls_user_1b")
            .roles(new Role("role").indexPermissions("*").dls(DocNode.of("prefix.dept.value", "dept_a")).on("index_*").clusterPermissions("*"));

    static final TestSgConfig.User DLS_USER_2 = new TestSgConfig.User("dls_user_2")
            .roles(new Role("role").indexPermissions("*").dls(DocNode.of("prefix.dept.value", "dept_b")).on("index_*").clusterPermissions("*"));

    static final TestSgConfig.User DLS_USER_WITH_ATTR = new TestSgConfig.User("dls_user_with_attr").attr("dept", "dept_a").roles(
            new Role("role").indexPermissions("*").dls(DocNode.of("prefix.dept.value", "${user.attr.dept}")).on("index_*").clusterPermissions("*"));

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(TestSgConfig.Authc.DEFAULT)
            .dlsFls(new TestSgConfig.DlsFls()).users(ADMIN, DLS_USER_1A, DLS_USER_1B, DLS_USER_2, DLS_USER_WITH_ATTR)
            .nodeSettings("searchguard.dls_fls.provide_thread_context_authz_hash", true).embedded().plugin(MockPlugin.class).build();

    @Test
    public void access_unrestricted() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.post("index_1/_mock");
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("header_values.*", containsInAnyOrder("0"))));
        }
    }

    @Test
    public void access_restricted() throws Exception {
        String expectedForUser1 = "8ddcf3b2f3e5c619a3271ba7df12507002280bbf4d22aac8f17b9ab984e23f74";

        try (GenericRestClient client = cluster.getRestClient(DLS_USER_1A)) {
            GenericRestClient.HttpResponse response = client.post("index_1/_mock");
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("header_values.*", containsInAnyOrder(expectedForUser1))));
        }

        try (GenericRestClient client = cluster.getRestClient(DLS_USER_1B)) {
            GenericRestClient.HttpResponse response = client.post("index_1/_mock");
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("header_values.*", containsInAnyOrder(expectedForUser1))));
        }
        
        try (GenericRestClient client = cluster.getRestClient(DLS_USER_1B)) {
            GenericRestClient.HttpResponse response = client.post("index_2/_mock");
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("header_values.*", containsInAnyOrder(expectedForUser1))));
        }

        String expectedForUser2 = "cd5196e132bc2afc486a98911b812a79c3b3aeaba5cc9c0724458731c7f14fb0";
        
        try (GenericRestClient client = cluster.getRestClient(DLS_USER_2)) {
            GenericRestClient.HttpResponse response = client.post("index_1/_mock");
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("header_values.*", containsInAnyOrder(expectedForUser2))));
        }
        
        String expectedForAttrUser = "61c42babbd798dad279d4828afa479f1eb7cf302b4d3c59ae19a1d1f7c8bd84a";
        
        try (GenericRestClient client = cluster.getRestClient(DLS_USER_WITH_ATTR)) {
            GenericRestClient.HttpResponse response = client.post("index_1/_mock");
            assertThat(response, isOk());
            assertThat(response, json(distinctNodesAt("header_values.*", containsInAnyOrder(expectedForAttrUser))));
        }
    }

    public static class MockPlugin extends Plugin implements ActionPlugin {

        public List<ActionHandler> getActions() {
            return Arrays.asList(new ActionHandler(MockTransportAction.TYPE, MockTransportAction.class));
        }

        @Override
        public List<RestHandler> getRestHandlers(Settings settings, NamedWriteableRegistry namedWriteableRegistry, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
            return Arrays.asList(new SimpleRestHandler<>(new Route(Method.POST, "/{index}/_mock"), MockTransportAction.TYPE,
                    (request) -> new MockActionNodesRequest(request.param("index"))));
        }

        public static class MockTransportAction extends TransportNodesAction<MockActionNodesRequest, MockActionNodesResponse, MockActionRequest, MockActionResponse, MockTransportAction> {
            static ActionType<MockActionNodesResponse> TYPE = new ActionType<>("indices:mock/action");

            final ThreadContext threadContext;

            @Inject
            public MockTransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                    ActionFilters actionFilters) {
                super(TYPE.name(), clusterService, transportService, actionFilters, MockActionRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));
                this.threadContext = threadPool.getThreadContext();
            }

            @Override
            protected MockActionResponse nodeOperation(MockActionRequest request, Task task) {
                String authzHeaderValue = this.threadContext.getHeader("_sg_dls_fls_authz");
                return new MockActionResponse(clusterService.localNode(), authzHeaderValue);
            }

            @Override
            protected MockActionNodesResponse newResponse(MockActionNodesRequest nodesRequest, List<MockActionResponse> nodes, List<FailedNodeException> failures) {
                return new MockActionNodesResponse(clusterService.getClusterName(), nodes, failures);
            }

            @Override
            protected MockActionRequest newNodeRequest(MockActionNodesRequest nodesRequest) {
                return new MockActionRequest(nodesRequest.index);
            }

            @Override
            protected MockActionResponse newNodeResponse(StreamInput streamInput, DiscoveryNode discoveryNode) throws IOException {
                return new MockActionResponse(streamInput);
            }
        }

        public static class MockActionNodesRequest extends BaseNodesRequest implements IndicesRequest {

            private String index;

            public MockActionNodesRequest(String index) {
                super(new String[0]);
                this.index = index;
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            public String getIndex() {
                return index;
            }

            public void setIndex(String index) {
                this.index = index;
            }

            @Override
            public String[] indices() {
                return new String[] { index };
            }

            @Override
            public IndicesOptions indicesOptions() {
                return IndicesOptions.STRICT_NO_EXPAND_FORBID_CLOSED;
            }
        }

        public static class MockActionNodesResponse extends BaseNodesResponse<MockActionResponse> implements ToXContentObject, ChunkedToXContentObject {

            public MockActionNodesResponse( ClusterName clusterName, List<MockActionResponse> nodes, List<FailedNodeException> failures) {
                super(clusterName, nodes, failures);
            }

            public MockActionNodesResponse(StreamInput in) throws IOException {
               super(in);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("header_values");
                builder.startObject();
                for (MockActionResponse nodeResponse : getNodes()) {
                    builder.field(nodeResponse.getNode().getName(), nodeResponse.headerValue);
                }
                builder.endObject();
                builder.field("failures");
                builder.startObject();
                for (FailedNodeException failedNodeException : failures()) {
                    builder.field(failedNodeException.nodeId(), failedNodeException);
                }
                builder.endObject();
                builder.endObject();
                return builder;
            }

            public RestStatus status() {
                return RestStatus.OK;
            }

            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                ImmutableList<ToXContent> list = ImmutableList.of(this);
                return list.iterator();
            }

            @Override
            public boolean isFragment() {
                return false;
            }

            @Override
            public List<MockActionResponse> readNodesFrom(StreamInput in) throws IOException {
                return in.readCollectionAsList(MockActionResponse::new);
            }

            @Override
            public void writeNodesTo(StreamOutput out, List<MockActionResponse> nodes) throws IOException {
                out.writeCollection(nodes);
            }
        }



        public static class MockActionRequest extends ActionRequest implements IndicesRequest {

            private String index;

            public MockActionRequest(String index) {
                this.index = index;
            }

            public MockActionRequest(StreamInput in) throws IOException {
                super(in);
                index = in.readOptionalString();
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeOptionalString(index);
            }

            public String getIndex() {
                return index;
            }

            public void setIndex(String index) {
                this.index = index;
            }

            @Override
            public String[] indices() {
                return new String[] { index };
            }

            @Override
            public IndicesOptions indicesOptions() {
                return IndicesOptions.STRICT_NO_EXPAND_FORBID_CLOSED;
            }
        }

        public static class MockActionResponse extends BaseNodeResponse implements ToXContentObject, ChunkedToXContentObject {

            private String headerValue;

            public MockActionResponse(DiscoveryNode node, String headerValue) {
                super(node);
                this.headerValue = headerValue;
            }

            public MockActionResponse(StreamInput in) throws IOException {
                super(in);
                this.headerValue = in.readOptionalString();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("header_value", headerValue);
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeOptionalString(headerValue);
            }

            public RestStatus status() {
                return RestStatus.OK;
            }

            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                ImmutableList<ToXContent> list = ImmutableList.of(this);
                return list.iterator();
            }

            @Override
            public boolean isFragment() {
                return false;
            }
        }

    }

}
