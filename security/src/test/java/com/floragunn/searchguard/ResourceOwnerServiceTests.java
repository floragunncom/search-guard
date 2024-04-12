/*
 * Copyright 2015-2022 floragunn GmbH
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch.async_search.DeleteAsyncSearchRequest;
import co.elastic.clients.elasticsearch.async_search.GetAsyncSearchRequest;
import co.elastic.clients.elasticsearch.async_search.GetAsyncSearchResponse;
import co.elastic.clients.elasticsearch.async_search.SubmitRequest;
import co.elastic.clients.elasticsearch.async_search.SubmitResponse;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.client.RestHighLevelClient;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.SimpleRestHandler;

public class ResourceOwnerServiceTests {

    private static TestSgConfig.Role ROLE_OWN_INDEX = new TestSgConfig.Role("own_index")//
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO")//
            .indexPermissions("SGS_CRUD").on("${user_name}", "${user_name}_*");

    private static TestSgConfig.User SULU = new TestSgConfig.User("sulu").roles(ROLE_OWN_INDEX);

    private static TestSgConfig.User EVIL_SULU = new TestSgConfig.User("evil_sulu").roles(ROLE_OWN_INDEX);

    private static TestSgConfig.User ADMIN = new TestSgConfig.User("admin").roles(
            new TestSgConfig.Role("admin_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "indices:searchguard:async_search/_all_owners"));

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(SULU, EVIL_SULU, ADMIN).embedded()
            .plugin(MockActionPlugin.class).build();

    @Test
    public void testAsyncSearch() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SULU)) {
            SubmitRequest request = new SubmitRequest.Builder().index( "test1", "test2")
                    .query(new MatchAllQuery.Builder().build()._toQuery())
                    .waitForCompletionTimeout(new Time.Builder().time("1ms").build())
                    .build();

            SubmitResponse<Map> response = client.getJavaClient().asyncSearch().submit(request, Map.class);

            client.getJavaClient().asyncSearch().get(new GetAsyncSearchRequest.Builder().id(response.id()).build(), Map.class);

            client.getJavaClient().asyncSearch().delete(new DeleteAsyncSearchRequest.Builder().id(response.id()).build());

            Thread.sleep(100);

            try {
                GetAsyncSearchResponse<Map> response2 = client.getJavaClient().asyncSearch().get(new GetAsyncSearchRequest.Builder().id(response.id()).build(), Map.class);
                Assert.fail(response2.toString());
            } catch (ElasticsearchException e) {
                Assert.assertEquals(e.toString(), RestStatus.NOT_FOUND.getStatus(), e.status());
            }
        }
    }

    @Test
    public void testAsyncSearchUserMismatch() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SULU);
             RestHighLevelClient client2 = cluster.getRestHighLevelClient(EVIL_SULU)) {
            SubmitRequest request = new SubmitRequest.Builder().index( "test1", "test2")
                    .query(new MatchAllQuery.Builder().build()._toQuery())
                    .waitForCompletionTimeout(new Time.Builder().time("1ms").build())
                    .build();

            SubmitResponse<Map> response = client.getJavaClient().asyncSearch().submit(request, Map.class);

            client2.getJavaClient().asyncSearch().get(new GetAsyncSearchRequest.Builder().id(response.id()).build(), Map.class);

            Assert.fail();
        } catch (ElasticsearchException e) {
            Assert.assertTrue(e.toString(), e.toString().contains("is not owned by user evil_sulu"));
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN.getStatus(), e.status());
        }
    }

    @Test
    public void testAsyncSearchUserOverride() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SULU);
                RestHighLevelClient client2 = cluster.getRestHighLevelClient(ADMIN);) {

            SubmitRequest request = new SubmitRequest.Builder().index( "test1", "test2")
                    .query(new MatchAllQuery.Builder().build()._toQuery())
                    .waitForCompletionTimeout(new Time.Builder().time("1ms").build())
                    .build();

            SubmitResponse<Map> response = client.getJavaClient().asyncSearch().submit(request, Map.class);

            client2.getJavaClient().asyncSearch().get(new GetAsyncSearchRequest.Builder().id(response.id()).build(), Map.class);

            client2.getJavaClient().asyncSearch().delete(new DeleteAsyncSearchRequest.Builder().id(response.id()).build());

            //TODO this should be ok?

        }
    }

    @Test
    public void testAsyncSearchUserMismatchForDelete() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SULU);
             RestHighLevelClient  client2 = cluster.getRestHighLevelClient(EVIL_SULU)) {

            SubmitRequest request = new SubmitRequest.Builder().index( "test1", "test2")
                    .query(new MatchAllQuery.Builder().build()._toQuery())
                    .waitForCompletionTimeout(new Time.Builder().time("1ms").build())
                    .build();

            SubmitResponse<Map> response = client.getJavaClient().asyncSearch().submit(request, Map.class);

            client2.getJavaClient().asyncSearch().get(new GetAsyncSearchRequest.Builder().id(response.id()).build(), Map.class);

            Assert.fail();
        } catch (ElasticsearchException e) {
            Assert.assertTrue(e.toString(), e.toString().contains("is not owned by user evil_sulu"));
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN.getStatus(), e.status());
        }
    }

    public static class MockActionPlugin extends Plugin implements ActionPlugin {

        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(new ActionHandler<>(MockSubmitTransportAction.TYPE, MockSubmitTransportAction.class),
                    new ActionHandler<>(MockGetTransportAction.TYPE, MockGetTransportAction.class),
                    new ActionHandler<>(MockDeleteTransportAction.TYPE, MockDeleteTransportAction.class));
        }

        @Override
        public List<RestHandler> getRestHandlers(Settings settings, NamedWriteableRegistry namedWriteableRegistry, RestController restController, ClusterSettings clusterSettings,
                                                 IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                                 Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
            return Arrays.asList(
                    new SimpleRestHandler<>(new Route(Method.POST, "/{index}/_async_search"), MockSubmitTransportAction.TYPE,
                            (request) -> new MockSubmitActionRequest(request.param("index"))),
                    new SimpleRestHandler<>(new Route(Method.GET, "/_async_search/{id}"), MockGetTransportAction.TYPE,
                            (request) -> new MockGetActionRequest(request.param("id"))),
                    new SimpleRestHandler<>(new Route(Method.DELETE, "/_async_search/{id}"), MockDeleteTransportAction.TYPE,
                            (request) -> new MockGetActionRequest(request.param("id"))));
        }
    }

    public static class MockSubmitTransportAction extends HandledTransportAction<MockSubmitActionRequest, MockActionResponse> {
        static ActionType<MockActionResponse> TYPE = new ActionType<>("indices:data/read/async_search/submit");

        @Inject
        public MockSubmitTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockSubmitActionRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));
        }

        @Override
        protected void doExecute(Task task, MockSubmitActionRequest request, ActionListener<MockActionResponse> listener) {
            listener.onResponse(new MockActionResponse(UUID.randomUUID().toString(), RestStatus.OK));
        }
    }

    public static class MockSubmitActionRequest extends ActionRequest {

        private String index;

        public MockSubmitActionRequest(String index) {
            this.index = index;
        }

        public MockSubmitActionRequest(StreamInput in) throws IOException {

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

    }

    public static class MockGetTransportAction extends HandledTransportAction<MockGetActionRequest, MockActionResponse> {
        static ActionType<MockActionResponse> TYPE = new ActionType<>("indices:data/read/async_search/get");

        @Inject
        public MockGetTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockGetActionRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));
        }

        @Override
        protected void doExecute(Task task, MockGetActionRequest request, ActionListener<MockActionResponse> listener) {
            listener.onResponse(new MockActionResponse(UUID.randomUUID().toString(), RestStatus.OK));
        }
    }

    public static class MockGetActionRequest extends ActionRequest {

        private String id;

        public MockGetActionRequest(String id) {
            this.id = id;
        }

        public MockGetActionRequest(StreamInput in) throws IOException {
            id = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getId() {
            return id;
        }

    }

    public static class MockDeleteTransportAction extends HandledTransportAction<MockGetActionRequest, AcknowledgedResponse> {
        static ActionType<AcknowledgedResponse> TYPE = new ActionType<>("indices:data/read/async_search/delete");

        @Inject
        public MockDeleteTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockGetActionRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));
        }

        @Override
        protected void doExecute(Task task, MockGetActionRequest request, ActionListener<AcknowledgedResponse> listener) {
            listener.onResponse(AcknowledgedResponse.of(true));
        }
    }

    public static class MockActionResponse extends ActionResponse implements ToXContentObject, ChunkedToXContentObject {

        private String id;
        private RestStatus restStatus;

        public MockActionResponse(String id, RestStatus restStatus) {
            this.id = id;
            this.restStatus = restStatus;
        }

        public MockActionResponse(StreamInput in) throws IOException {
            this.id = in.readOptionalString();
            this.restStatus = in.readEnum(RestStatus.class);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (id != null) {
                builder.field("id", id);
            }

            builder.field("is_partial", true);
            builder.field("is_running", true);

            builder.field("start_time_in_millis", System.currentTimeMillis());

            builder.field("expiration_time_in_millis", System.currentTimeMillis());

            builder.startObject("response");
            builder.field("took", 0);
            builder.field("timed_out", false);
            builder.field("num_reduce_phases", 0);
            builder.startObject("_shards");
            builder.field("total", 1);
            builder.field("successful", 1);
            builder.field("skipped", 0);
            builder.field("failed", 0);
            builder.endObject();

            builder.startObject("hits");
            builder.startObject("total");
            builder.field("value", 0);
            builder.field("relation", "eq");
            builder.endObject();
            builder.nullField("max_score");
            builder.startArray("hits");
            builder.endArray();
            builder.endObject();
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(id);
            out.writeEnum(restStatus);
        }

        public RestStatus status() {
            return restStatus;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
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
