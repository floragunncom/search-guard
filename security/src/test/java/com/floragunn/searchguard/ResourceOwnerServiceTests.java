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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;
import org.elasticsearch.client.asyncsearch.DeleteAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.GetAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
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
            SearchSourceBuilder searchSource = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchSource, "test1", "test2");
            request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));

            AsyncSearchResponse response = client.asyncSearch().submit(request, RequestOptions.DEFAULT);

            client.asyncSearch().get(new GetAsyncSearchRequest(response.getId()), RequestOptions.DEFAULT);

            client.asyncSearch().delete(new DeleteAsyncSearchRequest(response.getId()), RequestOptions.DEFAULT);

            Thread.sleep(100);

            try {
                AsyncSearchResponse response2 = client.asyncSearch().get(new GetAsyncSearchRequest(response.getId()), RequestOptions.DEFAULT);
                Assert.fail(response2.toString());
            } catch (ElasticsearchStatusException e) {
                Assert.assertEquals(e.toString(), RestStatus.NOT_FOUND, e.status());
            }
        }
    }

    @Test
    public void testAsyncSearchUserMismatch() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SULU);
                RestHighLevelClient client2 = cluster.getRestHighLevelClient(EVIL_SULU);) {
            SearchSourceBuilder searchSource = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchSource, "test1", "test2");
            request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));

            AsyncSearchResponse response = client.asyncSearch().submit(request, RequestOptions.DEFAULT);

            client2.asyncSearch().get(new GetAsyncSearchRequest(response.getId()), RequestOptions.DEFAULT);

            Assert.fail();
        } catch (ElasticsearchStatusException e) {
            Assert.assertTrue(e.toString(), e.toString().contains("is not owned by user evil_sulu"));
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN, e.status());
        }
    }

    @Test
    public void testAsyncSearchUserOverride() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SULU);
                RestHighLevelClient client2 = cluster.getRestHighLevelClient(ADMIN);) {
            SearchSourceBuilder searchSource = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchSource, "test1", "test2");
            request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));

            AsyncSearchResponse response = client.asyncSearch().submit(request, RequestOptions.DEFAULT);

            client2.asyncSearch().get(new GetAsyncSearchRequest(response.getId()), RequestOptions.DEFAULT);

            client2.asyncSearch().delete(new DeleteAsyncSearchRequest(response.getId()), RequestOptions.DEFAULT);

        } catch (ElasticsearchStatusException e) {
            Assert.assertTrue(e.toString(), e.toString().contains("is not owned by user evil_sulu"));
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN, e.status());
        }
    }

    @Test
    public void testAsyncSearchUserMismatchForDelete() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient(SULU);
                RestHighLevelClient client2 = cluster.getRestHighLevelClient(EVIL_SULU);) {
            SearchSourceBuilder searchSource = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchSource, "test1", "test2");
            request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));

            AsyncSearchResponse response = client.asyncSearch().submit(request, RequestOptions.DEFAULT);

            client2.asyncSearch().delete(new DeleteAsyncSearchRequest(response.getId()), RequestOptions.DEFAULT);

            Assert.fail();
        } catch (ElasticsearchStatusException e) {
            Assert.assertTrue(e.toString(), e.toString().contains("is not owned by user evil_sulu"));
            Assert.assertEquals(e.toString(), RestStatus.FORBIDDEN, e.status());
        }
    }

    public static class MockActionPlugin extends Plugin implements ActionPlugin {

        Settings settings;
        ThreadPool threadPool;

        public MockActionPlugin(final Settings settings, final Path configPath) {
            this.settings = settings;
        }

        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(new ActionHandler<>(MockSubmitTransportAction.TYPE, MockSubmitTransportAction.class),
                    new ActionHandler<>(MockGetTransportAction.TYPE, MockGetTransportAction.class),
                    new ActionHandler<>(MockDeleteTransportAction.TYPE, MockDeleteTransportAction.class));
        }

        public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<DiscoveryNodes> nodesInCluster) {
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
        static ActionType<MockActionResponse> TYPE = new ActionType<>("indices:data/read/async_search/submit", MockActionResponse::new);

        @Inject
        public MockSubmitTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockSubmitActionRequest::new);
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
        static ActionType<MockActionResponse> TYPE = new ActionType<>("indices:data/read/async_search/get", MockActionResponse::new);

        @Inject
        public MockGetTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockGetActionRequest::new);
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
        static ActionType<AcknowledgedResponse> TYPE = new ActionType<>("indices:data/read/async_search/delete", AcknowledgedResponse::readFrom);

        @Inject
        public MockDeleteTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockGetActionRequest::new);
        }

        @Override
        protected void doExecute(Task task, MockGetActionRequest request, ActionListener<AcknowledgedResponse> listener) {
            listener.onResponse(AcknowledgedResponse.of(true));
        }
    }

    public static class MockActionResponse extends ActionResponse implements StatusToXContentObject {

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
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(id);
            out.writeEnum(restStatus);
        }

        @Override
        public RestStatus status() {
            return restStatus;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

    }

}
