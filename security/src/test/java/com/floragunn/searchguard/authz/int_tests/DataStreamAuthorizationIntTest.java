/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchguard.authz.int_tests;

import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;

public class DataStreamAuthorizationIntTest {

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A").roles(//
            new Role("limited_user_a_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("a*", "za*")
                    .dataStreamPermissions("*").on("a*", "ds_a*"));

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user").roles(//
            new Role("unlimited_user_role").clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO").indexPermissions("SGS_CRUD").on("*")
                    .dataStreamPermissions("*").on("*"));

    static TestIndex index_a1 = TestIndex.name("a1").documentCount(10).seed(1).attr("prefix", "a").build();
    static TestIndex index_a2 = TestIndex.name("a2").documentCount(10).seed(2).attr("prefix", "a").build();
    static TestIndex index_a3 = TestIndex.name("a3").documentCount(10).seed(3).attr("prefix", "a").build();
    static TestIndex index_b1 = TestIndex.name("b1").documentCount(10).seed(4).attr("prefix", "b").build();
    static TestIndex index_b2 = TestIndex.name("b2").documentCount(10).seed(5).attr("prefix", "b").build();
    static TestIndex index_b3 = TestIndex.name("b3").documentCount(10).seed(6).attr("prefix", "b").build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(LIMITED_USER_A, UNLIMITED_USER)//
            .indices(index_a1, index_a2, index_a3, index_b1, index_b2, index_b3)//
            .plugin(MockActionPlugin.class).build();

    @Test
    public void create() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(LIMITED_USER_A)) {
            HttpResponse httpResponse = restClient.put("/_data_stream/x");
            assertThat(httpResponse, isForbidden());

            httpResponse = restClient.put("/_data_stream/a_ds1");
            assertThat(httpResponse, isOk());

            httpResponse = restClient.put("/_data_stream/ds_a_ds1");
            assertThat(httpResponse, isOk());
        }
    }

    public static class MockActionPlugin extends Plugin implements ActionPlugin {

        Settings settings;
        ThreadPool threadPool;

        public MockActionPlugin(final Settings settings, final Path configPath) {
            this.settings = settings;
        }

        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(new ActionHandler<>(MockDataStreamApi.CreateAction.INSTANCE, MockDataStreamApi.CreateAction.Handler.class));
        }

        public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<DiscoveryNodes> nodesInCluster) {
            return ImmutableList.of(MockDataStreamApi.REST_API);
        }
    }

    public static class MockDataStreamApi {

        public static final RestApi REST_API = new RestApi()//
                .handlesPut("/_data_stream/{name}").with(CreateAction.INSTANCE, (params, body) -> new CreateAction.Request(params.get("name")))//
                .name("/_data_stream");

        public static class CreateAction extends Action<CreateAction.Request, StandardResponse> {

            public static final CreateAction INSTANCE = new CreateAction();
            public static final String NAME = "indices:admin/data_stream/create";

            protected CreateAction() {
                super(NAME, Request::new, StandardResponse::new);
            }

            public static class Request extends Action.Request implements IndicesRequest {

                private final String dataStream;

                public Request(String dataStream) {
                    super();
                    this.dataStream = dataStream;
                }

                public Request(UnparsedMessage message) throws ConfigValidationException {
                    this.dataStream = message.requiredDocNode().getAsString("data_stream");
                }

                @Override
                public Object toBasicObject() {
                    return ImmutableSet.of("data_stream", dataStream);
                }

                @Override
                public String[] indices() {
                    return new String[] { dataStream };
                }

                @Override
                public IndicesOptions indicesOptions() {
                    return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
                }
            }

            public static class Handler extends Action.Handler<Request, StandardResponse> {

                @Inject
                public Handler(HandlerDependencies handlerDependencies) {
                    super(CreateAction.INSTANCE, handlerDependencies);

                }

                @Override
                protected CompletableFuture<StandardResponse> doExecute(Request request) {
                    return CompletableFuture.completedFuture(new StandardResponse(200));
                }

            }
        }
    }

}
