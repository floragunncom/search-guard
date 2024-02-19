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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestData;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.SimpleRestHandler;
import com.floragunn.searchsupport.action.Action;

public class DlsFlsPluginApiTest {
    static final TestData RESTRICTED_TEST_DATA = TestData.documentCount(100).get();
    static final TestData UNRESTRICTED_TEST_DATA = TestData.documentCount(100).seed(234).get();

    static final String RESTRICTED_INDEX = "restricted_index";
    static final String UNRESTRICTED_INDEX = "unrestricted_index";

    static final TestSgConfig.User ADMIN = new TestSgConfig.User("admin")
            .roles(new Role("all_access").indexPermissions("*").on("*").clusterPermissions("*"));

    static final TestSgConfig.User DEPT_A_USER = new TestSgConfig.User("dept_a")
            .roles(new Role("dept_a").indexPermissions("SGS_READ").dls(DocNode.of("prefix.dept.value", "dept_a")).on(RESTRICTED_INDEX)
                    .indexPermissions("SGS_READ").on(UNRESTRICTED_INDEX).clusterPermissions("*"));

    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls().useImpl("flx").metrics("detailed");

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().enterpriseModulesEnabled().singleNode().sslEnabled()
            .nodeSettings("searchguard.dlsfls.plugin_api.enabled", true).users(ADMIN, DEPT_A_USER).dlsFls(DLSFLS).plugin(TestPlugin.class).build();

    @BeforeClass
    public static void setupTestData() {
        try (Client client = cluster.getInternalNodeClient()) {
            RESTRICTED_TEST_DATA.createIndex(client, RESTRICTED_INDEX, Settings.builder().put("index.number_of_shards", 5).build());
            UNRESTRICTED_TEST_DATA.createIndex(client, UNRESTRICTED_INDEX, Settings.builder().put("index.number_of_shards", 5).build());
        }
    }

    @Test
    public void getDlsRestriction() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(DEPT_A_USER)) {
            GenericRestClient.HttpResponse response = client.get("/test/dls/restricted_index");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(
                    DocNode.of("unrestricted", false, "queries", DocNode.array("{\"prefix\":{\"dept\":{\"value\":\"dept_a\"}}}")).toDeepBasicObject(),
                    response.getBodyAsDocNode().toDeepBasicObject());
        }

        try (GenericRestClient client = cluster.getRestClient(ADMIN)) {
            GenericRestClient.HttpResponse response = client.get("/test/dls/restricted_index");
            Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
            Assert.assertEquals(DocNode.of("unrestricted", true, "queries", DocNode.array()).toDeepBasicObject(),
                    response.getBodyAsDocNode().toDeepBasicObject());
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin {

        Settings settings;
        ThreadPool threadPool;

        public TestPlugin(Settings settings, Path configPath) {
            this.settings = settings;
        }

        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(new ActionHandler<>(TestTransportAction.TYPE, TestTransportAction.class));
        }

        public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<DiscoveryNodes> nodesInCluster) {
            return Arrays.asList(new SimpleRestHandler<>(new Route(Method.GET, "/test/dls/{index}"), TestTransportAction.TYPE,
                    (request) -> new TestActionRequest("", request.param("index"))));
        }

        public static class TestTransportAction extends HandledTransportAction<TestActionRequest, TestActionResponse> {
            static ActionType<TestActionResponse> TYPE = new ActionType<>("indices:data/dlsflstest", TestActionResponse::new);

            private final DlsFlsPluginApi dlsFlsPluginApi;

            @Inject
            public TestTransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                    DlsFlsPluginApi dlsFlsPluginApi) {

                super(TYPE.name(), transportService, actionFilters, TestActionRequest::new);
                this.dlsFlsPluginApi = dlsFlsPluginApi;
            }

            @Override
            protected void doExecute(Task task, TestActionRequest request, ActionListener<TestActionResponse> listener) {
                try {
                    PrivilegesEvaluationContext context = dlsFlsPluginApi.getCurrentPrivilegeEvaluationContext();

                    DlsRestriction dlsRestriction = dlsFlsPluginApi.getDlsRestriction(context, request.getIndex());

                    listener.onResponse(new TestActionResponse(dlsRestriction.isUnrestricted(), dlsRestriction.getQueries()));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }

        public static class TestActionRequest extends ActionRequest {

            private String id;
            private String index;

            public TestActionRequest(String id, String index) {
                this.id = id;
                this.index = index;
            }

            public TestActionRequest(StreamInput in) throws IOException {
                id = in.readString();
                index = in.readString();
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

            public String getIndex() {
                return index;
            }

        }

        public static class TestActionResponse extends Action.Response {

            private boolean unrestricted;
            private List<com.floragunn.searchsupport.queries.Query> queries;

            public TestActionResponse(boolean unrestricted, List<com.floragunn.searchsupport.queries.Query> queries) {
                this.unrestricted = unrestricted;
                this.queries = queries;
            }

            public TestActionResponse(StreamInput in) throws IOException {

            }

            @Override
            public Object toBasicObject() {
                return DocNode.of("unrestricted", unrestricted, "queries", queries).toDeepBasicObject();
            }

        }

    }

}
