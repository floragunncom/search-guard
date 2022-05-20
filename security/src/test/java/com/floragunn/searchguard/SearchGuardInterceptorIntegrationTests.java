/*
 * Copyright 2020-2022 floragunn GmbH
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.http.message.BasicHeader;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.SimpleRestHandler;

public class SearchGuardInterceptorIntegrationTests {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @Test
    public void testAllowCustomHeaders() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder().nodeSettings(ConfigConstants.SEARCHGUARD_ALLOW_CUSTOM_HEADERS, ".*").singleNode()
                .sslEnabled().plugin(MockActionPlugin.class)
                .user("header_test_user", "secret", new Role("header_test_user_role").indexPermissions("*").on("*").clusterPermissions("*")).start();
                GenericRestClient restClient = cluster.getRestClient("header_test_user", "secret")) {

            GenericRestClient.HttpResponse httpResponse = restClient.get("/_header_test", new BasicHeader("test_header_name", "test_header_value"));
            JsonNode headers = httpResponse.toJsonNode().get("headers");

            Assert.assertEquals("test_header_value", headers.get("test_header_name").textValue());
        }
    }

    public static class MockActionPlugin extends Plugin implements ActionPlugin {

        Settings settings;
        ThreadPool threadPool;

        public MockActionPlugin(final Settings settings, final Path configPath) {
            this.settings = settings;
        }

        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(new ActionHandler<>(MockTransportAction.TYPE, MockTransportAction.class));
        }

        public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<DiscoveryNodes> nodesInCluster) {
            return Arrays.asList(new SimpleRestHandler<>(new Route(Method.GET, "/_header_test"), MockTransportAction.TYPE,
                    (request) -> new MockActionRequest(request.param("id"))));
        }

        public Collection<RestHeaderDefinition> getRestHeaders() {
            return Arrays.asList(new RestHeaderDefinition("test_header_name", true));
        }
    }

    public static class MockTransportAction extends HandledTransportAction<MockActionRequest, MockActionResponse> {
        static ActionType<MockActionResponse> TYPE = new ActionType<>("cluster:admin/header_test", MockActionResponse::new);

        private ThreadPool threadPool;

        @Inject
        public MockTransportAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
                final TransportService transportService, final AdminDNs adminDNs, final ActionFilters actionFilters) {

            super(TYPE.name(), transportService, actionFilters, MockActionRequest::new);
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, MockActionRequest request, ActionListener<MockActionResponse> listener) {
            listener.onResponse(new MockActionResponse(threadPool.getThreadContext().getHeaders()));
        }
    }

    public static class MockActionRequest extends ActionRequest {

        private String id;

        public MockActionRequest(String id) {
            this.id = id;
        }

        public MockActionRequest(StreamInput in) throws IOException {
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

    public static class MockActionResponse extends ActionResponse implements StatusToXContentObject {

        private Map<String, String> headers;

        public MockActionResponse(Map<String, String> headers) {
            this.headers = headers;
        }

        public MockActionResponse(StreamInput in) throws IOException {
            this.headers = in.readMap(StreamInput::readString, StreamInput::readString);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("headers", headers);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

    }

}
