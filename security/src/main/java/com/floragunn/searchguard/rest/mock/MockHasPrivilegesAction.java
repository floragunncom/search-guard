/*
 * Copyright 2026 floragunn GmbH
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

package com.floragunn.searchguard.rest.mock;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.floragunn.searchguard.rest.mock.AllPrivilegesGrantedResponseContentBuilder.buildAllPrivilegesGrantedResponseContent;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * A mock _has_privileges endpoint that always returns true. It was created to resolve issues with using certain Kibana features.
 */
public class MockHasPrivilegesAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final ThreadContext threadContext;

    public MockHasPrivilegesAction(ThreadPool threadPool) {
        super();
        this.threadContext = threadPool.getThreadContext();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_security/user/_has_privileges"), new Route(GET, "/_security/user/{user}/_has_privileges"),
                new Route(POST, "/_security/user/_has_privileges"), new Route(POST, "/_security/user/{user}/_has_privileges"));

    }
    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String user = getUserNameFromRequestOrContext(request);
        DocNode requestBody = readRequestBody(request);
        Map<String, Object> responseContent = buildAllPrivilegesGrantedResponseContent(user, requestBody);

        return restChannel -> {
            XContentBuilder builder = restChannel.newBuilder();
            RestResponse response;
            try {
                builder.value(responseContent);

                response = new RestResponse(RestStatus.OK, builder);
            } catch (final Exception e) {
                log.error(e.toString(), e);
                builder = restChannel.newBuilder();
                builder.startObject();
                builder.field("error", e.toString());
                builder.endObject();
                response = new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
            } finally {
                if(builder != null) {
                    builder.close();
                }
            }
            restChannel.sendResponse(response);
        };
    }

    private String getUserNameFromRequestOrContext(RestRequest request) {
        String userName = request.param("user");
        if (userName == null) {
            User user = threadContext.getTransient(ConfigConstants.SG_USER);
            if (user != null) {
                userName = user.getName();
            }
        }
        return userName;
    }

    private DocNode readRequestBody(RestRequest request) throws IOException {
        try {
            if (! request.hasContent()) {
                return DocNode.EMPTY;
            }
            BytesReference content = request.requiredContent();
            XContentType contentType = request.getXContentType();
            Map<String, Object> contentMap = DocReader.format(Format.getByContentType(contentType.mediaType())).readObject(BytesReference.toBytes(content));
            return DocNode.wrap(contentMap);
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public String getName() {
        return "Mock Has Privileges Action";
    }
}
