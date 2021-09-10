/*
 * Copyright 2021 floragunn GmbH
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
package com.floragunn.searchguard.session.api;

import java.io.IOException;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.auth.AuthInfoService;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.session.NoSuchSessionException;
import com.floragunn.searchguard.session.SessionService;
import com.floragunn.searchguard.session.SessionToken;
import com.floragunn.searchguard.user.User;

public class DeleteSessionAction extends ActionType<DeleteSessionAction.Response> {

    public static final DeleteSessionAction INSTANCE = new DeleteSessionAction();
    public static final String NAME = "cluster:admin:searchguard:session/_own/delete";

    protected DeleteSessionAction() {
        super(NAME, in -> {
            Response response = new Response(in);
            return response;
        });
    }

    public static class Request extends ActionRequest {

        private String authTokenId;

        public Request() {
            super();
        }

        public Request(String authTokenId) {
            super();
            this.authTokenId = authTokenId;

        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.authTokenId = in.readOptionalString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(authTokenId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getAuthTokenId() {
            return authTokenId;
        }

    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private String info;
        private RestStatus restStatus;
        private String error;

        public Response(String status) {
            this.info = status;
            this.restStatus = RestStatus.OK;
        }

        public Response(RestStatus restStatus, String error) {
            this.restStatus = restStatus;
            this.error = error;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.info = in.readOptionalString();
            this.restStatus = in.readEnum(RestStatus.class);
            this.error = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(info);
            out.writeEnum(this.restStatus);
            out.writeOptionalString(this.error);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (restStatus != null) {
                builder.field("status", restStatus.getStatus());
            }

            if (info != null) {
                builder.field("info", info);
            }

            if (error != null) {
                builder.field("error", error);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public RestStatus status() {
            return restStatus;
        }

    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final SessionService sessionService;
        private final AuthInfoService authInfoService;
        private final ThreadPool threadPool;

        @Inject
        public TransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters, SessionService sessionService,
                AuthInfoService authInfoService, PrivilegesEvaluator privilegesEvaluator) {
            super(DeleteSessionAction.NAME, transportService, actionFilters, Request::new);

            this.sessionService = sessionService;
            this.authInfoService = authInfoService;
            this.threadPool = threadPool;
        }

        @Override
        protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
            User user = authInfoService.getCurrentUser();

            threadPool.generic().submit(() -> {
                try {
                    String authTokenId = request.getAuthTokenId();

                    if (authTokenId == null && SessionService.USER_TYPE.equals(user.getType())) {
                        authTokenId = String.valueOf(user.getSpecialAuthzConfig());
                    }

                    if (authTokenId == null) {
                        listener.onResponse(new DeleteSessionAction.Response(RestStatus.BAD_REQUEST, "User has no active session"));
                        return;
                    }

                    SessionToken authToken = sessionService.getByIdFromIndex(authTokenId);

                    if (!user.getName().equals(authToken.getUserName())) {
                        throw new NoSuchSessionException(request.getAuthTokenId());
                    }

                    String status = sessionService.delete(user, authToken);

                    listener.onResponse(new DeleteSessionAction.Response(status));
                } catch (NoSuchSessionException e) {
                    listener.onResponse(new DeleteSessionAction.Response(RestStatus.NOT_FOUND, "No such auth token: " + request.getAuthTokenId()));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });

        }
    }

}
