/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.saml;

import java.io.IOException;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;

import com.floragunn.codova.documents.Document;
import com.floragunn.searchsupport.action.Responses;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Deprecated
public class AuthTokenProcessorAction extends BaseRestHandler {

    public AuthTokenProcessorAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(Method.POST, "/_searchguard/api/authtoken"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        return channel -> {
            try {
                // Just do nothing here. Eligible authenticators will intercept calls and
                // provide own responses.
                Responses.send(channel, RestStatus.NOT_IMPLEMENTED);

            } catch (Exception e) {
                Responses.sendError(channel, e);
            }
        };
    }

    public static class Response implements Document<Response> {
        private String authorization;

        public String getAuthorization() {
            return authorization;
        }

        public void setAuthorization(String authorization) {
            this.authorization = authorization;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("authorization", authorization);
        }
    }

    @Override
    public String getName() {
        return "SAML Authtoken";
    }

}
