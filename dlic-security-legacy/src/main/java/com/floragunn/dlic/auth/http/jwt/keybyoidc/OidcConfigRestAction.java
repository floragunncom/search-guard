/*
 * Copyright 2016-2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.jwt.keybyoidc;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.searchsupport.rest.Responses;
import com.google.common.collect.ImmutableList;

@Deprecated
public class OidcConfigRestAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "OIDC Configuration";
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/auth_domain/{authdomain}/openid/{endpoint}"),
                new Route(POST, "/_searchguard/auth_domain/{authdomain}/openid/{endpoint}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        request.param("authdomain");
        request.param("endpoint");

        return channel -> {
            Responses.sendError(channel, RestStatus.BAD_REQUEST, "Request could not be handled");
        };
    }

}
