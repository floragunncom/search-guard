/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.license.legacy;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;

import com.google.common.collect.ImmutableList;

@Deprecated
public class SearchGuardLicenseAction extends BaseRestHandler {

    public SearchGuardLicenseAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/license"), new Route(POST, "/_searchguard/license"),
                new Route(Method.GET, "/_searchguard/api/license"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        LicenseInfoRequest licenseInfoRequest = new LicenseInfoRequest();
        return channel -> client.executeLocally(LicenseInfoAction.INSTANCE, licenseInfoRequest,
                new NodesResponseRestListener<LicenseInfoResponse>(channel));
    }

    @Override
    public String getName() {
        return "Search Guard License Info";
    }

}
