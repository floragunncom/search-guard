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

package com.floragunn.searchguard.rest;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;

public class SearchGuardConfigUpdateAction extends BaseRestHandler {

    private final ThreadContext threadContext;
    private final AdminDNs adminDns;

    public SearchGuardConfigUpdateAction(final ThreadPool threadPool, final AdminDNs adminDns) {
        super();
        this.threadContext = threadPool.getThreadContext();
        this.adminDns = adminDns;
    }
    
    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(Method.PUT, "/_searchguard/configupdate"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
    	String[] configTypes = request.paramAsStringArrayOrEmptyIfAll("config_types");

        final User user = (User) threadContext.getTransient(ConfigConstants.SG_USER);

        //only allowed for admins
        if (user == null || !adminDns.isAdmin(user)) {
        	return channel -> channel.sendResponse(new RestResponse(RestStatus.FORBIDDEN, ""));
        } else {
        	ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest(configTypes);
        	return channel -> client.execute(
                    ConfigUpdateAction.INSTANCE,
                    configUpdateRequest,
                    new NodesResponseRestListener<ConfigUpdateResponse>(channel));
        }
    }

    @Override
    public String getName() {
        return "Search Guard config update";
    }

}
