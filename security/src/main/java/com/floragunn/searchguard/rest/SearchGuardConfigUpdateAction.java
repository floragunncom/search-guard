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
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper.SSLInfo;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class SearchGuardConfigUpdateAction extends BaseRestHandler {

    private final ThreadContext threadContext;
    private final AdminDNs adminDns;
    private final Settings settings;
    private final Path configPath;
    private final PrincipalExtractor principalExtractor;
	
    public SearchGuardConfigUpdateAction(final Settings settings, final ThreadPool threadPool,
                                         final AdminDNs adminDns, Path configPath, PrincipalExtractor principalExtractor) {
        super();
        this.threadContext = threadPool.getThreadContext();
        this.adminDns = adminDns;
        this.settings = settings;
        this.configPath = configPath;
        this.principalExtractor = principalExtractor;
    }
    
    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(Method.PUT, "/_searchguard/configupdate"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
    	String[] configTypes = request.paramAsStringArrayOrEmptyIfAll("config_types");
    	
    	SSLInfo sslInfo = SSLRequestHelper.getSSLInfo(settings, configPath, request, principalExtractor);
    			
		if(sslInfo  == null) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.FORBIDDEN, ""));
        }
    	
        final User user = (User) threadContext.getTransient(ConfigConstants.SG_USER);

        //only allowed for admins
        if (user == null || !adminDns.isAdmin(user)) {
        	return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.FORBIDDEN, ""));
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
