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

import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class SearchGuardWhoAmIAction extends BaseRestHandler {

    private final AdminDNs adminDns;
    private final ThreadPool threadPool;
    private final List<String> nodesDn ;
	
    public SearchGuardWhoAmIAction(final Settings settings, ThreadPool threadPool, final AdminDNs adminDns) {
        super();
        this.adminDns = adminDns;
        this.threadPool = threadPool;

        nodesDn = settings.getAsList(ConfigConstants.SEARCHGUARD_NODES_DN, Collections.emptyList());

    }
    
    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/whoami"));
    }
    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> {
            XContentBuilder builder = channel.newBuilder(); //NOSONAR

            final ThreadContext threadContext = threadPool.getThreadContext();
            String dn = threadContext.getTransient(SSLConfigConstants.SG_SSL_PRINCIPAL);

            final boolean isAdmin = adminDns.isAdminDN(dn);
            final boolean isNodeCertificateRequest = WildcardMatcher.matchAny(nodesDn, new String[] {dn}, true);

            builder.startObject();
            builder.field("dn", dn);
            builder.field("is_admin", isAdmin);
            builder.field("is_node_certificate_request", isNodeCertificateRequest);
            builder.endObject();

            builder.close();

            channel.sendResponse(new RestResponse(RestStatus.OK, builder));
        };
    }

    @Override
    public String getName() {
        return "Search Guard Who am i";
    }

}
