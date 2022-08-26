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
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper;
import com.floragunn.searchguard.ssl.util.SSLRequestHelper.SSLInfo;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class SearchGuardWhoAmIAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final AdminDNs adminDns;
    private final Settings settings;
    private final Path configPath;
    private final PrincipalExtractor principalExtractor;
    private final List<String> nodesDn ;
	
    public SearchGuardWhoAmIAction(final Settings settings, final AdminDNs adminDns,
                                   Path configPath, PrincipalExtractor principalExtractor) {
        super();
        this.adminDns = adminDns;
        this.settings = settings;
        this.configPath = configPath;
        this.principalExtractor = principalExtractor;
        
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
            RestResponse response = null;

            try {

                SSLInfo sslInfo = SSLRequestHelper.getSSLInfo(settings, configPath, request, principalExtractor);

                if(sslInfo  == null) {
                    response = new RestResponse(RestStatus.FORBIDDEN, "");
                } else {

                    final String dn = sslInfo.getPrincipal();
                    final boolean isAdmin = adminDns.isAdminDN(dn);
                    final boolean isNodeCertificateRequest = WildcardMatcher.matchAny(nodesDn, new String[] {dn}, true);

                    builder.startObject();
                    builder.field("dn", dn);
                    builder.field("is_admin", isAdmin);
                    builder.field("is_node_certificate_request", isNodeCertificateRequest);
                    builder.endObject();

                    response = new RestResponse(RestStatus.OK, builder);

                }
            } catch (final Exception e1) {
                log.error(e1.toString(), e1);
                builder = channel.newBuilder(); //NOSONAR
                builder.startObject();
                builder.field("error", e1.toString());
                builder.endObject();
                response = new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
            } finally {
                if (builder != null) {
                    builder.close();
                }
            }

            channel.sendResponse(response);
        };
    }

    @Override
    public String getName() {
        return "Search Guard Who am i";
    }

}
