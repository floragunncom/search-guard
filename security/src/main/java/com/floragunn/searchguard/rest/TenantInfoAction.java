/*
 * Copyright 2015-2018 floragunn GmbH
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

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;

public class TenantInfoAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final PrivilegesEvaluator evaluator;
    private final ThreadContext threadContext;
    private final ClusterService clusterService;
    private final AdminDNs adminDns;

    public TenantInfoAction(final Settings settings, final RestController controller, final PrivilegesEvaluator evaluator,
            final ThreadPool threadPool, final ClusterService clusterService, final AdminDNs adminDns) {
        super();
        this.threadContext = threadPool.getThreadContext();
        this.evaluator = evaluator;
        this.clusterService = clusterService;
        this.adminDns = adminDns;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/tenantinfo"), new Route(POST, "/_searchguard/tenantinfo"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return new RestChannelConsumer() {

            @Override
            public void accept(RestChannel channel) throws Exception {
                XContentBuilder builder = channel.newBuilder(); //NOSONAR
                BytesRestResponse response = null;

                try {

                    final User user = (User) threadContext.getTransient(ConfigConstants.SG_USER);

                    //only allowed for admins or the kibanaserveruser
                    if (user == null || (!user.getName().equals(evaluator.kibanaServerUsername())) && !adminDns.isAdmin(user)) {
                        response = new BytesRestResponse(RestStatus.FORBIDDEN, "");
                    } else {

                        builder.startObject();

                        final SortedMap<String, IndexAbstraction> lookup = clusterService.state().getMetadata().getIndicesLookup();
                        for (final String indexOrAlias : lookup.keySet()) {
                            final String tenant = tenantNameForIndex(indexOrAlias);
                            if (tenant != null) {
                                builder.field(indexOrAlias, tenant);
                            }
                        }

                        builder.endObject();

                        response = new BytesRestResponse(RestStatus.OK, builder);
                    }
                } catch (final Exception e1) {
                    log.error(e1.toString(), e1);
                    builder = channel.newBuilder(); //NOSONAR
                    builder.startObject();
                    builder.field("error", e1.toString());
                    builder.endObject();
                    response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
                } finally {
                    if (builder != null) {
                        builder.close();
                    }
                }

                channel.sendResponse(response);
            }
        };
    }

    private String tenantNameForIndex(String index) {
        String[] indexParts;
        if (index == null || (indexParts = index.split("_")).length != 3) {
            return null;
        }

        if (!indexParts[0].equals(evaluator.kibanaIndex())) {
            return null;
        }

        try {
            final int expectedHash = Integer.parseInt(indexParts[1]);
            final String sanitizedName = indexParts[2];

            for (String tenant : evaluator.getAllConfiguredTenantNames()) {
                if (tenant.hashCode() == expectedHash && sanitizedName.equals(tenant.toLowerCase().replaceAll("[^a-z0-9]+", ""))) {
                    return tenant;
                }
            }

            return "__private__";
        } catch (NumberFormatException e) {
            log.warn("Index " + index + " looks like a SG tenant index but we cannot parse the hashcode so we ignore it.");
            return null;
        }
    }

    @Override
    public String getName() {
        return "Tenant Info Action";
    }

}
