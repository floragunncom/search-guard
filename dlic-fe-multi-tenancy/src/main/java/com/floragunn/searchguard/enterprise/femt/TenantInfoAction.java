/*
 * Copyright 2017-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;

public class TenantInfoAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final FeMultiTenancyModule module;
    private final ThreadContext threadContext;
    private final ClusterService clusterService;
    private final AdminDNs adminDns;

    public TenantInfoAction(Settings settings, RestController controller, FeMultiTenancyModule module,
            ThreadPool threadPool, ClusterService clusterService, AdminDNs adminDns) {
        super();
        this.threadContext = threadPool.getThreadContext();
        this.module = module;
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
                RestResponse response = null;

                try {

                    final User user = (User) threadContext.getTransient(ConfigConstants.SG_USER);

                    //only allowed for admins or the kibanaserveruser
                    if (user == null || (!user.getName().equals(module.getConfig().getServerUsername())) && !adminDns.isAdmin(user)) {
                        response = new RestResponse(RestStatus.FORBIDDEN, "");
                    } else {

                        builder.startObject();

                        final SortedMap<String, IndexAbstraction> lookup = clusterService.state().getMetadata().getIndicesLookup();
                        // TODO add real implementation which loads information about tenants
                        builder.field("non_existing_tenant_related_index_0", "admintenant");
                        builder.field("non_existing_tenant_related_index_1", "admin_tenant");
                        builder.field("non_existing_tenant_related_index_2", "hr_tenant");
                        builder.field("non_existing_tenant_related_index_3", "r_and_d_tenant");
//                        for (final String indexOrAlias : lookup.keySet()) {
//                            final String tenant = tenantNameForIndex(indexOrAlias);
//                            if (tenant != null) {
//                                builder.field(indexOrAlias, tenant);
//                            }
//                        }

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
            }
        };
    }

    private String tenantNameForIndex(String index) {
        String[] indexParts;
        if (index == null || (indexParts = index.split("_")).length != 3) {
            return null;
        }

        if (!indexParts[0].equals(module.getConfig().getIndex())) {
            return null;
        }

        try {
            final int expectedHash = Integer.parseInt(indexParts[1]);
            final String sanitizedName = indexParts[2];

            for (String tenant : module.getTenantNames()) {
                if (tenant.hashCode() == expectedHash && sanitizedName.equals(tenant.toLowerCase().replaceAll("[^a-z0-9]+", ""))) {
                    return tenant;
                }
            }

            return "__private__";
        } catch (NumberFormatException e) {
            log.info("Index " + index + " looks like a SG tenant index but we cannot parse the hashcode so we ignore it.");
            return null;
        }
    }

    @Override
    public String getName() {
        return "Tenant Info Action";
    }

}
