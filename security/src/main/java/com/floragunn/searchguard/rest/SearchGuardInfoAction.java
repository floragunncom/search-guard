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

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;

import com.floragunn.searchguard.authz.TenantAccessMapper;
import com.floragunn.searchguard.configuration.AdminDNs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;

public class SearchGuardInfoAction extends BaseRestHandler {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final AuthorizationService authorizationService;
    private final TenantAccessMapper tenantAccessMapper;
    private final ThreadContext threadContext;
    private final ClusterService clusterService;

    private final AdminDNs adminDNs;
    
    public SearchGuardInfoAction(Settings settings, RestController controller, AuthorizationService authorizationService, TenantAccessMapper tenantAccessMapper, ThreadPool threadPool, ClusterService clusterService, AdminDNs adminDNs) {
        super();
        this.threadContext = threadPool.getThreadContext();
        this.tenantAccessMapper = tenantAccessMapper;
        this.clusterService = clusterService;
        this.authorizationService = authorizationService;
        this.adminDNs = adminDNs;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(GET, "/_searchguard/authinfo"), new Route(POST, "/_searchguard/authinfo"));
    }
    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return new RestChannelConsumer() {

            @Override
            public void accept(RestChannel channel) throws Exception {
                XContentBuilder builder = channel.newBuilder(); //NOSONAR
                RestResponse response = null;
                
                try {

                    
                    final boolean verbose = request.paramAsBoolean("verbose", false);
                    
                    final X509Certificate[] certs = threadContext.getTransient(ConfigConstants.SG_SSL_PEER_CERTIFICATES);
                    final User user = (User)threadContext.getTransient(ConfigConstants.SG_USER);
                    final TransportAddress remoteAddress = (TransportAddress) threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
                    
                    Set<String> sgRoles = ImmutableSet.empty();

                    boolean adminUser = false;
                    if(user != null) {
                         adminUser = adminDNs.isAdmin(user);
                        if (!adminUser) {
                            try {
                                sgRoles = authorizationService.getMappedRoles(user, remoteAddress);
                            } catch (Exception e) {
                                log.warn("Error while evaluating roles for user " + user, e);
                            }
                        }
                    }
                    
                    builder.startObject();
                    builder.field("user", user==null?null:user.toString());
                    builder.field("user_name", user==null?null:user.getName());
                    builder.field("user_requested_tenant", user==null?null:user.getRequestedTenant());
                    builder.field("remote_address", remoteAddress);
                    builder.field("backend_roles", user==null?null:user.getRoles());
                    builder.field("custom_attribute_names", user==null?null:user.getCustomAttributesMap().keySet());
                    builder.field("attribute_names", user==null?null:user.getStructuredAttributes().keySet());
                    builder.field("sg_roles", sgRoles);
                    builder.field("sg_tenants", user==null?null:tenantAccessMapper.mapTenantsAccess(user, adminUser, sgRoles));
                    builder.field("principal", (String)threadContext.getTransient(ConfigConstants.SG_SSL_PRINCIPAL));
                    builder.field("peer_certificates", certs != null && certs.length > 0 ? certs.length + "" : "0");
                    
                    // TODO Legacy: Remove when legacy auth support has been removed
                    String ssoLogoutUrl = (String) threadContext.getTransient(ConfigConstants.SSO_LOGOUT_URL);
                    
                    if (ssoLogoutUrl != null) {
                        builder.field("sso_logout_url", ssoLogoutUrl);
                    }
                    
                    builder.field("cluster_name", clusterService.getClusterName().value());
                    
                    if(user != null && verbose) {
                        try {
                            builder.field("size_of_user", RamUsageEstimator.humanReadableUnits(Base64Helper.serializeObject(user).length()));
                            builder.field("size_of_custom_attributes", RamUsageEstimator.humanReadableUnits(Base64Helper.serializeObject((Serializable) user.getCustomAttributesMap()).getBytes(StandardCharsets.UTF_8).length));
                            builder.field("size_of_attributes", RamUsageEstimator.humanReadableUnits(Base64Helper.serializeObject((Serializable) user.getStructuredAttributes()).getBytes(StandardCharsets.UTF_8).length));
                            builder.field("size_of_backendroles", RamUsageEstimator.humanReadableUnits(Base64Helper.serializeObject((Serializable)user.getRoles()).getBytes(StandardCharsets.UTF_8).length));
                        } catch (Throwable e) {
                            //ignore
                        }
                    }
                    
                    
                    builder.endObject();

                    response = new RestResponse(RestStatus.OK, builder);
                } catch (final Exception e1) {
                    log.error(e1.toString(),e1);
                    builder = channel.newBuilder(); //NOSONAR
                    builder.startObject();
                    builder.field("error", e1.toString());
                    builder.endObject();
                    response = new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
                } finally {
                    if(builder != null) {
                        builder.close();
                    }
                }

                channel.sendResponse(response);
            }
        };
    }
    
    @Override
    public String getName() {
        return "Search Guard Info Action";
    }
}
