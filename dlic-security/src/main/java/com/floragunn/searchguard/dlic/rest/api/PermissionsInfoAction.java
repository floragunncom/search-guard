/*
 * Copyright 2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.dlic.rest.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;

/**
 * Provides the evaluated REST API permissions for the currently logged in user 
 */
public class PermissionsInfoAction extends BaseRestHandler {

    private final RestApiPrivilegesEvaluator restApiPrivilegesEvaluator;
    private final ThreadPool threadPool;
    private final AuthorizationService authorizationService;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;

    protected PermissionsInfoAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
            final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs, final PrincipalExtractor principalExtractor,
            AuthorizationService authorizationService,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
            AuditLog auditLog) {
        super();
        this.threadPool = threadPool;
        this.authorizationService = authorizationService;
        this.restApiPrivilegesEvaluator = new RestApiPrivilegesEvaluator(settings, adminDNs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, principalExtractor, configPath, threadPool);
        this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(Method.GET, "/_searchguard/api/permissionsinfo"));
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        switch (request.method()) {
        case GET:
            return handleGet(request, client);
        default:
            throw new IllegalArgumentException(request.method() + " not supported");
        }
    }

    private RestChannelConsumer handleGet(RestRequest request, NodeClient client) throws IOException {

        return new RestChannelConsumer() {

            @Override
            public void accept(RestChannel channel) throws Exception {
                XContentBuilder builder = channel.newBuilder(); //NOSONAR
                BytesRestResponse response = null;

                try {

                    User user = (User) threadPool.getThreadContext().getTransient(ConfigConstants.SG_USER);
                    SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = null;

                    if (specialPrivilegesEvaluationContextProviderRegistry != null) {
                        specialPrivilegesEvaluationContext = specialPrivilegesEvaluationContextProviderRegistry.provide(user, threadPool.getThreadContext());
                    }

                    TransportAddress remoteAddress;
                    Set<String> userRoles;
                    boolean hasApiAccess = true;
                    
                    if (specialPrivilegesEvaluationContext == null) {
                        remoteAddress = (TransportAddress) threadPool.getThreadContext().getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
                        userRoles = authorizationService.getMappedRoles(user, remoteAddress);
                    } else {
                        user = specialPrivilegesEvaluationContext.getUser();
                        remoteAddress = specialPrivilegesEvaluationContext.getCaller() != null ? specialPrivilegesEvaluationContext.getCaller()
                                : (TransportAddress) threadPool.getThreadContext().getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
                        userRoles = specialPrivilegesEvaluationContext.getMappedRoles();
                        hasApiAccess = specialPrivilegesEvaluationContext.isSgConfigRestApiAllowed();
                    }

                    
                    hasApiAccess = hasApiAccess && restApiPrivilegesEvaluator.currentUserHasRestApiAccess(userRoles);
                    Map<Endpoint, List<Method>> disabledEndpoints = restApiPrivilegesEvaluator.getDisabledEndpointsForCurrentUser(user,
                            userRoles);

                    builder.startObject();
                    builder.field("user", user == null ? null : user.toString());
                    builder.field("user_name", user == null ? null : user.getName()); //NOSONAR
                    builder.field("has_api_access", hasApiAccess);
                    builder.startObject("disabled_endpoints");
                    for (Entry<Endpoint, List<Method>> entry : disabledEndpoints.entrySet()) {
                        builder.field(entry.getKey().name(), entry.getValue());
                    }
                    builder.endObject();
                    builder.endObject();
                    response = new BytesRestResponse(RestStatus.OK, builder);
                } catch (final Exception e1) {
                    e1.printStackTrace();
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

}
