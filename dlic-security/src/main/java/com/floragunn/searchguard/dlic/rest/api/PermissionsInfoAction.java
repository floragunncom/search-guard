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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableList;

/**
 * Provides the evaluated REST API permissions for the currently logged in user 
 */
public class PermissionsInfoAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(PermissionsInfoAction.class);

    private final RestApiPrivilegesEvaluator restApiPrivilegesEvaluator;
    private final ThreadPool threadPool;
    private final AuthorizationService authorizationService;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;

    protected PermissionsInfoAction(final Settings settings, final AdminDNs adminDNs, AuthorizationService authorizationService,
                                    SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool) {
        super();
        this.threadPool = threadPool;
        this.authorizationService = authorizationService;
        this.restApiPrivilegesEvaluator = new RestApiPrivilegesEvaluator(settings, adminDNs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool);
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
                RestResponse response = null;

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
                    response = new RestResponse(RestStatus.OK, builder);
                } catch (final Exception e1) {
                    log.warn("Permission info action failed", e1);
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

}
