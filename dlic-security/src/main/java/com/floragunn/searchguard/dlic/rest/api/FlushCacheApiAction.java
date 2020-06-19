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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.rest.Endpoint;
import com.floragunn.searchguard.rest.validation.NoOpValidator;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.google.common.collect.ImmutableList;

public class FlushCacheApiAction extends EnterpriseApiAction {

    @Inject
    public FlushCacheApiAction(final Settings settings, final Path configPath,
                               final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs, final PrincipalExtractor principalExtractor,
                               final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog, AdminDNs adminDns, ThreadContext threadContext) {
        super(settings, configPath, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog, adminDns, threadContext);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(Method.DELETE, "/_searchguard/api/cache"), new Route(Method.GET, "/_searchguard/api/cache"),
                new Route(Method.PUT, "/_searchguard/api/cache"), new Route(Method.POST, "/_searchguard/api/cache"));
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.CACHE;
    }

    @Override
    protected void handleDelete(RestChannel channel, RestRequest request, Client client, final JsonNode content) throws IOException {
        client.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0])),
                new ActionListener<ConfigUpdateResponse>() {

                    @Override
                    public void onResponse(ConfigUpdateResponse ur) {
                        if (ur.hasFailures()) {
                            log.error("Cannot flush cache due to", ur.failures().get(0));
                            internalErrorResponse(channel, "Cannot flush cache due to " + ur.failures().get(0).getMessage() + ".");
                            return;
                        }
                        successResponse(channel, "Cache flushed successfully.");
                        if (log.isDebugEnabled()) {
                            log.debug("cache flushed successfully");
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Cannot flush cache due to", e);
                        internalErrorResponse(channel, "Cannot flush cache due to " + e.getMessage() + ".");
                    }

                });
    }

    @Override
    protected void handlePost(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {
        notImplemented(channel, Method.POST);
    }

    @Override
    protected void handleGet(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {
        notImplemented(channel, Method.GET);
    }

    @Override
    protected void handlePut(RestChannel channel, RestRequest request, Client client, JsonNode content) throws IOException {
        notImplemented(channel, Method.PUT);
    }

    @Override
    protected void handlePutWithName(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {
        notImplemented(channel, Method.PUT);
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... param) {
        return new NoOpValidator(request, ref, this.settings, param);
    }

    @Override
    protected String getResourceName() {
        // not needed
        return null;
    }

    @Override
    protected CType getConfigName() {
        return null;
    }

    @Override
    protected void consumeParameters(final RestRequest request) {
        // not needed
    }

}
