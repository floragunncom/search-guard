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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoAction;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoRequest;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoResponse;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.LicenseValidator;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.sgconf.StaticSgConfig;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.google.common.collect.ImmutableList;

public class LicenseApiAction extends AbstractApiAction {

    public final static String CONFIG_LICENSE_KEY = "searchguard.dynamic.license";

    protected LicenseApiAction(Settings settings, Path configPath, RestController controller, Client client, AdminDNs adminDNs,
            ConfigurationRepository cl, StaticSgConfig staticSgConfig, ClusterService cs, PrincipalExtractor principalExtractor,
            final PrivilegesEvaluator evaluator,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
            AuditLog auditLog) {
        super(settings, configPath, controller, client, adminDNs, cl, staticSgConfig, cs, principalExtractor, evaluator,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(Method.DELETE, "/_searchguard/api/license"), new Route(Method.GET, "/_searchguard/api/license"),
                new Route(Method.PUT, "/_searchguard/api/license"), new Route(Method.POST, "/_searchguard/api/license"));
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.LICENSE;
    }

    @Override
    protected void handleGet(RestChannel channel, RestRequest request, Client client, final JsonNode content) throws IOException {

        client.execute(LicenseInfoAction.INSTANCE, new LicenseInfoRequest(), new ActionListener<LicenseInfoResponse>() {

            @Override
            public void onFailure(final Exception e) {
                request.params().clear();
                log.error("Unable to fetch license due to", e);
                internalErrorResponse(channel, "Unable to fetch license: " + e.getMessage());
            }

            @Override
            public void onResponse(final LicenseInfoResponse ur) {
                successResponse(channel, ur);
            }
        });
    }

    @Override
    protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {
        notImplemented(channel, Method.PUT);
    }

    protected void handlePost(RestChannel channel, final RestRequest request, final Client client, final Settings.Builder additionalSettings)
            throws IOException {
        notImplemented(channel, Method.POST);
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... param) {
        return new LicenseValidator(request, ref, this.settings, param);
    }

    @Override
    protected String getResourceName() {
        // not needed
        return null;
    }

    @Override
    protected CType<?> getConfigName() {
        return CType.CONFIG;
    }

}
