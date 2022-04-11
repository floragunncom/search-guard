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

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.LicenseValidator;
import com.floragunn.searchguard.license.LicenseHelper;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.license.SearchGuardLicenseKey;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.sgconf.StaticSgConfig;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.support.SgJsonNode;
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
        return ImmutableList.of(new Route(Method.DELETE, "/_searchguard/api/license"), new Route(Method.PUT, "/_searchguard/api/license"),
                new Route(Method.POST, "/_searchguard/api/license"));
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.LICENSE;
    }

    @Override
    protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {

        String licenseString = new SgJsonNode(content).get("sg_license").asString();

        if (licenseString == null || licenseString.length() == 0) {
            badRequestResponse(channel, "License must not be null.");
            return;
        }

        // try to decode the license String as base 64, armored PGP encoded String
        String plaintextLicense;

        try {
            plaintextLicense = LicenseHelper.validateLicense(licenseString);
        } catch (Exception e) {
            log.error("Could not decode license {} due to", licenseString, e);
            badRequestResponse(channel, "License could not be decoded due to: " + e.getMessage());
            return;
        }

        SearchGuardLicense license = new SearchGuardLicense(XContentHelper.convertToMap(XContentType.JSON.xContent(), plaintextLicense, true));
        license.dynamicValidate(cs);

        // check if license is valid at all, honor unsupported switch in es.yml 
        if (!license.isValid() && !acceptInvalidLicense) {
            badRequestResponse(channel, "License invalid due to: " + String.join(",", license.getMsgs()));
            return;
        }

        ValidationResult<SearchGuardLicenseKey> key = SearchGuardLicenseKey.parse(DocNode.of("key", licenseString), null);

        try {
            SgDynamicConfiguration<SearchGuardLicenseKey> newConfig = SgDynamicConfiguration.empty(CType.LICENSE_KEY);
            newConfig.putCEntry("default", key.get());

            saveAnUpdateConfigs(client, request, CType.LICENSE_KEY, newConfig, new OnSucessActionListener<IndexResponse>(channel) {

                @Override
                public void onResponse(IndexResponse response) {
                    successResponse(channel, "License updated.");
                }
            });
        } catch (ConfigValidationException e) {
            badRequestResponse(channel, "License invalid due to: " + e.getValidationErrors());
            return;
        }
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
        return CType.LICENSE_KEY;
    }

}
