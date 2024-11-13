/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

import com.floragunn.searchguard.configuration.validation.ConfigModificationValidators;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.SgConfigValidator;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.support.ConfigConstants;
import com.google.common.collect.ImmutableList;

public class SgConfigAction extends PatchableResourceApiAction {

    private final boolean allowPutOrPatch;

    @Inject
    public SgConfigAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
            final AdminDNs adminDNs, final ConfigurationRepository cl, StaticSgConfig staticSgConfig, final ClusterService cs,
            final PrincipalExtractor principalExtractor, AuthorizationService authorizationService,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
            AuditLog auditLog, ConfigModificationValidators configModificationValidators) {
        super(settings, configPath, controller, client, adminDNs, cl, staticSgConfig, cs, principalExtractor, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators);

        allowPutOrPatch = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_RESTAPI_ALLOW_SGCONFIG_MODIFICATION, false);
    }

    @Override
    public List<Route> routes() {
        //sgconfig resource name is deprecated, will be removed with SG 8, use sg_config instead of sgconfig

        if (allowPutOrPatch) {
            return ImmutableList.of(new Route(Method.GET, "/_searchguard/api/sgconfig/"), new Route(Method.GET, "/_searchguard/api/sg_config/"),
                    new Route(Method.PUT, "/_searchguard/api/sg_config/"), new Route(Method.PUT, "/_searchguard/api/sgconfig/{name}"),
                    new Route(Method.PATCH, "/_searchguard/api/sgconfig/"), new Route(Method.PUT, "/_searchguard/api/sg_config/{name}"),
                    new Route(Method.PATCH, "/_searchguard/api/sg_config/"));
        } else {
            return ImmutableList.of(new Route(Method.GET, "/_searchguard/api/sgconfig/"), new Route(Method.GET, "/_searchguard/api/sg_config/"));
        }

    }

	@Override
	protected void handleGet(RestChannel channel, RestRequest request, Client client, final DocNode content)
			throws IOException {

		try {
			SgDynamicConfiguration<?> configuration = load(getConfigName(), true);

			configuration = filter(configuration);

			successResponse(channel, configuration);
		} catch (ConfigUnavailableException e) {
			internalErrorResponse(channel, e.getMessage());
			return;
		}
	}

    @Override
    protected void handleApiRequest(RestChannel channel, RestRequest request, Client client) throws IOException {
        if (request.method() == Method.PATCH && !allowPutOrPatch) {
            notImplemented(channel, Method.PATCH);
        } else {
            super.handleApiRequest(channel, request, client);
        }
    }

    @Override
    protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final DocNode content) throws IOException {
        if (allowPutOrPatch) {
            // Consume unused name param
            request.param("name");

            super.handlePut("sg_config", channel, request, client, content);
        } else {
            notImplemented(channel, Method.PUT);
        }
    }

    @Override
    protected void handlePost(RestChannel channel, final RestRequest request, final Client client, final DocNode content) throws IOException {
        notImplemented(channel, Method.POST);
    }

    @Override
    protected void handleDelete(RestChannel channel, final RestRequest request, final Client client, final DocNode content) throws IOException {
        notImplemented(channel, Method.DELETE);
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... param) {
        return new SgConfigValidator(request, ref, this.settings, param);
    }

    @Override
    protected CType<LegacySgConfig> getConfigName() {
        return CType.CONFIG;
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.SGCONFIG;
    }

    @Override
    protected String getResourceName() {
        // not needed, no single resource
        return null;
    }

}
