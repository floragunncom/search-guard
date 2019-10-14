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

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.SgConfigValidator;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.support.ConfigConstants;

public class SgConfigAction extends PatchableResourceApiAction {
    
    private final boolean allowPutOrPatch;

	@Inject
	public SgConfigAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
			final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs,
			final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog) {
		super(settings, configPath, controller, client, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog);
		
		allowPutOrPatch = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_RESTAPI_ALLOW_SGCONFIG_MODIFICATION, false);
		
		//deprecated, will be removed with SG 8, use sg_config instead of sgconfig
		controller.registerHandler(Method.GET, "/_searchguard/api/sgconfig/", this);
		
		controller.registerHandler(Method.GET, "/_searchguard/api/sg_config/", this);
		
		if(allowPutOrPatch) {
		    
		    //deprecated, will be removed with SG 8, use sg_config instead of sgconfig
		    controller.registerHandler(Method.PUT, "/_searchguard/api/sgconfig/{name}", this);
		    controller.registerHandler(Method.PATCH, "/_searchguard/api/sgconfig/", this);
		    
		    controller.registerHandler(Method.PUT, "/_searchguard/api/sg_config/{name}", this);
            controller.registerHandler(Method.PATCH, "/_searchguard/api/sg_config/", this);
		}
	}



	@Override
	protected void handleGet(RestChannel channel, RestRequest request, Client client, final JsonNode content) throws IOException{

		final SgDynamicConfiguration<?> configuration = load(getConfigName(), true);
		
		filter(configuration);

		successResponse(channel, configuration);
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
	protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException{
        if (allowPutOrPatch) {
            
            if(!"sg_config".equals(request.param("name"))) {
                badRequestResponse(channel, "name must be sg_config");
                return;
            }
            
            super.handlePut(channel, request, client, content);
        } else {
            notImplemented(channel, Method.PUT);
        }
	}
	
	@Override
    protected void handlePost(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException{
        notImplemented(channel, Method.POST);
    }

	@Override
	protected void handleDelete(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException{
		notImplemented(channel, Method.DELETE);
	}

	@Override
	protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... param) {
		return new SgConfigValidator(request, ref, this.settings, param);
	}

    @Override
    protected CType getConfigName() {
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
