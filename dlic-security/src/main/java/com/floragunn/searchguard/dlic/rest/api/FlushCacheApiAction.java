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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
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
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.NoOpValidator;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;

public class FlushCacheApiAction extends AbstractApiAction {

	@Inject
	public FlushCacheApiAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
			final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs,
            final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog) {
		super(settings, configPath, controller, client, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog);
		controller.registerHandler(Method.DELETE, "/_searchguard/api/cache", this);
		controller.registerHandler(Method.GET, "/_searchguard/api/cache", this);
		controller.registerHandler(Method.PUT, "/_searchguard/api/cache", this);
		controller.registerHandler(Method.POST, "/_searchguard/api/cache", this);
	}

	@Override
	protected Endpoint getEndpoint() {
		return Endpoint.CACHE;
	}

	@Override
	protected void handleDelete(RestChannel channel, 
	        RestRequest request, Client client, final JsonNode content) throws IOException
			{

		client.execute(
				ConfigUpdateAction.INSTANCE,
				new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0])),
				new ActionListener<ConfigUpdateResponse>() {

					@Override
					public void onResponse(ConfigUpdateResponse ur) {
					    if(ur.hasFailures()) {
					        log.error("Cannot flush cache due to", ur.failures().get(0));
	                        internalErrorResponse(channel, "Cannot flush cache due to "+ ur.failures().get(0).getMessage()+".");
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
			            internalErrorResponse(channel, "Cannot flush cache due to "+ e.getMessage()+".");
					}

				}
		);
	}

	@Override
	protected void handlePost(RestChannel channel, final RestRequest request, final Client client, final JsonNode content)throws IOException {
		notImplemented(channel, Method.POST);
	}

	@Override
	protected void handleGet(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException{
		notImplemented(channel, Method.GET);
	}

	@Override
	protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException{
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
