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

import java.nio.file.Path;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.RolesMappingValidator;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;

public class RolesMappingApiAction extends PatchableResourceApiAction {

	@Inject
	public RolesMappingApiAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
			final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs,
            final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog) {
		super(settings, configPath, controller, client, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog);
		
		controller.registerHandler(Method.GET, "/_searchguard/api/rolesmapping/", this);
		controller.registerHandler(Method.GET, "/_searchguard/api/rolesmapping/{name}", this);
		controller.registerHandler(Method.DELETE, "/_searchguard/api/rolesmapping/{name}", this);
		controller.registerHandler(Method.PUT, "/_searchguard/api/rolesmapping/{name}", this);
        controller.registerHandler(Method.PATCH, "/_searchguard/api/rolesmapping/", this);		
        controller.registerHandler(Method.PATCH, "/_searchguard/api/rolesmapping/{name}", this);

	}

	@Override
	protected Endpoint getEndpoint() {
		return Endpoint.ROLESMAPPING;
	}

	@Override
	protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... param) {
		return new RolesMappingValidator(request, ref, this.settings, param);
	}

	@Override
	protected String getResourceName() {
		return "rolesmapping";
	}

    @Override
    protected CType getConfigName() {
        return CType.ROLESMAPPING;
    }

}
