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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;

public class SearchGuardRestApiActions {

	public static Collection<RestHandler> getHandler(Settings settings, Path configPath, RestController controller, Client client, 
	        AdminDNs adminDns, ConfigurationRepository cr, ClusterService cs, PrincipalExtractor principalExtractor, 
	        final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog) {
	    final List<RestHandler> handlers = new ArrayList<RestHandler>(10);
	    handlers.add(new InternalUsersApiAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new RolesMappingApiAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new RolesApiAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new ActionGroupsApiAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new FlushCacheApiAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new LicenseApiAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new SgConfigAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new PermissionsInfoAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new AuthTokenProcessorAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    handlers.add(new TenantsApiAction(settings, configPath, controller, client, adminDns, cr, cs, principalExtractor, evaluator, threadPool, auditLog));
	    return Collections.unmodifiableCollection(handlers);
	}
}
