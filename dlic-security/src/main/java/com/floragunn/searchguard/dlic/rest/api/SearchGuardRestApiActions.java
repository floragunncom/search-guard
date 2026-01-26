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

import com.floragunn.searchguard.configuration.validation.ConfigModificationValidators;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;

public class SearchGuardRestApiActions {

    public static Collection<RestHandler> getHandler(Settings settings, Path configPath, RestController controller, Client client, AdminDNs adminDns,
                                                     ConfigurationRepository cr, StaticSgConfig staticSgConfig, ClusterService cs, PrincipalExtractor principalExtractor,
                                                     AuthorizationService authorizationService,
                                                     SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
                                                     AuditLog auditLog, ConfigModificationValidators configModificationValidators) {
        final List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new InternalUsersApiAction(settings, client, adminDns, cr, staticSgConfig, cs,
                authorizationService, specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));
        handlers.add(new RolesMappingApiAction(settings, client, adminDns, cr, staticSgConfig, cs,
                authorizationService, specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));
        handlers.add(new RolesApiAction(settings, client, adminDns, cr, staticSgConfig, cs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));
        handlers.add(new ActionGroupsApiAction(settings, client, adminDns, cr, staticSgConfig, cs,
                authorizationService, specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));
        handlers.add(new FlushCacheApiAction(settings, client, adminDns, cr, staticSgConfig, cs,
                authorizationService, specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));
        handlers.add(new LicenseApiAction(settings, client, adminDns, cr, staticSgConfig, cs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));
        handlers.add(new PermissionsInfoAction(settings, adminDns, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool));
        handlers.add(new TenantsApiAction(settings, client, adminDns, cr, staticSgConfig, cs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));
        handlers.add(new BlocksApiAction(settings, client, adminDns, cr, staticSgConfig, cs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators));

        return Collections.unmodifiableCollection(handlers);
    }
}
