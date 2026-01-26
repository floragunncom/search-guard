/*
 * Copyright 2016-2019 by floragunn GmbH - All rights reserved
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

import java.util.List;

import com.floragunn.searchguard.configuration.validation.ConfigModificationValidators;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.TenantValidator;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;

public class TenantsApiAction extends PatchableResourceApiAction {

    @Inject
    public TenantsApiAction(final Settings settings, final Client client, final AdminDNs adminDNs, final ConfigurationRepository cl,
                            StaticSgConfig staticSgConfig, final ClusterService cs, AuthorizationService authorizationService,
                            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
                            AuditLog auditLog, ConfigModificationValidators configModificationValidators) {
        super(settings, client, adminDNs, cl, staticSgConfig, cs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators);
    }

    @Override
    public List<Route> routes() {
        return getStandardResourceRoutes("tenants");
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.TENANTS;
    }

    @Override
    protected AbstractConfigurationValidator getValidator(final RestRequest request, BytesReference ref, Object... param) {
        return new TenantValidator(request, ref, this.settings, param);
    }

    @Override
    protected CType<Tenant> getConfigName() {
        return CType.TENANTS;
    }

    @Override
    protected String getResourceName() {
        return "tenant";
    }

    @Override
    protected void consumeParameters(final RestRequest request) {
        request.param("name");
    }

}
