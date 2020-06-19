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

import java.nio.file.Path;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.rest.Endpoint;
import com.floragunn.searchguard.rest.validation.TenantValidator;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;

public class TenantsApiAction extends PatchableResourceApiAction {

    @Inject
    public TenantsApiAction(final Settings settings, final Path configPath,
                            final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs, final PrincipalExtractor principalExtractor,
                            final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog, AdminDNs adminDns, ThreadContext threadContext) {
        super(settings, configPath, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog, adminDns, threadContext);
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
    protected CType getConfigName() {
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
