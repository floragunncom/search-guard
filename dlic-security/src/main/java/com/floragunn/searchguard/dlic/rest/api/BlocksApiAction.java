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
import java.util.List;

import com.floragunn.searchguard.configuration.validation.ConfigModificationValidators;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.blocking.Blocks;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.BlocksValidator;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;

public class BlocksApiAction extends PatchableResourceApiAction {

    @Inject
    public BlocksApiAction(Settings settings, final Path configPath, RestController controller, Client client, AdminDNs adminDNs,
                           ConfigurationRepository cl, StaticSgConfig staticSgConfig, ClusterService cs, final PrincipalExtractor principalExtractor,
                           AuthorizationService authorizationService,
                           SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
                           AuditLog auditLog, ConfigModificationValidators configModificationValidators) {
        super(settings, configPath, controller, client, adminDNs, cl, staticSgConfig, cs, principalExtractor, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators);
    }

    @Override
    public List<Route> routes() {
        return getStandardResourceRoutes("blocks");
    }
    
    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.BLOCKS;
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... param) {
        return new BlocksValidator(request, ref, this.settings, param);
    }

    @Override
    protected String getResourceName() {
        return "blocks";
    }

    @Override
    protected CType<Blocks> getConfigName() {
        return CType.BLOCKS;
    }

}
