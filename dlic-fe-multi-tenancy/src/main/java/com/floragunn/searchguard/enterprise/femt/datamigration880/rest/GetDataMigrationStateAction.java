/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt.datamigration880.rest;

import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationService;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepsFactory;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.Action;
import com.floragunn.searchsupport.action.StandardRequests;
import com.floragunn.searchsupport.action.StandardResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.injection.guice.Inject;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

class GetDataMigrationStateAction extends Action<StandardRequests.EmptyRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(GetDataMigrationStateAction.class);

    public final static String NAME = "cluster:admin:searchguard:config/multitenancy/frontend_data_migration/8_8_0/get";
    public static final GetDataMigrationStateAction INSTANCE = new GetDataMigrationStateAction();

    public GetDataMigrationStateAction() {
        super(NAME, StandardRequests.EmptyRequest::new, StandardResponse::new);
    }

    public static class GetDataMigrationStateHandler extends Handler<StandardRequests.EmptyRequest, StandardResponse> {
        private final DataMigrationService dataMigrationService;

        @Inject
        public GetDataMigrationStateHandler(HandlerDependencies handlerDependencies, NodeClient client,
            FeMultiTenancyConfigurationProvider provider, ConfigurationRepository repository) {
            super(INSTANCE, handlerDependencies);
            Objects.requireNonNull(client, "Client is required");
            Objects.requireNonNull(provider, "Multi-tenancy configuration provider is required");
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            MigrationStateRepository migrationStateRepository = new IndexMigrationStateRepository(privilegedConfigClient);
            StepsFactory stepsFactory = new StepsFactory(privilegedConfigClient, provider, repository);
            this.dataMigrationService = new DataMigrationService(migrationStateRepository, stepsFactory);
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(StandardRequests.EmptyRequest request) {
            return supplyAsync(() -> {
                try {
                    return dataMigrationService.findDataMigrationState()
                            .map(migrationExecutionSummary -> new StandardResponse(200).data(migrationExecutionSummary))
                            .orElse(new StandardResponse(404).message("Multi-tenancy data migration state not found"));
                } catch (Exception ex) {
                    log.error("Unexpected error occurred while getting multi-tenancy data migration state.", ex);
                    return new StandardResponse(500).error("Unexpected error occurred while getting multi-tenancy data migration state. " + ex.getMessage());
                }
            });
        }
    }
}