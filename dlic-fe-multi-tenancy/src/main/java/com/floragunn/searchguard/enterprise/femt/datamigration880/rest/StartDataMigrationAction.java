package com.floragunn.searchguard.enterprise.femt.datamigration880.rest;

import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.enterprise.femt.MultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationService;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepsFactory;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class StartDataMigrationAction extends Action<EmptyRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(StartDataMigrationAction.class);

    public final static String NAME = "cluster:admin:searchguard:config/multitenancy/frontend_data_migration/8_8_0/start";
    public static final StartDataMigrationAction INSTANCE = new StartDataMigrationAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesPost("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0")//
        .with(INSTANCE, (params, body) -> new EmptyRequest())//
        .name("POST /_searchguard/config/fe_multi_tenancy/data_migration/8_8_0");

    public StartDataMigrationAction() {
        super(NAME, EmptyRequest::new, StandardResponse::new);
    }

    public static class StartDataMigrationHandler extends Handler<EmptyRequest, StandardResponse> {
        private final DataMigrationService dataMigrationService;

        @Inject
        public StartDataMigrationHandler(HandlerDependencies handlerDependencies, NodeClient client,
            MultiTenancyConfigurationProvider provider) {
            super(INSTANCE, handlerDependencies);
            Objects.requireNonNull(client, "Client is required");
            Objects.requireNonNull(provider, "Multi-tenancy configuration provider is required");
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            MigrationStateRepository migrationStateRepository = new IndexMigrationStateRepository(privilegedConfigClient);
            StepsFactory stepsFactory = new StepsFactory(privilegedConfigClient, provider);
            this.dataMigrationService = new DataMigrationService(migrationStateRepository, stepsFactory, Clock.systemUTC());
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
            return supplyAsync(() -> {
                try {
                    return dataMigrationService.migrateData();
                } catch (Exception ex) {
                    log.error("Unexpected error during multi-tenancy data migration occured.", ex);
                    return new StandardResponse(500).error("Unexpected error during data migration: " + ex.getMessage());
                }
            });
        }
    }
}