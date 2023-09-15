package com.floragunn.searchguard.enterprise.femt.datamigration880.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.SearchGuardVersion;
import com.floragunn.searchguard.enterprise.femt.FeMultiTenancyConfigurationProvider;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationService;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationConfig;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence.IndexMigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps.StepsFactory;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.RestApi;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class StartDataMigrationAction extends Action<StartDataMigrationAction.StartDataMigrationRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(StartDataMigrationAction.class);

    public final static String NAME = "cluster:admin:searchguard:config/multitenancy/frontend_data_migration/8_8_0/start";
    public static final StartDataMigrationAction INSTANCE = new StartDataMigrationAction();

    public static final RestApi REST_API = new RestApi().responseHeaders(SearchGuardVersion.header())//
        .handlesPost("/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0")//
        .with(INSTANCE, (params, body) -> new StartDataMigrationRequest(body))//
        .name("POST /_searchguard/config/fe_multi_tenancy/data_migration/8_8_0");

    public StartDataMigrationAction() {
        super(NAME, StartDataMigrationRequest::new, StandardResponse::new);
    }

    public static class StartDataMigrationRequest extends Request {

        public static final String FIELD_ALLOW_YELLOW_INDICES = "allow_yellow_data_indices";
        private final MigrationConfig config;

        public StartDataMigrationRequest(UnparsedDocument<?> message) throws ConfigValidationException {
            this(message.parseAsDocNode());

        }

        public StartDataMigrationRequest(UnparsedMessage message) throws ConfigValidationException {
            this(message.requiredDocNode());
        }

        private StartDataMigrationRequest(DocNode docNode) throws ConfigValidationException {
            boolean allowYellowDataIndices = Optional.ofNullable(docNode.getBoolean(FIELD_ALLOW_YELLOW_INDICES)).orElse(false);
            this.config = new MigrationConfig(allowYellowDataIndices);
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of(FIELD_ALLOW_YELLOW_INDICES, config.allowYellowDataIndices());
        }

        public MigrationConfig getConfig() {
            return config;
        }
    }

    public static class StartDataMigrationHandler extends Handler<StartDataMigrationRequest, StandardResponse> {
        private final DataMigrationService dataMigrationService;

        @Inject
        public StartDataMigrationHandler(HandlerDependencies handlerDependencies, NodeClient client,
            FeMultiTenancyConfigurationProvider provider) {
            super(INSTANCE, handlerDependencies);
            Objects.requireNonNull(client, "Client is required");
            Objects.requireNonNull(provider, "Multi-tenancy configuration provider is required");
            PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);
            MigrationStateRepository migrationStateRepository = new IndexMigrationStateRepository(privilegedConfigClient);
            StepsFactory stepsFactory = new StepsFactory(privilegedConfigClient, provider);
            this.dataMigrationService = new DataMigrationService(migrationStateRepository, stepsFactory, Clock.systemUTC());
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(StartDataMigrationRequest request) {
            return supplyAsync(() -> {
                try {
                    return dataMigrationService.migrateData(request.getConfig());
                } catch (Exception ex) {
                    log.error("Unexpected error during multi-tenancy data migration occured.", ex);
                    return new StandardResponse(500).error("Unexpected error during data migration: " + ex.getMessage());
                }
            });
        }
    }
}