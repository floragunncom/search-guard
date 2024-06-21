/*
 * Copyright 2015-2022 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.configuration;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.floragunn.codova.config.templates.PipeExpression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocUpdateException;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.StaticSettings.AttributeSet;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.xcontent.XContentParserContext;

public class ConfigurationRepository implements ComponentStateProvider {
    private static final Logger LOGGER = LogManager.getLogger(ConfigurationRepository.class);

    private static final String OLD_INDEX_NAME_DEFAULT = "searchguard";
    private static final String NEW_INDEX_NAME_DEFAULT = ".searchguard";

    /**
     * @deprecated This is superseded by searchguard.internal_indices.main_config.name. This index configuration is only used if an index with the name configured in searchguard.internal_indices.main_config.name does not exist.
     */
    private static final StaticSettings.Attribute<String> OLD_INDEX_NAME = StaticSettings.Attribute.define("searchguard.config_index_name")
            .withDefault(OLD_INDEX_NAME_DEFAULT).asString();
    private static final StaticSettings.Attribute<String> NEW_INDEX_NAME = StaticSettings.Attribute.define("searchguard.config_repository.index_name")
            .withDefault(NEW_INDEX_NAME_DEFAULT).asString();
    private static final StaticSettings.Attribute<Boolean> ALLOW_DEFAULT_INIT_SGINDEX = StaticSettings.Attribute
            .define("searchguard.allow_default_init_sgindex").withDefault(false).asBoolean();

    /**
     * If false, Search Guard does not start the background thread which polls for the creation of the config index, but just waits for config update requests. This, some log entries are avoided. Used by the JUnit tests.
     */
    private static final StaticSettings.Attribute<Boolean> BACKGROUND_INIT_IF_SGINDEX_NOT_EXIST = StaticSettings.Attribute
            .define("searchguard.background_init_if_sgindex_not_exist").withDefault(true).asBoolean();

    public static final AttributeSet STATIC_SETTINGS = AttributeSet.of(OLD_INDEX_NAME, NEW_INDEX_NAME, ALLOW_DEFAULT_INIT_SGINDEX,
            BACKGROUND_INIT_IF_SGINDEX_NOT_EXIST);

    private final String configuredSearchguardIndexOld;
    private final String configuredSearchguardIndexNew;
    private final Pattern configuredSearchguardIndices;
    private final Client client;

    private volatile ConfigMap currentConfig;
    private final List<ConfigurationChangeListener> configurationChangedListener;

    /**
     * ConfigurationLoader for config that will be used by SG. Keeps component state up-to-date.
     * Also adds static configuration to the loaded configuration.
     */
    private final ConfigurationLoader mainConfigLoader;

    /**
     * ConfigurationLoader for config that will be just passed through APIs.
     * DOES NOT add static configuration to the loaded configuration.
     */
    private final ConfigurationLoader externalUseConfigLoader;

    private final StaticSettings settings;
    private final ClusterService clusterService;
    private final ComponentState componentState = new ComponentState(-1000, null, "config_repository", ConfigurationRepository.class);
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ThreadPool threadPool;

    public final static ImmutableMap<String, Object> SG_INDEX_MAPPING = ImmutableMap.of("dynamic_templates", Collections.singletonList(ImmutableMap
            .of("encoded_config", ImmutableMap.of("match", "*", "match_mapping_type", "*", "mapping", ImmutableMap.of("type", "binary")))));

    private final static ImmutableMap<String, Object> SG_INDEX_SETTINGS = ImmutableMap.of("index.number_of_shards", 1, "index.auto_expand_replicas",
            "0-all");

    private final VariableResolvers variableResolvers;
    private final Context parserContext;

    private final IndexNameExpressionResolver resolver;
    private final ConfigsRelationsValidator configsRelationsValidator;

    public ConfigurationRepository(StaticSettings settings, ThreadPool threadPool, Client client, ClusterService clusterService,
            ConfigVarService configVarService, SearchGuardModulesRegistry modulesRegistry, StaticSgConfig staticSgConfig,
            NamedXContentRegistry xContentRegistry, Environment environment, IndexNameExpressionResolver resolver) {
        this.configuredSearchguardIndexOld = settings.get(OLD_INDEX_NAME);
        this.configuredSearchguardIndexNew = settings.get(NEW_INDEX_NAME);
        this.configuredSearchguardIndices = Pattern.createUnchecked(this.configuredSearchguardIndexNew, this.configuredSearchguardIndexOld);
        this.client = client;
        this.settings = settings;
        this.clusterService = clusterService;
        this.configurationChangedListener = new ArrayList<>();
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.componentState.setMandatory(true);
        this.mainConfigLoader = new ConfigurationLoader(client, componentState, this, staticSgConfig);
        this.externalUseConfigLoader = new ConfigurationLoader(client, null, this, null);
        this.variableResolvers = new VariableResolvers()
                .with("file", (file) -> VariableResolvers.FILE_PRIVILEGED.apply(environment.configFile().resolve(file).toAbsolutePath().toString()))
                .with("env", VariableResolvers.ENV).with("var", (key) -> configVarService.get(key))
                .with("json_file", (file) -> VariableResolvers.JSON_FILE_PRIVILEGED.apply(environment.configFile().resolve(file).toAbsolutePath().toString()));
        ImmutableMap<String, PipeExpression.PipeFunction> pipeFunctions = PipeExpression.PipeFunction.all() //
            .with(BcryptPipeFunction.NAME, new BcryptPipeFunction());
        this.parserContext = new Context(variableResolvers, modulesRegistry, settings, xContentRegistry, pipeFunctions);
        this.threadPool = threadPool;

        configVarService.addChangeListener(() -> {
            if (currentConfig != null) {
                try {
                    reloadConfiguration(CType.all(), "Config variable update");
                } catch (Exception e) {
                    LOGGER.error("Error while reloading configuration after config var change", e);
                }
            }
        });

        this.resolver = resolver;
        this.configsRelationsValidator = new ConfigsRelationsValidator(this);
    }

    public void initOnNodeStart() {
        componentState.setState(State.INITIALIZING, "waiting_for_state_recovery");

        threadPool.generic().execute(() -> {
            synchronized (ConfigurationRepository.this) {

                if (!checkClusterState(clusterService.state())) {
                    this.clusterService.addListener(new ClusterStateListener() {
                        @Override
                        public void clusterChanged(ClusterChangedEvent event) {
                            if (checkClusterState(event.state())) {
                                clusterService.removeListener(this);
                            }
                        }
                    });
                }

            }
        });
    }

    private boolean checkClusterState(ClusterState clusterState) {
        if (!clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            componentState.setState(State.INITIALIZING, "cluster_state_recovered");
            LOGGER.info("Cluster state has been recovered. Starting config index initialization.");
            checkIndicesNow();
            return true;
        } else {
            return false;
        }
    }

    private void checkIndicesNow() {
        LOGGER.debug("Check if one of the indices " + configuredSearchguardIndexNew + " or " + configuredSearchguardIndexOld + " does exist ...");

        try {
            if (resolver.hasIndexAbstraction(configuredSearchguardIndexNew, clusterService.state())) {
                LOGGER.info("{} index does exist. Loading configuration.", configuredSearchguardIndexNew);
                threadPool.generic().submit(() -> loadConfigurationOnStartup(configuredSearchguardIndexNew));
            } else if (resolver.hasIndexAbstraction(configuredSearchguardIndexOld, clusterService.state())) {
                LOGGER.info("Legacy {} index does exist. Loading configuration.", configuredSearchguardIndexOld);
                threadPool.generic().submit(() -> loadConfigurationOnStartup(configuredSearchguardIndexOld));
            } else if (settings.get(ALLOW_DEFAULT_INIT_SGINDEX)) {
                LOGGER.info("{} index does not exist yet, so we create a default config", configuredSearchguardIndexNew);
                threadPool.generic().submit(() -> {
                    try {
                        installDefaultConfiguration(configuredSearchguardIndexNew);
                        loadConfigurationOnStartup(configuredSearchguardIndexNew);
                    } catch (Exception e) {
                        LOGGER.error("An error occurred while initializing default config. Initialisation halted.", e);
                    }
                });
            } else if (settings.get(BACKGROUND_INIT_IF_SGINDEX_NOT_EXIST)) {
                LOGGER.info("{} index does not exist yet, so no need to load config on node startup. Use sgctl to initialize cluster",
                        configuredSearchguardIndexNew);
                threadPool.generic().submit(() -> waitForConfigIndex());
            } else {
                LOGGER.info("{} index does not exist yet, use sgctl to initialize the cluster. We will not perform background initialization",
                        configuredSearchguardIndexNew);
                componentState.setState(State.SUSPENDED, "waiting_for_config_update");
            }
        } catch (Throwable e2) {
            LOGGER.error("Error during node initialization", e2);
            componentState.addLastException("initOnNodeStart", e2);
            componentState.setFailed(e2);
        }
    }

    /**
     * @param configurationType
     * @return can also return empty in case it was never loaded
     */
    public <T> SgDynamicConfiguration<T> getConfiguration(CType<T> configurationType) {
        if (currentConfig == null) {
            throw new RuntimeException("ConfigurationRepository is not yet initialized");
        }

        return currentConfig.get(configurationType);
    }

    public boolean isInitialized() {
        return currentConfig != null;
    }

    public boolean isIndexInitialized() {
        return getEffectiveSearchGuardIndex() != null;
    }

    private final Lock LOCK = new ReentrantLock();

    public void reloadConfiguration(Set<CType<?>> configTypes, String reason)
            throws ConfigUpdateAlreadyInProgressException, ConfigUnavailableException {
        // Drop user information from thread context to avoid spamming of audit log
        try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            if (LOCK.tryLock(60, TimeUnit.SECONDS)) {
                try {
                    reloadConfiguration0(configTypes, reason);
                } finally {
                    LOCK.unlock();
                }
            } else {
                throw new ConfigUpdateAlreadyInProgressException("A config update is already in progress");
            }
        } catch (InterruptedException e) {
            throw new ConfigUpdateAlreadyInProgressException("Interrupted config update", e);
        }
    }

    private void waitForConfigIndex() {
        try {
            componentState.setState(State.INITIALIZING, "waiting_for_config_index");
            do {
                Thread.sleep(500);
            } while (!resolver.hasIndexAbstraction(this.configuredSearchguardIndexNew, clusterService.state())
                    && !resolver.hasIndexAbstraction(this.configuredSearchguardIndexOld, clusterService.state()));

            if (resolver.hasIndexAbstraction(this.configuredSearchguardIndexNew, clusterService.state())) {
                loadConfigurationOnStartup(configuredSearchguardIndexNew);
            } else {
                loadConfigurationOnStartup(configuredSearchguardIndexOld);
            }
        } catch (Exception e) {
            LOGGER.error("Error while waiting for the configuration index to be created", e);
            componentState.setFailed(e);
        }
    }

    private void loadConfigurationOnStartup(String searchguardIndex) {
        try {
            LOGGER.debug("Node started, try to initialize it. Wait for at least yellow state on index " + searchguardIndex);

            componentState.setState(State.INITIALIZING, "waiting_for_yellow_index");
            componentState.setConfigProperty("effective_main_config_index", searchguardIndex);

            ClusterHealthResponse response = null;
            try {
                response = client.admin().cluster().health(new ClusterHealthRequest(searchguardIndex).waitForActiveShards(1).waitForYellowStatus())
                        .actionGet();
            } catch (Exception e1) {
                LOGGER.debug("Catched a {} but we just try again ...", e1.toString());
            }

            while (response == null || response.isTimedOut() || response.getStatus() == ClusterHealthStatus.RED) {
                LOGGER.debug("index '{}' not healthy yet, we try again ... (Reason: {})", searchguardIndex,
                        response == null ? "no response" : (response.isTimedOut() ? "timeout" : "other, maybe red cluster"));

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(Strings.toString(response));
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    //ignore
                }
                componentState.startNextTry();
                try {
                    response = client.admin().cluster()
                            .health(new ClusterHealthRequest(searchguardIndex).waitForActiveShards(1).waitForYellowStatus()).actionGet();

                } catch (Exception e1) {
                    LOGGER.debug("Catched again a {} but we just try again ...", e1.toString());
                }
                continue;
            }

            final int maxAttempts = 30;
            final long requiredDocCount = CType.all().stream().filter(cType -> cType.isRequired()).count();

            componentState.setState(State.INITIALIZING, "waiting_for_docs_in_index");

            long docCount = 0;
            int attempts = 0;
            while (docCount < requiredDocCount && attempts < maxAttempts) {

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    LOGGER.debug("Thread was interrupted so we cancel initialization");
                    break;
                }
                
                try {

                    final ImmutableOpenMap<String, List<AliasMetadata>> aliasMetadata = client.admin().indices().getAliases(new GetAliasesRequest(searchguardIndex)).actionGet().getAliases();

                    if(aliasMetadata != null && aliasMetadata.size() == 1) {

                        final String indexName = aliasMetadata.keysIt().next();

                        LOGGER.info("Resolved alias '{}' to index '{}' for looking up doc count", searchguardIndex, indexName);

                        client.admin().indices().refresh(new RefreshRequest(indexName)).get();
                        docCount = client.admin().indices().stats(new IndicesStatsRequest().indices(indexName).docs(true)).actionGet()
                                .getIndex(indexName).getTotal().docs.getCount();

                    } else {
                        client.admin().indices().refresh(new RefreshRequest(searchguardIndex)).get();
                        docCount = client.admin().indices().stats(new IndicesStatsRequest().indices(searchguardIndex).docs(true)).actionGet()
                                .getIndex(searchguardIndex).getTotal().docs.getCount();
                    }

                    if (docCount < requiredDocCount) {
                        LOGGER.info("Got {} documents, waiting for {} in total, we just try again ...", docCount, requiredDocCount);
                    } else {
                        break;
                    }
                } catch (Exception e1) {
                    LOGGER.warn("Catched a {} but we just try again ...", e1.toString());
                }

                attempts++;
            }

            componentState.setState(State.INITIALIZING, "loading");

            while (ConfigurationRepository.this.currentConfig == null) {
                componentState.startNextTry();
                try {
                    LOGGER.debug("Try to load config ...");
                    reloadConfiguration(CType.all(), "Initialization");
                    break;
                } catch (Exception e) {
                    LOGGER.debug("Unable to load configuration due to {}", String.valueOf(ExceptionUtils.getRootCause(e)));
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        LOGGER.debug("Thread was interrupted so we cancel initialization");
                        break;
                    }
                }
            }

            LOGGER.info("Node '{}' initialized", clusterService.localNode().getName());

            componentState.setInitialized();

        } catch (Exception e) {
            LOGGER.error("Unexpected exception while initializing node " + e, e);
            componentState.setFailed(e);
        }
    }

    private void installDefaultConfiguration(String searchguardIndex) throws ConfigUpdateException {
        try {
            componentState.setState(State.INITIALIZING, "install_default_config");

            String lookupDir = System.getProperty("sg.default_init.dir");
            File cd = lookupDir != null ? new File(lookupDir)
                    : settings.getPlatformPluginsDirectory().resolve("search-guard-flx/sgconfig/").toAbsolutePath().toFile();
            File confFile = new File(cd, "sg_authc.yml");
            File legacyConfFile = new File(cd, "sg_config.yml");

            if (confFile.exists() || legacyConfFile.exists()) {
                LOGGER.info("Will create {} index so we can apply default config", searchguardIndex);

                try {
                    createConfigIndex(searchguardIndex);
                } catch (ResourceAlreadyExistsException e) {
                    LOGGER.debug(
                            "Search Guard index was created in the meantime. Possibly by some other node in the cluster. Not applying default config",
                            e);
                }
                if (new File(cd, "sg_authc.yml").exists()) {
                    uploadFile(privilegedConfigClient, cd, "sg_authc.yml", searchguardIndex, CType.AUTHC, parserContext);
                } else if (new File(cd, "sg_config.yml").exists()) {
                    uploadFile(privilegedConfigClient, cd, "sg_config.yml", searchguardIndex, CType.CONFIG, parserContext);
                }

                uploadFile(privilegedConfigClient, cd, "sg_roles.yml", searchguardIndex, CType.ROLES, parserContext);
                uploadFile(privilegedConfigClient, cd, "sg_roles_mapping.yml", searchguardIndex, CType.ROLESMAPPING, parserContext);
                uploadFile(privilegedConfigClient, cd, "sg_internal_users.yml", searchguardIndex, CType.INTERNALUSERS, parserContext);
                uploadFile(privilegedConfigClient, cd, "sg_action_groups.yml", searchguardIndex, CType.ACTIONGROUPS, parserContext);
                uploadFile(privilegedConfigClient, cd, "sg_tenants.yml", searchguardIndex, CType.TENANTS, parserContext);
                uploadFile(privilegedConfigClient, cd, "sg_frontend_authc.yml", searchguardIndex, CType.FRONTEND_AUTHC, parserContext);

                if (new File(cd, "sg_blocks.yml").exists()) {
                    uploadFile(privilegedConfigClient, cd, "sg_blocks.yml", searchguardIndex, CType.BLOCKS, parserContext);
                }

                if (new File(cd, "sg_authz.yml").exists()) {
                    uploadFile(privilegedConfigClient, cd, "sg_authz.yml", searchguardIndex, CType.AUTHZ, parserContext);
                }

                if (new File(cd, "sg_license_key.yml").exists()) {
                    uploadFile(privilegedConfigClient, cd, "sg_license_key.yml", searchguardIndex, CType.LICENSE_KEY, parserContext);
                }

                LOGGER.info("Default config applied");
            }
        } catch (ConfigUpdateException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigUpdateException("Error while installing default configuration: " + e.getMessage(), e);
        }
    }

    private void createConfigIndex(String searchguardIndex) throws ConfigUpdateException {

        ImmutableMap<String, Object> indexSettings = SG_INDEX_SETTINGS;

        if (searchguardIndex.startsWith(".")) {
            indexSettings = indexSettings.with("index.hidden", true);
        }

        boolean ok = client.admin().indices()
                .create(new CreateIndexRequest(searchguardIndex).settings(indexSettings).mapping("_doc", SG_INDEX_MAPPING)).actionGet()
                .isAcknowledged();

        if (!ok) {
            LOGGER.error("Can not create {} index", searchguardIndex);
            componentState.setFailed("Index creation was not acknowledged");
            throw new ConfigUpdateException("Creation of " + searchguardIndex + " was not acknowledged");
        }
    }

    public String getEffectiveSearchGuardIndex() {
        if (resolver.hasIndexAbstraction(configuredSearchguardIndexNew, clusterService.state())) {
            return configuredSearchguardIndexNew;
        } else if (resolver.hasIndexAbstraction(configuredSearchguardIndexOld, clusterService.state())) {
            return configuredSearchguardIndexOld;
        } else {
            return null;
        }
    }

    public StandardResponse migrateIndex() {
        String effectiveSearchGuardIndex = getEffectiveSearchGuardIndex();

        if (effectiveSearchGuardIndex == null) {
            return new StandardResponse(503, "Search Guard is not initialized; the config index does not exist.");
        }

        if (configuredSearchguardIndexNew.equals(effectiveSearchGuardIndex)) {
            return new StandardResponse(412, "Search Guard already uses the new-style index: " + effectiveSearchGuardIndex);
        }

        PrivilegedConfigClient privilegedConfigClient = PrivilegedConfigClient.adapt(client);

        SearchResponse searchResponse = null;

        try {
            searchResponse = privilegedConfigClient.search(
                    new SearchRequest(effectiveSearchGuardIndex).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1000)))
                    .actionGet();

            if (searchResponse.getHits().getHits().length == 0) {
                throw new Exception("Search request returned too few entries: " + Strings.toString(searchResponse));
            }
        } catch (Exception e) {
            LOGGER.error("Error while reading data from existing index for migration", e);
            return new StandardResponse(500, "Error while reading data from old index: " + e.getMessage());
        }

        try {
            createConfigIndex(configuredSearchguardIndexNew);
        } catch (Exception e) {
            LOGGER.error("Error while creating new index for migration", e);
            return new StandardResponse(500, e.getMessage());
        }

        BulkRequest bulkRequest = new BulkRequest(configuredSearchguardIndexNew).setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            bulkRequest
                    .add(new IndexRequest(configuredSearchguardIndexNew).id(searchHit.getId()).source(searchHit.getSourceRef(), XContentType.JSON));
        }

        try {
            BulkResponse bulkResponse = privilegedConfigClient.bulk(bulkRequest).actionGet();

            if (bulkResponse.hasFailures()) {
                throw new ConfigUpdateException("Update failed: " + bulkResponse.buildFailureMessage());
            }

            ConfigMap configMap = this.externalUseConfigLoader.loadSync(CType.all(), "Testing configuration after migration", parserContext);

            LOGGER.info("Configuration after migration: " + configMap);

            if (!configMap.getTypes().equals(currentConfig.getTypes())) {
                throw new Exception("Validation of migrated configuration failed");
            }

            ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]));
            ConfigUpdateResponse configUpdateResponse = privilegedConfigClient.execute(ConfigUpdateAction.INSTANCE, configUpdateRequest).actionGet();

            if (!configUpdateResponse.hasFailures()) {
                return new StandardResponse(200, "Index migration and configuration update was successful")
                        .data(ImmutableMap.of("new_config", configMap.toString()));
            } else {
                return new StandardResponse(500, "Index migration was successful; however, some nodes reported failures. Please check these nodes.")
                        .data(ImmutableMap.of("failures", configUpdateResponse.failures().toString()));
            }
        } catch (Exception e) {
            LOGGER.error("Error while doing builk write for migration. Deleting index again", e);
            try {
                privilegedConfigClient.admin().indices().delete(new DeleteIndexRequest(configuredSearchguardIndexNew)).actionGet();
            } catch (Exception e2) {
                LOGGER.error("Error while deleting new search guard index due to previous error", e2);
            }
            return new StandardResponse(500, e.getMessage());
        }

    }

    public boolean usesLegacySearchGuardIndex() {
        return configuredSearchguardIndexOld.equals(getEffectiveSearchGuardIndex());
    }

    public String getEffectiveSearchGuardIndexAndCreateIfNecessary() throws ConfigUpdateException {
        String searchGuardIndex = getEffectiveSearchGuardIndex();

        if (searchGuardIndex != null) {
            return searchGuardIndex;
        } else {
            createConfigIndex(configuredSearchguardIndexNew);
            return configuredSearchguardIndexNew;
        }
    }

    private void reloadConfiguration0(Set<CType<?>> configTypes, String reason) throws ConfigUnavailableException {
        try {
            ConfigMap loadedConfig = mainConfigLoader.load(configTypes, reason, parserContext.withExternalResources()).get();
            ConfigMap discardedConfig;
            boolean initialLoad = false;

            componentState.setConfigProperty("effective_main_config_index", loadedConfig.getSourceIndex());

            if (this.currentConfig == null) {
                this.currentConfig = loadedConfig;
                initialLoad = true;
                discardedConfig = null;
            } else {
                ConfigMap oldConfig = this.currentConfig;
                ConfigMap mergedConfig = oldConfig.with(loadedConfig);
                discardedConfig = oldConfig.only(loadedConfig.getTypes());

                this.currentConfig = mergedConfig;
            }

            notifyAboutChanges(this.currentConfig);

            if (initialLoad) {
                LOGGER.info("Search Guard configuration has been successfully initialized");
            }
            
            if (discardedConfig != null && !discardedConfig.isEmpty()) {
                this.threadPool.schedule(() -> {
                    LOGGER.debug("Destroying old configuration: " + discardedConfig);
                    discardedConfig.close();
                }, TimeValue.timeValueSeconds(10), ThreadPool.Names.GENERIC);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for configuration", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ConfigUnavailableException) {
                throw (ConfigUnavailableException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void subscribeOnChange(ConfigurationChangeListener listener) {
        configurationChangedListener.add(listener);
    }

    private synchronized void notifyAboutChanges(ConfigMap configMap) {
        for (ConfigurationChangeListener listener : configurationChangedListener) {
            try {
                LOGGER.debug("Notify {} listener about change configuration with type {}", listener);
                listener.onChange(configMap);
            } catch (Exception e) {
                LOGGER.error("{} listener errored: " + e, listener, e);
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
    }

    /**
     * This retrieves the config directly from the index without caching involved 
     *
     * @param configType
     * @return
     * @throws ConfigUnavailableException
     */
    public <T> SgDynamicConfiguration<T> getConfigurationFromIndex(CType<T> configType, String reason) throws ConfigUnavailableException {
        return externalUseConfigLoader.loadSync(configType, reason, parserContext);
    }
   
    public <T> StandardResponse addOrUpdate(CType<T> ctype, String id, T entry, String matchETag)
            throws ConfigUpdateException, ConcurrentConfigUpdateException, ConfigValidationException {
        try {
            ValidationErrors validationErrors = new ValidationErrors();
            validationErrors.add(configsRelationsValidator.validateConfigEntryRelations(entry));

            validationErrors.throwExceptionForPresentErrors();

            SgDynamicConfiguration<T> configInstance = getConfigurationFromIndex(ctype, "Update of entry " + id);

            if (matchETag != null && !configInstance.getETag().equals(matchETag)) {
                throw new ConcurrentConfigUpdateException("Unable to update configuration due to concurrent modification:\nIf-Match: " + matchETag
                        + "; ETag: " + configInstance.getETag());
            }

            boolean alreadyExists = configInstance.getCEntry(id) != null;

            configInstance = configInstance.with(id, entry);

            update(ctype, configInstance, matchETag, false);

            if (!alreadyExists) {
                return new StandardResponse(201).message(ctype.getUiName() + " " + id + " has been created");
            } else {
                return new StandardResponse(200).message(ctype.getUiName() + " " + id + " has been updated");
            }
        } catch (ConfigUnavailableException e) {
            throw new ConfigUpdateException(e);
        }
    }

    public <T> StandardResponse applyPatch(CType<T> configType, DocPatch patch, String matchETag)
            throws ConfigUpdateException, ConcurrentConfigUpdateException, NoSuchConfigEntryException, ConfigValidationException {
        try {

            if (configType.getArity() == CType.Arity.SINGLE) {
                return applyPatch(configType, "default", patch, matchETag, PatchDefaultHandling.TREAT_MISSING_DOCUMENT_AS_EMPTY_DOCUMENT);
            }

            SgDynamicConfiguration<T> configInstance = getConfigurationFromIndex(configType, "Update of entry");

            if (matchETag != null && !configInstance.getETag().equals(matchETag)) {
                throw new ConcurrentConfigUpdateException("Unable to update configuration due to concurrent modification:\nIf-Match: " + matchETag
                        + "; ETag: " + configInstance.getETag());
            }

            PatchableDocument<Map<String, T>> doc = new PatchableDocument<Map<String, T>>() {

                @Override
                public Object toBasicObject() {
                    return configInstance.toBasicObject();
                }

                @Override
                public Map<String, T> parseI(DocNode docNode, com.floragunn.codova.documents.Parser.Context context)
                        throws ConfigValidationException {
                    ValidationErrors validationErrors = new ValidationErrors();

                    if (!docNode.isMap()) {
                        throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "A JSON object"));
                    }

                    LinkedHashMap<String, T> result = new LinkedHashMap<>();

                    for (Map.Entry<String, DocNode> entry : docNode.toMapOfNodes().entrySet()) {
                        ValidationResult<T> parsedEntry = configType.getParser()
                                .parse(entry.getValue(), parserContext.withExternalResources().withoutLenientValidation());

                        if (parsedEntry.hasErrors()) {
                            validationErrors.add(entry.getKey(), parsedEntry.getValidationErrors());
                        } else {
                            result.put(entry.getKey(), parsedEntry.peek());
                        }
                        
                        if (parsedEntry.peek() instanceof AutoCloseable) {
                            try {
                                ((AutoCloseable) parsedEntry.peek()).close();
                            } catch (Exception e) {
                                LOGGER.warn("Error while closing {}", parsedEntry, e);
                            }
                        }
                    }

                    validationErrors.throwExceptionForPresentErrors();

                    return result;
                }
            };

            update(configType, SgDynamicConfiguration.of(configType, doc.patch(patch, parserContext)), matchETag, true);

            return new StandardResponse(200).message(configType.getUiName() + " has been updated");

        } catch (ConfigUnavailableException | DocUpdateException e) {
            throw new ConfigUpdateException(e);
        }
    }

    public <T> StandardResponse applyPatch(CType<T> configType, String id, DocPatch patch, String matchETag,
            PatchDefaultHandling patchDefaultHandling)
            throws ConfigUpdateException, ConcurrentConfigUpdateException, NoSuchConfigEntryException, ConfigValidationException {
        try {
            SgDynamicConfiguration<T> configInstance = getConfigurationFromIndex(configType, "Update of entry " + id);

            if (matchETag != null && !configInstance.getETag().equals(matchETag)) {
                throw new ConcurrentConfigUpdateException("Unable to update configuration due to concurrent modification:\nIf-Match: " + matchETag
                        + "; ETag: " + configInstance.getETag());
            }

            T document = configInstance.getCEntry(id);

            if (document == null) {
                if (patchDefaultHandling == PatchDefaultHandling.TREAT_MISSING_DOCUMENT_AS_EMPTY_DOCUMENT) {
                    document = configType.createDefaultInstance(parserContext);
                } else {
                    throw new NoSuchConfigEntryException(configType, id);
                }
            }

            if (!(document instanceof PatchableDocument)) {
                throw new ConfigUpdateException("The config type " + configType + " cannot be patched");
            }

            @SuppressWarnings("unchecked")
            PatchableDocument<T> entry = (PatchableDocument<T>) document;
            T newEntry = entry.patch(patch, parserContext.withExternalResources().withoutLenientValidation());
            
            try {
                ValidationErrors validationErrors = new ValidationErrors();
                validationErrors.add(configsRelationsValidator.validateConfigEntryRelations(newEntry));

                validationErrors.throwExceptionForPresentErrors();

                update(configType, configInstance.with(id, newEntry), matchETag, false);
            } finally {
                if (newEntry instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) newEntry).close();
                    } catch (Exception e) {
                        LOGGER.warn("Error while closing {}", newEntry, e);
                    }
                }
            }
            
            String message;

            if (configType.getArity() == CType.Arity.SINGLE) {
                message = configType.getUiName() + " has been updated";
            } else {
                message = configType.getUiName() + " " + id + " has been updated";
            }

            return new StandardResponse(200).message(message);

        } catch (ConfigUnavailableException | DocUpdateException e) {
            throw new ConfigUpdateException(e);
        }
    }

    public <T> StandardResponse delete(CType<T> configType, String id)
            throws ConfigUpdateException, ConcurrentConfigUpdateException, NoSuchConfigEntryException {
        try {

            SgDynamicConfiguration<T> configInstance = getConfigurationFromIndex(configType, "Deletion of entry " + id);

            if (!configInstance.exists(id)) {
                throw new NoSuchConfigEntryException(configType, id);
            }

            update(configType, configInstance.without(id), null, false);

            return new StandardResponse(200).message(configType.getUiName() + " " + id + " has been deleted");
        } catch (ConfigUnavailableException | ConfigValidationException e) {
            throw new ConfigUpdateException(e);
        }
    }

    public <T> StandardResponse delete(CType<T> configType) throws ConfigUpdateException, ConcurrentConfigUpdateException {
        try {
            DeleteRequest request = new DeleteRequest(getEffectiveSearchGuardIndexAndCreateIfNecessary(), configType.toLCString());
            DeleteResponse response = privilegedConfigClient.delete(request).actionGet();
            if (response.status() == RestStatus.NOT_FOUND) {
                return new StandardResponse(404).message(configType.toLCString() + " does not exist");
            }
            reloadConfiguration(Collections.singleton(configType), "Config deletion");
            return new StandardResponse(200).message(configType.toLCString() + " has been deleted");
        } catch (ConfigUpdateException | ConfigUnavailableException e) {
            return new StandardResponse(e);
        }
    }

    public <T> void update(CType<T> ctype, SgDynamicConfiguration<T> configInstance, String matchETag)
            throws ConfigUpdateException, ConcurrentConfigUpdateException, ConfigValidationException {
        update(ctype, configInstance, matchETag, true);
    }

    private  <T> void update(CType<T> ctype, SgDynamicConfiguration<T> configInstance, String matchETag,
                             boolean validateConfigRelations) throws ConfigUpdateException, ConcurrentConfigUpdateException, ConfigValidationException {

        if (validateConfigRelations) {
            ValidationErrors validationErrors = new ValidationErrors();
            validationErrors.add(configsRelationsValidator.validateConfigRelations(configInstance));
            validationErrors.throwExceptionForPresentErrors();
        }

        IndexRequest indexRequest = new IndexRequest(getEffectiveSearchGuardIndexAndCreateIfNecessary());

        try {
            String id = ctype.toLCString();

            indexRequest = indexRequest.id(id).source(id, XContentHelper.toXContent(configInstance.withoutStatic(), XContentType.JSON, false))
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (matchETag != null) {
            try {
                int dot = matchETag.indexOf('.');

                if (dot == -1) {
                    throw new ConcurrentConfigUpdateException("Invalid E-Tag " + matchETag);
                }

                indexRequest.setIfPrimaryTerm(Long.parseLong(matchETag.substring(0, dot)));
                indexRequest.setIfSeqNo(Long.parseLong(matchETag.substring(dot + 1)));
            } catch (NumberFormatException e) {
                throw new ConcurrentConfigUpdateException("Invalid E-Tag " + matchETag);
            }
        } else if (configInstance.getPrimaryTerm() != -1 && configInstance.getSeqNo() != -1) {
            indexRequest.setIfPrimaryTerm(configInstance.getPrimaryTerm());
            indexRequest.setIfSeqNo(configInstance.getSeqNo());
        }

        try {
            privilegedConfigClient.index(indexRequest).actionGet();
        } catch (VersionConflictEngineException e) {
            throw new ConcurrentConfigUpdateException("Unable to update configuration due to concurrent modification", e);
        } catch (Exception e) {
            throw new ConfigUpdateException("Updating the config failed", e);
        }

        try {
            ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]));

            ConfigUpdateResponse configUpdateResponse = privilegedConfigClient.execute(ConfigUpdateAction.INSTANCE, configUpdateRequest).actionGet();

            if (configUpdateResponse.hasFailures()) {
                throw new ConfigUpdateException("Configuration index was updated; however, some nodes reported failures while refreshing.",
                        configUpdateResponse);
            }
        } catch (Exception e) {
            throw new ConfigUpdateException("Configuration index was updated; however, the refresh failed", e);
        }
    }

    public Map<CType<?>, ConfigUpdateResult> update(Map<CType<?>, ConfigWithMetadata> configTypeToConfigMap)
            throws ConfigUpdateException, ConfigValidationException, ConcurrentConfigUpdateException {
        LOGGER.info("Updating configuration {}", configTypeToConfigMap.keySet());
        
        ValidationErrors validationErrors = new ValidationErrors();
        BulkRequest bulkRequest = new BulkRequest();
        List<SgDynamicConfiguration<?>> parsedConfigs = new ArrayList<>();

        bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        if (configTypeToConfigMap.isEmpty()) {
            throw new ConfigValidationException(new ValidationError(null, "No configuration given"));
        }

        List<String> configTypesWithConcurrentModifications = new ArrayList<>();
        String searchGuardIndex = getEffectiveSearchGuardIndexAndCreateIfNecessary();

        for (Map.Entry<CType<?>, ConfigWithMetadata> entry : configTypeToConfigMap.entrySet()) {
            CType<?> ctype = entry.getKey();

            if (entry.getValue() == null) {
                validationErrors.add(new InvalidAttributeValue(ctype.toLCString(), null, "A config JSON document"));
                continue;
            }

            Map<String, ?> configMap = entry.getValue().getContent();
            String matchETag = entry.getValue().getEtag();

            if (configMap == null) {
                validationErrors.add(new InvalidAttributeValue(ctype.toLCString(), null, "A config JSON document"));
                continue;
            }

            if (ctype.getArity() == CType.Arity.SINGLE) {
                if (!configMap.isEmpty()) {
                    configMap = ImmutableMap.of("default", DocNode.wrap(configMap).splitDottedAttributeNamesToTree().toMap());
                } else {
                    configMap = ImmutableMap.empty();
                }
            }

            try {
                @SuppressWarnings({ "unchecked", "rawtypes" }) // XXX weird generics issue
                ValidationResult<SgDynamicConfiguration<?>> configInstance = (ValidationResult<SgDynamicConfiguration<?>>) (ValidationResult) SgDynamicConfiguration
                        .fromMap(configMap, ctype, parserContext.withExternalResources().withoutLenientValidation());

                if (configInstance.getValidationErrors() != null && configInstance.getValidationErrors().hasErrors()) {
                    validationErrors.add(ctype.toLCString(), configInstance.getValidationErrors());
                    continue;
                }

                String id = ctype.toLCString();

                IndexRequest indexRequest = new IndexRequest(searchGuardIndex).id(id).source(id,
                        XContentHelper.toXContent(configInstance.peek().withoutStatic(), XContentType.JSON, false));

                if (matchETag != null) {
                    try {
                        int dot = matchETag.indexOf('.');

                        if (dot == -1) {
                            validationErrors.add(new InvalidAttributeValue(ctype.toLCString(), matchETag, null).message("Invalid E-Tag"));
                            continue;
                        }

                        long primaryTerm = Long.parseLong(matchETag.substring(0, dot));
                        long seqNo = Long.parseLong(matchETag.substring(dot + 1));

                        if (currentConfig != null) {
                            SgDynamicConfiguration<?> currentConfigInstance = currentConfig.get(ctype);

                            if (currentConfigInstance != null
                                    && (currentConfigInstance.getPrimaryTerm() != primaryTerm || currentConfigInstance.getSeqNo() != seqNo)) {
                                configTypesWithConcurrentModifications.add(ctype.toLCString());
                            }
                        }

                        indexRequest.setIfPrimaryTerm(primaryTerm);
                        indexRequest.setIfSeqNo(seqNo);
                    } catch (NumberFormatException e) {
                        validationErrors.add(new InvalidAttributeValue(ctype.toLCString(), matchETag, null).message("Invalid E-Tag"));
                        continue;
                    }
                }

                bulkRequest.add(indexRequest);
                if(configInstance.hasResult()) {
                    parsedConfigs.add(configInstance.peek());
                }
            } catch (ConfigValidationException e) {
                validationErrors.add(ctype.toLCString(), e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        validationErrors.add(configsRelationsValidator.validateConfigsRelations(parsedConfigs));

        parsedConfigs.forEach(SgDynamicConfiguration::close);

        validationErrors.throwExceptionForPresentErrors();

        if (!configTypesWithConcurrentModifications.isEmpty()) {
            if (configTypesWithConcurrentModifications.size() == 1) {
                throw new ConcurrentConfigUpdateException(
                        "The configuration " + configTypesWithConcurrentModifications.get(0) + " has been concurrently modified.");
            } else {
                throw new ConcurrentConfigUpdateException("The configurations "
                        + (configTypesWithConcurrentModifications.stream().collect(Collectors.joining(", "))) + " have been concurrently modified.");
            }
        }

        try {
            BulkResponse bulkResponse = privilegedConfigClient.bulk(bulkRequest).actionGet();
            Map<CType<?>, ConfigUpdateResult> result = new LinkedHashMap<>(configTypeToConfigMap.size());

            for (BulkItemResponse item : bulkResponse.getItems()) {
                CType<?> ctype = CType.fromString(item.getId());

                if (ctype == null) {
                    LOGGER.error("Could not find CType for " + item.getId());
                    continue;
                }

                if (item.isFailed()) {
                    result.put(ctype, new ConfigUpdateResult("Update failed", new StandardResponse.Error(item.getFailureMessage())));
                } else {
                    result.put(ctype,
                            new ConfigUpdateResult("Update successful", item.getResponse().getPrimaryTerm() + "." + item.getResponse().getSeqNo()));
                }
            }

            if (bulkResponse.hasFailures()) {
                LOGGER.error("Index update finished with errors:\n" + Strings.toString(bulkResponse));

                List<String> documentIdsWithVersionConflictEngineException = getDocumentIdsWithVersionConflictEngineException(bulkResponse);

                if (!documentIdsWithVersionConflictEngineException.isEmpty()) {
                    if (documentIdsWithVersionConflictEngineException.size() == 1) {
                        throw new ConcurrentConfigUpdateException(
                                "The configuration " + documentIdsWithVersionConflictEngineException.get(0) + " has been concurrently modified.")
                                        .updateResult(result);
                    } else {
                        throw new ConcurrentConfigUpdateException(
                                "The configurations " + (documentIdsWithVersionConflictEngineException.stream().collect(Collectors.joining(", ")))
                                        + " have been concurrently modified.").updateResult(result);
                    }
                } else {
                    throw new ConfigUpdateException("Updating the configuration failed", bulkResponse).updateResult(result);
                }
            } else {
                LOGGER.info("Index update done:\n{}", Strings.toString(bulkResponse));
                LOGGER.info("Sending config update request");
                
                try {
                    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]));

                    ConfigUpdateResponse configUpdateResponse = privilegedConfigClient.execute(ConfigUpdateAction.INSTANCE, configUpdateRequest)
                            .actionGet();

                    if (configUpdateResponse.hasFailures()) {
                        throw new ConfigUpdateException("Configuration index was updated; however, some nodes reported failures while refreshing.",
                                configUpdateResponse).updateResult(result);
                    }
                } catch (Exception e) {
                    throw new ConfigUpdateException("Configuration index was updated; however, the refresh failed", e).updateResult(result);
                }

                return result;
            }
        } catch (ConfigUpdateException | ConcurrentConfigUpdateException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigUpdateException("Updating the configuration failed", e);
        }
    }

    @Override
    public ComponentState getComponentState() {
        componentState.updateStateFromParts();
        return componentState;
    }

    public Context getParserContext() {
        return parserContext;
    }

    private List<String> getDocumentIdsWithVersionConflictEngineException(BulkResponse bulkResponse) {
        ArrayList<String> result = new ArrayList<>();

        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.getFailure() != null && item.getFailure().getCause() instanceof VersionConflictEngineException) {
                result.add(item.getId());
            }
        }

        return result;
    }

    public static class Context implements Parser.Context, XContentParserContext {
        private final VariableResolvers variableResolvers;
        private final SearchGuardModulesRegistry searchGuardModulesRegistry;
        private final StaticSettings staticSettings;
        private final NamedXContentRegistry xContentRegistry;
        private final boolean externalResourceCreationEnabled;
        private final boolean lenientValidationEnabled;
        private final ImmutableMap<String, PipeExpression.PipeFunction> pipeFunctionsMap;

        public Context(VariableResolvers variableResolvers, SearchGuardModulesRegistry searchGuardModulesRegistry, StaticSettings staticSettings,
                NamedXContentRegistry xContentRegistry, ImmutableMap<String, PipeExpression.PipeFunction> pipeFunctionsMap) {
            this.variableResolvers = variableResolvers;
            this.searchGuardModulesRegistry = searchGuardModulesRegistry;
            this.staticSettings = staticSettings;
            this.xContentRegistry = xContentRegistry;
            this.externalResourceCreationEnabled = false;
            this.lenientValidationEnabled = true;
            this.pipeFunctionsMap = pipeFunctionsMap == null ? ImmutableMap.empty() : pipeFunctionsMap;
        }
        
        private Context(VariableResolvers variableResolvers, SearchGuardModulesRegistry searchGuardModulesRegistry, StaticSettings staticSettings,
                NamedXContentRegistry xContentRegistry, boolean externalResourceCreationEnabled, boolean lenientValidationEnabled,
            ImmutableMap<String, PipeExpression.PipeFunction> pipeFunctionsMap) {
            this.variableResolvers = variableResolvers;
            this.searchGuardModulesRegistry = searchGuardModulesRegistry;
            this.staticSettings = staticSettings;
            this.xContentRegistry = xContentRegistry;
            this.externalResourceCreationEnabled = externalResourceCreationEnabled;
            this.lenientValidationEnabled = lenientValidationEnabled;
            this.pipeFunctionsMap = pipeFunctionsMap == null ? ImmutableMap.empty() : pipeFunctionsMap;
        }


        @Override
        public VariableResolvers variableResolvers() {
            return variableResolvers;
        }

        public SearchGuardModulesRegistry modulesRegistry() {
            return searchGuardModulesRegistry;
        }

        public StaticSettings getStaticSettings() {
            return staticSettings;
        }

        @Override
        public NamedXContentRegistry xContentRegistry() {
            return xContentRegistry;
        }
        
        @Override
        public boolean isExternalResourceCreationEnabled() {
            return externalResourceCreationEnabled;
        }

        @Override
        public boolean isLenientValidationRequested() {
            return lenientValidationEnabled;
        }

        public Context withExternalResources() {
            return new Context(this.variableResolvers, this.searchGuardModulesRegistry, this.staticSettings,
                    this.xContentRegistry, true, this.lenientValidationEnabled, this.pipeFunctionsMap
            );
        }

        public Context withoutLenientValidation() {
            return new Context(this.variableResolvers, this.searchGuardModulesRegistry, this.staticSettings,
                    this.xContentRegistry, this.externalResourceCreationEnabled, false, this.pipeFunctionsMap
            );
        }

        @Override
        public ImmutableMap<String, PipeExpression.PipeFunction> pipeFunctions() {
            return pipeFunctionsMap;
        }
    }

    private static <T> void uploadFile(Client tc, File configDirectory, String fileName, String index, CType<T> cType,
            ConfigurationRepository.Context parserContext) throws ConfigUpdateException {
        File filepath = new File(configDirectory, fileName);

        LOGGER.info("Will update '" + cType + "' with " + filepath);

        try (Reader reader = new FileReader(filepath)) {

            DocNode docNode = DocNode.parse(Format.YAML).from(reader);

            if (docNode.isNull()) {
                docNode = DocNode.EMPTY;
            }

            if (cType.getArity() == CType.Arity.SINGLE) {
                docNode = DocNode.of("default", docNode.toMap());
            }

            String res = tc.index(new IndexRequest(index).id(cType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(cType.toLCString(),
                    new BytesArray(docNode.toJsonString()))).actionGet().getId();

            if (!cType.toLCString().equals(res)) {
                throw new ConfigUpdateException(
                        "   FAIL: Configuration for '" + cType.toLCString() + "' failed for unknown reasons. Pls. consult logfile of elasticsearch");
            }
        } catch (Exception e) {
            throw new ConfigUpdateException(
                    "Error while initializing configuration " + cType + " with defaults from " + filepath + ": " + e.getMessage(), e);
        }
    }

    public static class ConfigWithMetadata {
        private final Map<String, ?> content;
        private final String etag;

        public ConfigWithMetadata(Map<String, ?> content, String etag) {
            this.content = content;
            this.etag = etag;
        }

        public Map<String, ?> getContent() {
            return content;
        }

        public String getEtag() {
            return etag;
        }
    }

    public static class ConfigUpdateResult implements Document<ConfigUpdateResult> {
        private final String message;
        private final String etag;
        private final StandardResponse.Error error;

        public ConfigUpdateResult(String message, String etag) {
            this.message = message;
            this.etag = etag;
            this.error = null;
        }

        public ConfigUpdateResult(String message, StandardResponse.Error error) {
            this.message = message;
            this.etag = null;
            this.error = error;
        }

        @Override
        public Object toBasicObject() {
            return OrderedImmutableMap.ofNonNull("message", message, "etag", etag, "error", error);
        }
    }

    public Pattern getConfiguredSearchguardIndices() {
        return configuredSearchguardIndices;
    }

    public static ImmutableSet<String> getConfiguredSearchguardIndices(Settings settings) {
        StaticSettings staticSettings = new StaticSettings(settings, null);
        String configuredSearchguardIndexOld = staticSettings.get(NEW_INDEX_NAME);
        String configuredSearchguardIndexNew = staticSettings.get(OLD_INDEX_NAME);
        return ImmutableSet.of(configuredSearchguardIndexOld, configuredSearchguardIndexNew);
    }

    public ConfigsRelationsValidator getConfigsRelationsValidator() {
        return configsRelationsValidator;
    }

    public enum PatchDefaultHandling {
        FAIL_ON_MISSING_DOCUMENT, TREAT_MISSING_DOCUMENT_AS_EMPTY_DOCUMENT
    }
}