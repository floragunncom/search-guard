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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.threadpool.ThreadPool;

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
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.variables.ConfigVarService;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.ComponentState.State;

public class ConfigurationRepository implements ComponentStateProvider {
    private static final Logger LOGGER = LogManager.getLogger(ConfigurationRepository.class);

    private final String searchguardIndex;
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

    private final Settings settings;
    private final ClusterService clusterService;
    private final Thread bgThread;
    private final AtomicBoolean installDefaultConfig = new AtomicBoolean();
    private final ComponentState componentState = new ComponentState(-1000, null, "config_repository", ConfigurationRepository.class);
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ThreadPool threadPool;

    public final static Map<String, ?> SG_INDEX_MAPPING = ImmutableMap.of("dynamic_templates", Arrays.asList(ImmutableMap.of("encoded_config",
            ImmutableMap.of("match", "*", "match_mapping_type", "*", "mapping", ImmutableMap.of("type", "binary")))));

    private final static Map<String, ?> SG_INDEX_SETTINGS = ImmutableMap.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");

    private final VariableResolvers variableResolvers;
    private final Context parserContext;

    public ConfigurationRepository(Settings settings, Path configPath, ThreadPool threadPool, Client client, ClusterService clusterService,
            ConfigVarService configVarService, SearchGuardModulesRegistry modulesRegistry, StaticSgConfig staticSgConfig) {
        this.searchguardIndex = settings.get(ConfigConstants.SEARCHGUARD_CONFIG_INDEX_NAME, ConfigConstants.SG_DEFAULT_CONFIG_INDEX);
        this.settings = settings;
        this.client = client;
        this.clusterService = clusterService;
        this.configurationChangedListener = new ArrayList<>();
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.componentState.setMandatory(true);
        this.mainConfigLoader = new ConfigurationLoader(client, settings, componentState, this, staticSgConfig);
        this.externalUseConfigLoader = new ConfigurationLoader(client, settings, null, this, null);
        this.variableResolvers = VariableResolvers.ALL_PRIVILEGED.with("var", (key) -> configVarService.get(key));
        this.parserContext = new Context(variableResolvers, modulesRegistry, settings, configPath);
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

        bgThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LOGGER.info("Background init thread started. Install default config?: " + installDefaultConfig.get());

                    if (installDefaultConfig.get()) {
                        componentState.setState(State.INITIALIZING, "install_default_config");

                        try {
                            String lookupDir = System.getProperty("sg.default_init.dir");
                            final String cd = lookupDir != null ? (lookupDir + "/")
                                    : new Environment(settings, configPath).pluginsFile().toAbsolutePath().toString() + "/search-guard-flx/sgconfig/";
                            File confFile = new File(cd + "sg_authc.yml");
                            File legacyConfFile = new File(cd + "sg_config.yml");

                            if (confFile.exists() || legacyConfFile.exists()) {
                                final ThreadContext threadContext = threadPool.getThreadContext();
                                try (StoredContext ctx = threadContext.stashContext()) {
                                    threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                                    LOGGER.info("Will create {} index so we can apply default config", searchguardIndex);

                                    boolean ok = client.admin().indices().create(
                                            new CreateIndexRequest(searchguardIndex).settings(SG_INDEX_SETTINGS).mapping("_doc", SG_INDEX_MAPPING))
                                            .actionGet().isAcknowledged();

                                    LOGGER.info("Index {} created?: {}", searchguardIndex, ok);
                                    if (ok) {
                                        if (new File(cd + "sg_authc.yml").exists()) {
                                            uploadFile(client, cd + "sg_authc.yml", searchguardIndex, CType.AUTHC, parserContext);
                                        } else if (new File(cd + "sg_config.yml").exists()) {
                                            uploadFile(client, cd + "sg_config.yml", searchguardIndex, CType.CONFIG, parserContext);
                                        }

                                        uploadFile(client, cd + "sg_roles.yml", searchguardIndex, CType.ROLES, parserContext);
                                        uploadFile(client, cd + "sg_roles_mapping.yml", searchguardIndex, CType.ROLESMAPPING, parserContext);
                                        uploadFile(client, cd + "sg_internal_users.yml", searchguardIndex, CType.INTERNALUSERS, parserContext);
                                        uploadFile(client, cd + "sg_action_groups.yml", searchguardIndex, CType.ACTIONGROUPS, parserContext);
                                        uploadFile(client, cd + "sg_tenants.yml", searchguardIndex, CType.TENANTS, parserContext);
                                        uploadFile(client, cd + "sg_blocks.yml", searchguardIndex, CType.BLOCKS, parserContext);
                                        uploadFile(client, cd + "sg_frontend_authc.yml", searchguardIndex, CType.FRONTEND_AUTHC, parserContext);

                                        if (new File(cd + "sg_authc_transport.yml").exists()) {
                                            uploadFile(client, cd + "sg_authc_transport.yml", searchguardIndex, CType.AUTHC_TRANSPORT, parserContext);
                                        }

                                        if (new File(cd + "sg_authz.yml").exists()) {
                                            uploadFile(client, cd + "sg_authz.yml", searchguardIndex, CType.AUTHZ, parserContext);
                                        }

                                        if (new File(cd + "sg_license_key.yml").exists()) {
                                            uploadFile(client, cd + "sg_license_key.yml", searchguardIndex, CType.LICENSE_KEY, parserContext);
                                        }

                                        LOGGER.info("Default config applied");
                                    } else {
                                        LOGGER.error("Can not create {} index", searchguardIndex);
                                        componentState.setFailed("Index creation was not acknowledged");
                                    }
                                }
                            } else {
                                LOGGER.error("{} does not exist", confFile.getAbsolutePath());
                                componentState.setFailed(confFile.getAbsolutePath() + " does not exist");

                            }
                        } catch (ResourceAlreadyExistsException e) {
                            LOGGER.debug("Cannot apply default config (this is maybe not an error!) due to {}", e.getMessage());
                        } catch (Exception e) {
                            LOGGER.error("Cannot apply default config (this is maybe not an error!) due to {}", e.getMessage(), e);
                            // TODO find out when this is not an error o.O
                            componentState.setFailed(e);
                        }
                    }

                    LOGGER.debug("Node started, try to initialize it. Wait for at least yellow cluster state....");

                    componentState.setState(State.INITIALIZING, "waiting_for_yellow_index");

                    ClusterHealthResponse response = null;
                    try {
                        response = client.admin().cluster()
                                .health(new ClusterHealthRequest(searchguardIndex).waitForActiveShards(1).waitForYellowStatus()).actionGet();
                    } catch (Exception e1) {
                        LOGGER.debug("Catched a {} but we just try again ...", e1.toString());
                    }

                    while (response == null || response.isTimedOut() || response.getStatus() == ClusterHealthStatus.RED) {
                        LOGGER.debug("index '{}' not healthy yet, we try again ... (Reason: {})", searchguardIndex,
                                response == null ? "no response" : (response.isTimedOut() ? "timeout" : "other, maybe red cluster"));
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e1) {
                            //ignore
                            Thread.currentThread().interrupt();
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
        });

    }

    public void initOnNodeStart() {

        LOGGER.info("Check if " + searchguardIndex + " index exists ...");

        try {

            if (clusterService.state().getMetadata().hasConcreteIndex(searchguardIndex)) {
                LOGGER.info("{} index does already exist, so we try to load the config from it", searchguardIndex);
                bgThread.start();
            } else {
                if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ALLOW_DEFAULT_INIT_SGINDEX, false)) {
                    LOGGER.info("{} index does not exist yet, so we create a default config", searchguardIndex);
                    installDefaultConfig.set(true);
                    bgThread.start();
                } else if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_BACKGROUND_INIT_IF_SGINDEX_NOT_EXIST, true)) {
                    LOGGER.info("{} index does not exist yet, so no need to load config on node startup. Use sgadmin to initialize cluster",
                            searchguardIndex);
                    bgThread.start();
                } else {
                    LOGGER.info("{} index does not exist yet, use sgadmin to initialize the cluster. We will not perform background initialization",
                            searchguardIndex);
                }
            }

        } catch (Throwable e2) {
            LOGGER.error("Error during node initialization: {}", e2, e2);
            bgThread.start();
            componentState.addLastException("initOnNodeStart", e2);
        }
    }

    /**
     * 
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

    private final Lock LOCK = new ReentrantLock();

    public void reloadConfiguration(Set<CType<?>> configTypes, String reason)
            throws ConfigUpdateAlreadyInProgressException, ConfigUnavailableException {
        try {
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

    private void reloadConfiguration0(Set<CType<?>> configTypes, String reason) throws ConfigUnavailableException {
        try {
            ConfigMap loadedConfig = mainConfigLoader.load(configTypes, reason).get();
            ConfigMap discardedConfig;

            if (this.currentConfig == null) {
                this.currentConfig = loadedConfig;
                discardedConfig = null;
            } else {
                ConfigMap oldConfig = this.currentConfig;
                ConfigMap mergedConfig = oldConfig.with(loadedConfig);
                discardedConfig = oldConfig.only(loadedConfig.getTypes());

                this.currentConfig = mergedConfig;
            }

            notifyAboutChanges(this.currentConfig);

            if (discardedConfig != null && !discardedConfig.isEmpty()) {
                this.threadPool.schedule(() -> {
                    LOGGER.info("Destroying old configuration: " + discardedConfig);
                    discardedConfig.destroy();
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
        return externalUseConfigLoader.loadSync(configType, reason);
    }

    /**
     * This retrieves the config directly from the index without caching involved
     * 
     * @param configTypes
     * @return
     * @throws ConfigUnavailableException
     */
    public ConfigMap getConfigurationsFromIndex(Set<CType<?>> configTypes, String reason) throws ConfigUnavailableException {
        return externalUseConfigLoader.loadSync(configTypes, reason);
    }

    public <T> SgConfigEntry<T> getConfigEntryFromIndex(CType<T> configType, String id, String reason)
            throws ConfigUnavailableException, NoSuchConfigEntryException {
        SgDynamicConfiguration<T> baseConfig = getConfigurationFromIndex(configType, reason);

        T entry = baseConfig.getCEntry(id);

        if (entry != null) {
            return new SgConfigEntry<T>(entry, baseConfig);
        } else {
            throw new NoSuchConfigEntryException(configType, id);
        }
    }

    public <T> StandardResponse addOrUpdate(CType<T> ctype, String id, T entry, String matchETag)
            throws ConfigUpdateException, ConcurrentConfigUpdateException {
        try {
            SgDynamicConfiguration<T> configInstance = getConfigurationFromIndex(ctype, "Update of entry " + id);

            if (matchETag != null && !configInstance.getETag().equals(matchETag)) {
                throw new ConcurrentConfigUpdateException("Unable to update configuration due to concurrent modification:\nIf-Match: " + matchETag
                        + "; ETag: " + configInstance.getETag());
            }

            boolean alreadyExists = configInstance.getCEntry(id) != null;

            configInstance.putCEntry(id, entry);

            update(ctype, configInstance, matchETag);

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
                return applyPatch(configType, "default", patch, matchETag);
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
                        ValidationResult<T> parsedEntry = configType.getParser().parse(entry.getValue(), parserContext);

                        if (parsedEntry.hasErrors()) {
                            validationErrors.add(entry.getKey(), parsedEntry.getValidationErrors());
                        } else {
                            result.put(entry.getKey(), parsedEntry.peek());
                        }
                    }

                    validationErrors.throwExceptionForPresentErrors();

                    return result;
                }
            };

            configInstance.setEntries(doc.patch(patch, parserContext));

            update(configType, configInstance, matchETag);

            return new StandardResponse(200).message(configType.getUiName() + " has been updated");

        } catch (ConfigUnavailableException | DocUpdateException e) {
            throw new ConfigUpdateException(e);
        }
    }

    public <T> StandardResponse applyPatch(CType<T> configType, String id, DocPatch patch, String matchETag)
            throws ConfigUpdateException, ConcurrentConfigUpdateException, NoSuchConfigEntryException, ConfigValidationException {
        try {
            SgDynamicConfiguration<T> configInstance = getConfigurationFromIndex(configType, "Update of entry " + id);

            if (matchETag != null && !configInstance.getETag().equals(matchETag)) {
                throw new ConcurrentConfigUpdateException("Unable to update configuration due to concurrent modification:\nIf-Match: " + matchETag
                        + "; ETag: " + configInstance.getETag());
            }

            @SuppressWarnings("unchecked")
            Document<T> document = (Document<T>) configInstance.getCEntry(id);

            if (document == null) {
                throw new NoSuchConfigEntryException(configType, id);
            }

            if (!(document instanceof PatchableDocument)) {
                throw new ConfigUpdateException("The config type " + configType + " cannot be patched");
            }

            PatchableDocument<T> entry = (PatchableDocument<T>) document;

            configInstance.putCEntry(id, entry.patch(patch, parserContext));

            update(configType, configInstance, matchETag);

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

            configInstance.remove(id);

            update(configType, configInstance, null);

            return new StandardResponse(200).message(configType.getUiName() + " " + id + " has been deleted");
        } catch (ConfigUnavailableException e) {
            throw new ConfigUpdateException(e);
        }
    }

    public <T> void update(CType<T> ctype, SgDynamicConfiguration<T> configInstance, String matchETag)
            throws ConfigUpdateException, ConcurrentConfigUpdateException {

        IndexRequest indexRequest = new IndexRequest(this.searchguardIndex);

        try {
            configInstance.removeStatic();

            String id = ctype.toLCString();

            indexRequest = indexRequest.id(id).source(id, XContentHelper.toXContent(configInstance, XContentType.JSON, false))
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

        if (!clusterService.state().getMetadata().hasIndex(searchguardIndex)) {
            boolean ok = privilegedConfigClient.admin().indices()
                    .create(new CreateIndexRequest(searchguardIndex).settings(SG_INDEX_SETTINGS).mapping("_doc", SG_INDEX_MAPPING)).actionGet()
                    .isAcknowledged();

            if (!ok) {
                throw new ConfigUpdateException("The creation of the Search Guard index was not acknowledged");
            }
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
        ValidationErrors validationErrors = new ValidationErrors();
        BulkRequest bulkRequest = new BulkRequest();

        bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        if (configTypeToConfigMap.isEmpty()) {
            throw new ConfigValidationException(new ValidationError(null, "No configuration given"));
        }

        List<String> configTypesWithConcurrentModifications = new ArrayList<>();

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
                SgDynamicConfiguration<?> configInstance = SgDynamicConfiguration.fromMap(configMap, ctype, parserContext);
                configInstance.removeStatic();

                if (configInstance.getValidationErrors() != null && configInstance.getValidationErrors().hasErrors()) {
                    validationErrors.add(ctype.toLCString(), configInstance.getValidationErrors());
                    continue;
                }

                String id = ctype.toLCString();

                IndexRequest indexRequest = new IndexRequest(this.searchguardIndex).id(id).source(id,
                        XContentHelper.toXContent(configInstance, XContentType.JSON, false));

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
            } catch (ConfigValidationException e) {
                validationErrors.add(ctype.toLCString(), e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!configTypesWithConcurrentModifications.isEmpty()) {
            if (configTypesWithConcurrentModifications.size() == 1) {
                throw new ConcurrentConfigUpdateException(
                        "The configuration " + configTypesWithConcurrentModifications.get(0) + " has been concurrently modified.");
            } else {
                throw new ConcurrentConfigUpdateException("The configurations "
                        + (configTypesWithConcurrentModifications.stream().collect(Collectors.joining(", "))) + " have been concurrently modified.");
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        if (!clusterService.state().getMetadata().hasIndex(searchguardIndex)) {
            boolean ok = client.admin().indices()
                    .create(new CreateIndexRequest(searchguardIndex).settings(SG_INDEX_SETTINGS).mapping("_doc", SG_INDEX_MAPPING)).actionGet()
                    .isAcknowledged();

            if (!ok) {
                throw new ConfigUpdateException("The creation of the Search Guard index was not acknowledged");
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
                                "The configuration " + documentIdsWithVersionConflictEngineException.get(0) + " has been concurrently modified.").updateResult(result);
                    } else {
                        throw new ConcurrentConfigUpdateException(
                                "The configurations " + (documentIdsWithVersionConflictEngineException.stream().collect(Collectors.joining(", ")))
                                        + " have been concurrently modified.").updateResult(result);
                    }
                } else {
                    throw new ConfigUpdateException("Updating the configuration failed", bulkResponse).updateResult(result);
                }
            } else {
                LOGGER.info("Index update done:\n" + Strings.toString(bulkResponse));
                
                try {
                    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]));

                    ConfigUpdateResponse configUpdateResponse = privilegedConfigClient.execute(ConfigUpdateAction.INSTANCE, configUpdateRequest).actionGet();

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

    public String getSearchguardIndex() {
        return searchguardIndex;
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

    public static class Context implements Parser.Context {
        private final VariableResolvers variableResolvers;
        private final SearchGuardModulesRegistry searchGuardModulesRegistry;
        private final Settings esSettings;
        private final Path configPath;

        public Context(VariableResolvers variableResolvers, SearchGuardModulesRegistry searchGuardModulesRegistry, Settings esSettings,
                Path configPath) {
            this.variableResolvers = variableResolvers;
            this.searchGuardModulesRegistry = searchGuardModulesRegistry;
            this.esSettings = esSettings;
            this.configPath = configPath;
        }

        @Override
        public VariableResolvers variableResolvers() {
            return variableResolvers;
        }

        public SearchGuardModulesRegistry modulesRegistry() {
            return searchGuardModulesRegistry;
        }

        public Settings getEsSettings() {
            return esSettings;
        }

        public Path getConfigPath() {
            return configPath;
        }

    }

    private static <T> void uploadFile(Client tc, String filepath, String index, CType<T> cType, ConfigurationRepository.Context parserContext)
            throws Exception {
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
                throw new Exception(
                        "   FAIL: Configuration for '" + cType.toLCString() + "' failed for unknown reasons. Pls. consult logfile of elasticsearch");
            }
        } catch (Exception e) {
            throw e;
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
}