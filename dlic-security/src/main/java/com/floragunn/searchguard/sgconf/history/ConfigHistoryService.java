/*
 * Copyright 2020-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.sgconf.history;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexNotFoundException;

import com.fasterxml.jackson.core.Base64Variants;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authc.blocking.Blocks;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.RoleMapping;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.ExceptionRecord;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ConfigHistoryService implements ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(ConfigHistoryService.class);

    public static final StaticSettings.Attribute<String> INDEX_NAME = StaticSettings.Attribute.define("searchguard.config_history.index.name")
            .withDefault(".searchguard_config_history").asString();
    public static final StaticSettings.Attribute<Integer> CACHE_TTL = StaticSettings.Attribute.define("searchguard.config_history.cache.ttl")
            .withDefault(60 * 24 * 2).asInteger();
    public static final StaticSettings.Attribute<Integer> CACHE_MAX_SIZE = StaticSettings.Attribute
            .define("searchguard.config_history.cache.max_size").withDefault(100).asInteger();
    public static final StaticSettings.Attribute<Integer> MODEL_CACHE_TTL = StaticSettings.Attribute
            .define("searchguard.config_history.model.cache.ttl").withDefault(60 * 24 * 2).asInteger();
    public static final StaticSettings.Attribute<Integer> MODEL_CACHE_MAX_SIZE = StaticSettings.Attribute
            .define("searchguard.config_history.model.cache.max_size").withDefault(100).asInteger();

    private final String indexName;
    private final ConfigurationRepository configurationRepository;
    private final StaticSgConfig staticSgConfig;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final Actions actions;
    private final Cache<ConfigVersion, SgDynamicConfiguration<?>> configCache;
    private final Cache<ConfigVersionSet, ConfigModel> configModelCache;

    private final ComponentState componentState = new ComponentState(1000, null, "config_history_service", ConfigHistoryService.class);

    private final PrivilegesEvaluator privilegesEvaluator;

    public ConfigHistoryService(ConfigurationRepository configurationRepository, StaticSgConfig staticSgConfig,
            PrivilegedConfigClient privilegedConfigClient, ProtectedConfigIndexService protectedConfigIndexService, Actions actions,
            StaticSettings settings, PrivilegesEvaluator privilegesEvaluator) {
        this.indexName = settings.get(INDEX_NAME);
        this.privilegedConfigClient = privilegedConfigClient;
        this.configurationRepository = configurationRepository;
        this.staticSgConfig = staticSgConfig;
        this.actions = actions;
        this.configCache = CacheBuilder.newBuilder().weakValues().build();
        this.configModelCache = CacheBuilder.newBuilder().maximumSize(settings.get(MODEL_CACHE_MAX_SIZE))
                .expireAfterAccess(settings.get(MODEL_CACHE_TTL), TimeUnit.MINUTES).build();
        this.privilegesEvaluator = privilegesEvaluator;

        componentState.addPart(protectedConfigIndexService.createIndex(new ConfigIndex(indexName).onIndexReady((f) -> {
            f.onSuccess();
            componentState.setInitialized();
        })));

    }

    public ConfigSnapshot getCurrentConfigSnapshot() {
        return getCurrentConfigSnapshot(CType.all());
    }

    public ConfigSnapshot getCurrentConfigSnapshot(CType<?> first, CType<?>... rest) {
        return getCurrentConfigSnapshot(CType.of(first, rest));
    }

    public ConfigSnapshot getCurrentConfigSnapshot(Set<CType<?>> configurationTypes) {
        Map<CType<?>, SgDynamicConfiguration<?>> configByType = new HashMap<>();

        for (CType<?> configurationType : configurationTypes) {
            SgDynamicConfiguration<?> configuration = configurationRepository.getConfiguration(configurationType);

            if (configuration == null) {
                throw new IllegalStateException("Could not get configuration of type " + configurationType + " from configuration repository");
            }

            configByType.put(configurationType, configuration.withoutStatic());
        }

        ConfigVersionSet configVersionSet = ConfigVersionSet.from(configByType);
        ConfigSnapshot existingConfigSnapshots = peekConfigSnapshot(configVersionSet);

        if (existingConfigSnapshots.hasMissingConfigVersions()) {
            log.info("Storing missing config versions: " + existingConfigSnapshots.getMissingConfigVersions());
            storeMissingConfigDocs(existingConfigSnapshots.getMissingConfigVersions(), configByType);
            return new ConfigSnapshot(configByType);
        } else {
            return existingConfigSnapshots;
        }
    }

    public void getConfigSnapshot(ConfigVersionSet configVersionSet, Consumer<ConfigSnapshot> onResult, Consumer<Exception> onFailure) {

        peekConfigSnapshot(configVersionSet, (configSnapshot) -> {

            if (configSnapshot.hasMissingConfigVersions()) {
                onFailure.accept(new UnknownConfigVersionException(configSnapshot.getMissingConfigVersions()));
            } else {
                onResult.accept(configSnapshot);
            }
        }, onFailure);

    }

    public void getConfigSnapshots(Set<ConfigVersionSet> configVersionSets, Consumer<Map<ConfigVersionSet, ConfigSnapshot>> onResult,
            Consumer<Exception> onFailure) {
        Map<ConfigVersion, SgDynamicConfiguration<?>> configVersionMap = new HashMap<>(configVersionSets.size() * 2);
        Set<ConfigVersion> missingConfigVersions = new HashSet<>(configVersionSets.size() * 2);

        for (ConfigVersionSet configVersionSet : configVersionSets) {
            for (ConfigVersion configurationVersion : configVersionSet) {
                SgDynamicConfiguration<?> configuration = configCache.getIfPresent(configurationVersion);

                if (configuration != null) {
                    configVersionMap.put(configurationVersion, configuration);
                } else {
                    missingConfigVersions.add(configurationVersion);
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("missingConfigVersions: " + missingConfigVersions.size());
        }

        if (missingConfigVersions.size() == 0) {
            onResult.accept(buildConfigSnapshotResultMap(configVersionSets, configVersionMap));
        } else {
            MultiGetRequest multiGetRequest = new MultiGetRequest();

            for (ConfigVersion configurationVersion : missingConfigVersions) {
                multiGetRequest.add(indexName, configurationVersion.toId());
            }

            privilegedConfigClient.multiGet(multiGetRequest, new ActionListener<MultiGetResponse>() {

                @Override
                public void onResponse(MultiGetResponse response) {
                    try {
                        for (MultiGetItemResponse itemResponse : response.getResponses()) {
                            if (itemResponse.getResponse() == null) {
                                if (itemResponse.getFailure() != null) {
                                    if (itemResponse.getFailure().getFailure() instanceof IndexNotFoundException) {
                                        continue;
                                    } else {
                                        log.warn("Error while retrieving configuration versions " + itemResponse + ": "
                                                + itemResponse.getFailure().getFailure());
                                    }
                                } else {
                                    log.warn("Error while retrieving configuration versions " + itemResponse);
                                }
                                continue;
                            }

                            if (itemResponse.getResponse().isExists()) {

                                SgDynamicConfiguration<?> sgDynamicConfig = parseConfig(itemResponse.getResponse());
                                ConfigVersion configVersion = new ConfigVersion(sgDynamicConfig.getCType(), sgDynamicConfig.getDocVersion());

                                configVersionMap.put(configVersion, sgDynamicConfig);
                                configCache.put(configVersion, sgDynamicConfig);
                            }

                            onResult.accept(buildConfigSnapshotResultMap(configVersionSets, configVersionMap));
                        }
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    onFailure(e);
                }
            });

        }
    }

    private Map<ConfigVersionSet, ConfigSnapshot> buildConfigSnapshotResultMap(Set<ConfigVersionSet> configVersionSets,
            Map<ConfigVersion, SgDynamicConfiguration<?>> configVersionMap) {
        Map<ConfigVersionSet, ConfigSnapshot> result = new HashMap<>(configVersionSets.size());

        for (ConfigVersionSet configVersionSet : configVersionSets) {
            ConfigSnapshot configSnapshot = peekConfigSnapshotFromCache(configVersionSet);

            if (configSnapshot.hasMissingConfigVersions()) {
                log.error("Could not completely load " + configVersionSet + ". Missing: " + configSnapshot.getMissingConfigVersions());
                continue;
            }

            result.put(configVersionSet, configSnapshot);
        }

        return result;
    }

    public ConfigModel getConfigModelForSnapshot(ConfigSnapshot configSnapshot) {
        ConfigVersionSet configVersionSet = configSnapshot.getConfigVersions();

        ConfigModel configModel = configModelCache.getIfPresent(configVersionSet);

        if (configModel != null) {
            return configModel;
        }

        return createConfigModelForSnapshot(configSnapshot);
    }

    private ConfigModel createConfigModelForSnapshot(ConfigSnapshot configSnapshot) {
        SgDynamicConfiguration<Role> roles = configSnapshot.getConfigByType(Role.class);
        SgDynamicConfiguration<RoleMapping> roleMappings = configSnapshot.getConfigByType(RoleMapping.class);
        SgDynamicConfiguration<ActionGroup> actionGroups = configSnapshot.getConfigByType(ActionGroup.class);
        SgDynamicConfiguration<Tenant> tenants = configSnapshot.getConfigByType(Tenant.class);
        SgDynamicConfiguration<Blocks> blocks = configSnapshot.getConfigByType(Blocks.class);

        if (blocks == null) {
            blocks = SgDynamicConfiguration.empty(CType.BLOCKS);
        }

        roles = staticSgConfig.addTo(roles);
        actionGroups = staticSgConfig.addTo(actionGroups);
        tenants = staticSgConfig.addTo(tenants);

        ConfigModel configModel = new ConfigModel(roles, roleMappings, actionGroups, tenants, blocks, actions, privilegesEvaluator.getResolver(),
                privilegesEvaluator.getClusterService());

        configModelCache.put(configSnapshot.getConfigVersions(), configModel);

        return configModel;
    }

    public ConfigSnapshot peekConfigSnapshotFromCache(ConfigVersionSet configVersionSet) {
        Map<CType<?>, SgDynamicConfiguration<?>> configByType = new HashMap<>();

        for (ConfigVersion configurationVersion : configVersionSet) {
            SgDynamicConfiguration<?> configuration = configCache.getIfPresent(configurationVersion);

            if (configuration != null) {
                configByType.put(configurationVersion.getConfigurationType(), configuration);
            }
        }

        return new ConfigSnapshot(configByType, configVersionSet);
    }

    public ConfigSnapshot peekConfigSnapshot(ConfigVersionSet configVersionSet) {
        CompletableFuture<ConfigSnapshot> completableFuture = new CompletableFuture<>();

        peekConfigSnapshot(configVersionSet, completableFuture::complete, completableFuture::completeExceptionally);

        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public void peekConfigSnapshot(ConfigVersionSet configVersionSet, Consumer<ConfigSnapshot> onResult, Consumer<Exception> onFailure) {
        try {
            Map<CType<?>, SgDynamicConfiguration<?>> configByType = new HashMap<>();

            for (ConfigVersion configurationVersion : configVersionSet) {
                SgDynamicConfiguration<?> configuration = configCache.getIfPresent(configurationVersion);

                if (configuration != null) {
                    configByType.put(configurationVersion.getConfigurationType(), configuration);
                }
            }

            if (configByType.size() == configVersionSet.size()) {
                onResult.accept(new ConfigSnapshot(configByType, configVersionSet));
            } else {
                MultiGetRequest multiGetRequest = new MultiGetRequest();

                for (ConfigVersion configurationVersion : configVersionSet) {
                    if (!configByType.containsKey(configurationVersion.getConfigurationType())) {
                        multiGetRequest.add(indexName, configurationVersion.toId());
                    }
                }

                privilegedConfigClient.multiGet(multiGetRequest, new ActionListener<MultiGetResponse>() {

                    @Override
                    public void onResponse(MultiGetResponse response) {
                        try {
                            for (MultiGetItemResponse itemResponse : response.getResponses()) {
                                if (itemResponse.getResponse() == null) {
                                    if (itemResponse.getFailure() != null) {
                                        if (itemResponse.getFailure().getFailure() instanceof IndexNotFoundException) {
                                            continue;
                                        } else {
                                            throw new ElasticsearchException("Error while retrieving configuration versions " + configVersionSet
                                                    + ": " + itemResponse.getFailure().getFailure());
                                        }
                                    } else {
                                        throw new ElasticsearchException(
                                                "Error while retrieving configuration versions " + configVersionSet + ": " + itemResponse);
                                    }
                                }

                                if (itemResponse.getResponse().isExists()) {
                                    SgDynamicConfiguration<?> sgDynamicConfig = parseConfig(itemResponse.getResponse());
                                    configByType.put(sgDynamicConfig.getCType(), sgDynamicConfig);
                                    configCache.put(new ConfigVersion(sgDynamicConfig.getCType(), sgDynamicConfig.getDocVersion()), sgDynamicConfig);
                                }

                            }

                            onResult.accept(new ConfigSnapshot(configByType, configVersionSet));
                        } catch (Exception e) {
                            onFailure.accept(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailure.accept(e);
                    }
                });
            }
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    public SgDynamicConfiguration<?> parseConfig(GetResponse singleGetResponse) throws ConfigValidationException {
        ConfigVersion configurationVersion = ConfigVersion.fromId(singleGetResponse.getId());

        Object config = singleGetResponse.getSource().get("config");

        if (!(config instanceof String)) {
            throw new IllegalStateException("Malformed config history record: " + config + "\n" + singleGetResponse.getSource());
        }

        String jsonString = new String(Base64Variants.getDefaultVariant().decode((String) config));

        try {
            return SgDynamicConfiguration.fromJson(jsonString, configurationVersion.getConfigurationType(), configurationVersion.getVersion(), 0, 0,
                    configurationRepository.getParserContext()).get();
        } catch (Exception e) {
            componentState.addLastException("parseConfig", new ExceptionRecord(e, "Error while parsing config history record"));
            throw new RuntimeException("Error while parsing config history record: " + jsonString + "\n" + singleGetResponse);
        }

    }

    private void storeMissingConfigDocs(ConfigVersionSet missingVersions, Map<CType<?>, SgDynamicConfiguration<?>> configByType) {
        BulkRequestBuilder bulkRequest = privilegedConfigClient.prepareBulk().setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        for (ConfigVersion missingVersion : missingVersions) {
            SgDynamicConfiguration<?> config = configByType.get(missingVersion.getConfigurationType());
            configCache.put(missingVersion, config);
            BytesReference uninterpolatedConfigBytes = BytesReference.fromByteBuffer(ByteBuffer.wrap(config.getUninterpolatedJson().getBytes()));

            bulkRequest.add(new IndexRequest(indexName).id(missingVersion.toId()).source("config", uninterpolatedConfigBytes));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            throw new RuntimeException("Failure while storing configs " + missingVersions + "; " + bulkResponse.buildFailureMessage());
        }
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
