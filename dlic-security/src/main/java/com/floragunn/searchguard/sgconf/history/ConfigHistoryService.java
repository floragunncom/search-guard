/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import com.fasterxml.jackson.core.Base64Variants;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.ConfigModelV7;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.sgconf.StaticSgConfig;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleMappingsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ConfigHistoryService {
    private static final Logger log = LogManager.getLogger(ConfigHistoryService.class);

    public static final Setting<String> INDEX_NAME = Setting.simpleString("searchguard.config_history.index.name", ".searchguard_config_history",
            Property.NodeScope);
    public static final Setting<Integer> CACHE_TTL = Setting.intSetting("searchguard.config_history.cache.ttl", 60 * 24 * 2, Property.NodeScope);
    public static final Setting<Integer> CACHE_MAX_SIZE = Setting.intSetting("searchguard.config_history.cache.max_size", 100, Property.NodeScope);

    public static final Setting<Integer> MODEL_CACHE_TTL = Setting.intSetting("searchguard.config_history.model.cache.ttl", 60 * 24 * 2,
            Property.NodeScope);
    public static final Setting<Integer> MODEL_CACHE_MAX_SIZE = Setting.intSetting("searchguard.config_history.model.cache.max_size", 100,
            Property.NodeScope);

    private final String indexName;
    private final ConfigurationRepository configurationRepository;
    private final StaticSgConfig staticSgConfig;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final Cache<ConfigVersion, SgDynamicConfiguration<?>> configCache;
    private final Cache<ConfigVersionSet, ConfigModel> configModelCache;

    private volatile DynamicConfigModel currentDynamicConfigModel;

    private final Settings settings;

    public ConfigHistoryService(ConfigurationRepository configurationRepository, StaticSgConfig staticSgConfig,
            PrivilegedConfigClient privilegedConfigClient, ProtectedConfigIndexService protectedConfigIndexService,
            DynamicConfigFactory dynamicConfigFactory, Settings settings) {
        this.indexName = INDEX_NAME.get(settings);
        this.privilegedConfigClient = privilegedConfigClient;
        this.configurationRepository = configurationRepository;
        this.staticSgConfig = staticSgConfig;
        this.configCache = CacheBuilder.newBuilder().weakValues().build();
        this.configModelCache = CacheBuilder.newBuilder().maximumSize(MODEL_CACHE_MAX_SIZE.get(settings))
                .expireAfterAccess(MODEL_CACHE_TTL.get(settings), TimeUnit.MINUTES).build();
        this.settings = settings;

        protectedConfigIndexService.createIndex(new ConfigIndex(indexName));

        dynamicConfigFactory.registerDCFListener(dcfListener);
    }

    public ConfigSnapshot getCurrentConfigSnapshot() {
        return getCurrentConfigSnapshot(EnumSet.allOf(CType.class));
    }

    public ConfigSnapshot getCurrentConfigSnapshot(CType first, CType... rest) {
        return getCurrentConfigSnapshot(EnumSet.of(first, rest));
    }

    public ConfigSnapshot getCurrentConfigSnapshot(Set<CType> configurationTypes) {
        Map<CType, SgDynamicConfiguration<?>> configByType = new HashMap<>();

        for (CType configurationType : configurationTypes) {
            SgDynamicConfiguration<?> configuration = configurationRepository.getConfiguration(configurationType);

            if (configuration == null) {
                throw new IllegalStateException("Could not get configuration of type " + configurationType + " from configuration repository");
            }

            if (configuration.getVersion() <= 0) {
                throw new IllegalStateException("Illegal config version " + configuration.getVersion() + " in " + configuration);

            }

            configByType.put(configurationType, configuration);
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

    public ConfigSnapshot getConfigSnapshot(ConfigVersionSet configVersionSet) throws UnknownConfigVersionException {

        ConfigSnapshot configSnapshot = peekConfigSnapshot(configVersionSet);

        if (configSnapshot.hasMissingConfigVersions()) {
            throw new UnknownConfigVersionException(configSnapshot.getMissingConfigVersions());
        }

        return configSnapshot;
    }

    public Map<ConfigVersionSet, ConfigSnapshot> getConfigSnapshots(Set<ConfigVersionSet> configVersionSets) {
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

        if (missingConfigVersions.size() > 0) {

            MultiGetRequest multiGetRequest = new MultiGetRequest();

            for (ConfigVersion configurationVersion : missingConfigVersions) {
                multiGetRequest.add(indexName, configurationVersion.toId());
            }

            MultiGetResponse response = privilegedConfigClient.multiGet(multiGetRequest).actionGet();

            for (MultiGetItemResponse itemResponse : response.getResponses()) {
                if (itemResponse.getResponse() == null) {
                    if (itemResponse.getFailure() != null) {
                        if (itemResponse.getFailure().getFailure() instanceof IndexNotFoundException) {
                            continue;
                        } else {
                            log.warn("Error while retrieving configuration versions " + itemResponse + ": " + itemResponse.getFailure().getFailure());
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

            }
        }

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

    public ConfigModel getConfigSnapshotAsModel(ConfigVersionSet configVersionSet) throws UnknownConfigVersionException {

        ConfigModel configModel = configModelCache.getIfPresent(configVersionSet);

        if (configModel != null) {
            return configModel;
        }

        return createConfigModelForSnapshot(getConfigSnapshot(configVersionSet));
    }

    private ConfigModel createConfigModelForSnapshot(ConfigSnapshot configSnapshot) {
        SgDynamicConfiguration<RoleV7> roles = configSnapshot.getConfigByType(RoleV7.class).deepClone();
        SgDynamicConfiguration<RoleMappingsV7> roleMappings = configSnapshot.getConfigByType(RoleMappingsV7.class);
        SgDynamicConfiguration<ActionGroupsV7> actionGroups = configSnapshot.getConfigByType(ActionGroupsV7.class).deepClone();
        SgDynamicConfiguration<TenantV7> tenants = configSnapshot.getConfigByType(TenantV7.class).deepClone();
        SgDynamicConfiguration<BlocksV7> blocks = configSnapshot.getConfigByType(BlocksV7.class);

        if (blocks == null) {
            blocks = SgDynamicConfiguration.empty();
        }
        
        staticSgConfig.addTo(roles);
        staticSgConfig.addTo(actionGroups);
        staticSgConfig.addTo(tenants);

        ConfigModel configModel = new ConfigModelV7(roles, roleMappings, actionGroups, tenants, blocks, currentDynamicConfigModel, settings);

        configModelCache.put(configSnapshot.getConfigVersions(), configModel);

        return configModel;
    }

    public ConfigSnapshot peekConfigSnapshotFromCache(ConfigVersionSet configVersionSet) {
        Map<CType, SgDynamicConfiguration<?>> configByType = new HashMap<>();

        for (ConfigVersion configurationVersion : configVersionSet) {
            SgDynamicConfiguration<?> configuration = configCache.getIfPresent(configurationVersion);

            if (configuration != null) {
                configByType.put(configurationVersion.getConfigurationType(), configuration);
            }
        }

        return new ConfigSnapshot(configByType, configVersionSet);
    }

    public ConfigSnapshot peekConfigSnapshot(ConfigVersionSet configVersionSet) {
        Map<CType, SgDynamicConfiguration<?>> configByType = new HashMap<>();

        for (ConfigVersion configurationVersion : configVersionSet) {
            SgDynamicConfiguration<?> configuration = configCache.getIfPresent(configurationVersion);

            if (configuration != null) {
                configByType.put(configurationVersion.getConfigurationType(), configuration);
            }
        }

        if (configByType.size() != configVersionSet.size()) {
            MultiGetRequest multiGetRequest = new MultiGetRequest();

            for (ConfigVersion configurationVersion : configVersionSet) {
                if (!configByType.containsKey(configurationVersion.getConfigurationType())) {
                    multiGetRequest.add(indexName, configurationVersion.toId());
                }
            }

            MultiGetResponse response = privilegedConfigClient.multiGet(multiGetRequest).actionGet();

            for (MultiGetItemResponse itemResponse : response.getResponses()) {
                if (itemResponse.getResponse() == null) {
                    if (itemResponse.getFailure() != null) {
                        if (itemResponse.getFailure().getFailure() instanceof IndexNotFoundException) {
                            continue;
                        } else {
                            throw new ElasticsearchException("Error while retrieving configuration versions " + configVersionSet + ": "
                                    + itemResponse.getFailure().getFailure());
                        }
                    } else {
                        throw new ElasticsearchException("Error while retrieving configuration versions " + configVersionSet + ": " + itemResponse);
                    }
                }

                if (itemResponse.getResponse().isExists()) {

                    SgDynamicConfiguration<?> sgDynamicConfig = parseConfig(itemResponse.getResponse());

                    configByType.put(sgDynamicConfig.getCType(), sgDynamicConfig);
                    configCache.put(new ConfigVersion(sgDynamicConfig.getCType(), sgDynamicConfig.getDocVersion()), sgDynamicConfig);
                }

            }

        }

        return new ConfigSnapshot(configByType, configVersionSet);

    }

    public SgDynamicConfiguration<?> parseConfig(GetResponse singleGetResponse) {
        ConfigVersion configurationVersion = ConfigVersion.fromId(singleGetResponse.getId());

        Object config = singleGetResponse.getSource().get("config");

        if (!(config instanceof String)) {
            throw new IllegalStateException("Malformed config history record: " + config + "\n" + singleGetResponse.getSource());
        }

        String jsonString = new String(Base64Variants.getDefaultVariant().decode((String) config));

        try {
            return SgDynamicConfiguration.fromJson(jsonString, configurationVersion.getConfigurationType(), configurationVersion.getVersion(), 0, 0,
                    settings);
        } catch (IOException e) {
            throw new RuntimeException("Error while parsing config history record: " + jsonString + "\n" + singleGetResponse);
        }

    }

    private void storeMissingConfigDocs(ConfigVersionSet missingVersions, Map<CType, SgDynamicConfiguration<?>> configByType) {
        BulkRequestBuilder bulkRequest = privilegedConfigClient.prepareBulk().setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        for (ConfigVersion missingVersion : missingVersions) {
            SgDynamicConfiguration<?> config = configByType.get(missingVersion.getConfigurationType());
            configCache.put(missingVersion, config);
            BytesReference uninterpolatedConfigBytes = BytesReference.fromByteBuffer(ByteBuffer.wrap(config.getUninterpolatedJson().getBytes()));

            // TODO interpolated config

            bulkRequest.add(new IndexRequest(indexName).id(missingVersion.toId()).source("config",
                    uninterpolatedConfigBytes /*, "interpolated_config", config */));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            throw new RuntimeException("Failure while storing configs " + missingVersions + "; " + bulkResponse.buildFailureMessage());
        }
    }

    private final DCFListener dcfListener = new DCFListener() {

        @Override
        public void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium) {
            ConfigHistoryService.this.currentDynamicConfigModel = dcm;
        }
    };

    public String getIndexName() {
        return indexName;
    }
}
