/*
 * Copyright 2015-2017 floragunn GmbH
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
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ConfigVariableProviders;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.compliance.ComplianceConfig;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.ConfigHelper;
import com.floragunn.searchguard.support.LicenseHelper;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.support.SgUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

public class ConfigurationRepository implements ComponentStateProvider {
    private static final Logger LOGGER = LogManager.getLogger(ConfigurationRepository.class);

    private final String searchguardIndex;
    private final Client client;
    private final Cache<CType, SgDynamicConfiguration<?>> configCache;
    private final List<ConfigurationChangeListener> configurationChangedListener;
    private final List<LicenseChangeListener> licenseChangeListener;
    private final ConfigurationLoaderSG7 cl;
    private final Settings settings;
    private final ClusterService clusterService;
    private final AuditLog auditLog;
    private final ComplianceConfig complianceConfig;
    private final ThreadPool threadPool;
    private volatile SearchGuardLicense effectiveLicense;
    private DynamicConfigFactory dynamicConfigFactory;
    private final int configVersion = 2;
    private final Thread bgThread;
    private final AtomicBoolean installDefaultConfig = new AtomicBoolean();
    private final ComponentState componentState = new ComponentState(1, null, "config_repository", ConfigurationRepository.class);
    private final PrivilegedConfigClient privilegedConfigClient;
    public final static Map<String, ?> SG_INDEX_MAPPING = ImmutableMap.of("dynamic_templates", Arrays.asList(ImmutableMap.of("encoded_config",
            ImmutableMap.of("match", "*", "match_mapping_type", "*", "mapping", ImmutableMap.of("type", "binary")))));

    private final static Map<String, ?> SG_INDEX_SETTINGS = ImmutableMap.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");    
    
    public ConfigurationRepository(Settings settings, final Path configPath, ThreadPool threadPool, 
            Client client, ClusterService clusterService, AuditLog auditLog, ComplianceConfig complianceConfig,
            SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigVariableProviders configVariableProviders) {
        this.searchguardIndex = settings.get(ConfigConstants.SEARCHGUARD_CONFIG_INDEX_NAME, ConfigConstants.SG_DEFAULT_CONFIG_INDEX);
        this.settings = settings;
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.auditLog = auditLog;
        this.complianceConfig = complianceConfig;
        this.configurationChangedListener = new ArrayList<>();
        this.licenseChangeListener = new ArrayList<LicenseChangeListener>();
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.componentState.setMandatory(true);
        cl = new ConfigurationLoaderSG7(client, threadPool, settings, clusterService, componentState, searchGuardModulesRegistry, configVariableProviders);
        
        configCache = CacheBuilder
                      .newBuilder()
                      .build();
        
        bgThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LOGGER.info("Background init thread started. Install default config?: "+installDefaultConfig.get());


                    if(installDefaultConfig.get()) {
                        componentState.setState(State.INITIALIZING, "install_default_config");

                        try {
                            String lookupDir = System.getProperty("sg.default_init.dir");
                            final String cd = lookupDir != null? (lookupDir+"/") : new Environment(settings, configPath).pluginsFile().toAbsolutePath().toString()+"/search-guard-7/sgconfig/";
                            File confFile = new File(cd+"sg_config.yml");
                            if(confFile.exists()) {
                                final ThreadContext threadContext = threadPool.getThreadContext();
                                try(StoredContext ctx = threadContext.stashContext()) {
                                    threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                                    LOGGER.info("Will create {} index so we can apply default config", searchguardIndex);

                                    boolean ok = client.admin().indices().create(
                                            new CreateIndexRequest(searchguardIndex).settings(SG_INDEX_SETTINGS).mapping("_doc", SG_INDEX_MAPPING))
                                            .actionGet().isAcknowledged();

                                    LOGGER.info("Index {} created?: {}", searchguardIndex, ok);
                                    if(ok) {
                                        ConfigHelper.uploadFile(client, cd+"sg_config.yml", searchguardIndex, CType.CONFIG, configVersion);
                                        ConfigHelper.uploadFile(client, cd+"sg_roles.yml", searchguardIndex, CType.ROLES, configVersion);
                                        ConfigHelper.uploadFile(client, cd+"sg_roles_mapping.yml", searchguardIndex, CType.ROLESMAPPING, configVersion);
                                        ConfigHelper.uploadFile(client, cd+"sg_internal_users.yml", searchguardIndex, CType.INTERNALUSERS, configVersion);
                                        ConfigHelper.uploadFile(client, cd+"sg_action_groups.yml", searchguardIndex, CType.ACTIONGROUPS, configVersion);
                                        if(configVersion == 2) {
                                            ConfigHelper.uploadFile(client, cd+"sg_tenants.yml", searchguardIndex, CType.TENANTS, configVersion);
                                            ConfigHelper.uploadFile(client, cd+"sg_blocks.yml", searchguardIndex, CType.BLOCKS, configVersion);
                                            ConfigHelper.uploadFile(client, cd+"sg_frontend_config.yml", searchguardIndex, CType.FRONTEND_CONFIG, configVersion);

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
                            LOGGER.error("Cannot apply default config (this is maybe not an error!) due to {}", e.getMessage());
                            // TODO find out when this is not an error o.O
                            componentState.setFailed(e);
                        }
                    }

                    LOGGER.debug("Node started, try to initialize it. Wait for at least yellow cluster state....");

                    componentState.setState(State.INITIALIZING, "waiting_for_yellow_index");

                    ClusterHealthResponse response = null;
                    try {
                        response = client.admin().cluster().health(new ClusterHealthRequest(searchguardIndex)
                                .waitForActiveShards(1)
                                .waitForYellowStatus()).actionGet();
                    } catch (Exception e1) {
                        LOGGER.debug("Catched a {} but we just try again ...", e1.toString());
                    }

                    while(response == null || response.isTimedOut() || response.getStatus() == ClusterHealthStatus.RED) {
                        LOGGER.debug("index '{}' not healthy yet, we try again ... (Reason: {})", searchguardIndex, response==null?"no response":(response.isTimedOut()?"timeout":"other, maybe red cluster"));
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e1) {
                            //ignore
                            Thread.currentThread().interrupt();
                        }
                        componentState.startNextTry();
                        try {
                            response = client.admin().cluster().health(new ClusterHealthRequest(searchguardIndex).waitForYellowStatus()).actionGet();
                        } catch (Exception e1) {
                            LOGGER.debug("Catched again a {} but we just try again ...", e1.toString());
                        }
                        continue;
                    }

                    componentState.setState(State.INITIALIZING, "loading");
                    
                    while(!dynamicConfigFactory.isInitialized()) {
                        componentState.startNextTry();
                        try {
                            LOGGER.debug("Try to load config ...");
                            reloadConfiguration(Arrays.asList(CType.values()));
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
                    LOGGER.error("Unexpected exception while initializing node "+e, e);
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
                } else if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_BACKGROUND_INIT_IF_SGINDEX_NOT_EXIST, true)){
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

    public void setDynamicConfigFactory(DynamicConfigFactory dynamicConfigFactory) {
        this.dynamicConfigFactory = dynamicConfigFactory;
    }

    /**
     * 
     * @param configurationType
     * @return can also return empty in case it was never loaded 
     */
    public SgDynamicConfiguration<?> getConfiguration(CType configurationType) {
        SgDynamicConfiguration<?> conf=  configCache.getIfPresent(configurationType);
        if(conf != null) {
            return conf.deepClone();
        }
        return SgDynamicConfiguration.empty();
    }
    
    private final Lock LOCK = new ReentrantLock();

    public void reloadConfiguration(Collection<CType> configTypes) throws ConfigUpdateAlreadyInProgressException {
        try {
            if (LOCK.tryLock(60, TimeUnit.SECONDS)) {
                try {
                    reloadConfiguration0(configTypes);
                } finally {
                    LOCK.unlock();
                }
            } else {
                throw new ConfigUpdateAlreadyInProgressException("A config update is already imn progress");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConfigUpdateAlreadyInProgressException("Interrupted config update");
        }
    }


   private void reloadConfiguration0(Collection<CType> configTypes) {
        final Map<CType, SgDynamicConfiguration<?>> loaded = getConfigurationsFromIndex(configTypes, false);
        configCache.putAll(loaded);
        notifyAboutChanges(loaded);

        final SearchGuardLicense sgLicense = getLicense();
        
        notifyAboutLicenseChanges(sgLicense);
        
        final String license = sgLicense==null?"No license needed because enterprise modules are not enabled" :sgLicense.toString();
        LOGGER.info("Search Guard License Info: "+license);

        if (sgLicense != null) {
        	LOGGER.info("Search Guard License Type: "+sgLicense.getType()+", " + (sgLicense.isValid() ? "valid" : "invalid"));

        	if (sgLicense.getExpiresInDays() <= 30 && sgLicense.isValid()) {
            	LOGGER.warn("Your Search Guard license expires in " + sgLicense.getExpiresInDays() + " days.");
            	System.out.println("Your Search Guard license expires in " + sgLicense.getExpiresInDays() + " days.");
            }

        	if (!sgLicense.isValid()) {
            	final String reasons = String.join("; ", sgLicense.getMsgs());
            	LOGGER.error("You are running an unlicensed version of Search Guard. Reason(s): " + reasons);
            	System.out.println("You are running an unlicensed version of Search Guard. Reason(s): " + reasons);
            	System.err.println("You are running an unlicensed version of Search Guard. Reason(s): " + reasons);
            }
        }
    }

    public synchronized void subscribeOnChange(ConfigurationChangeListener listener) {
        configurationChangedListener.add(listener);
    }
    
    public synchronized void subscribeOnLicenseChange(LicenseChangeListener licenseChangeListener) {
        if(licenseChangeListener != null) {
            this.licenseChangeListener.add(licenseChangeListener);
        }
    }

    private synchronized void notifyAboutLicenseChanges(SearchGuardLicense license) {
        for(LicenseChangeListener listener: this.licenseChangeListener) {
            listener.onChange(license);
        }
    }

    private synchronized void notifyAboutChanges(Map<CType, SgDynamicConfiguration<?>> typeToConfig) {
        for (ConfigurationChangeListener listener : configurationChangedListener) {
            try {
                LOGGER.debug("Notify {} listener about change configuration with type {}", listener);
                listener.onChange(typeToConfig);
            } catch (Exception e) {
                LOGGER.error("{} listener errored: "+e, listener, e);
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
    }

    /**
     * This retrieves the config directly from the index without caching involved
     * @param configTypes
     * @param logComplianceEvent
     * @return
     */
    public Map<CType, SgDynamicConfiguration<?>> getConfigurationsFromIndex(Collection<CType> configTypes, boolean logComplianceEvent) {

            final ThreadContext threadContext = threadPool.getThreadContext();
            final Map<CType, SgDynamicConfiguration<?>> retVal = new LinkedHashMap<>();

            try(StoredContext ctx = threadContext.stashContext()) {
                threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");

                IndexMetadata searchGuardMetaData = clusterService.state().getMetadata().index(this.searchguardIndex);
                MappingMetadata mappingMetaData = searchGuardMetaData==null?null:searchGuardMetaData.mapping();

                if(searchGuardMetaData !=null && mappingMetaData !=null ) {
                    if("sg".equals(mappingMetaData.type())) {
                        LOGGER.debug("sg index exists and was created before ES 7 (legacy layout)");
                    } else {
                        LOGGER.debug("sg index exists and was created with ES 7 (new layout)");
                    }
                    
                    retVal.putAll(validate(cl.load(configTypes.toArray(new CType[0]), 10, TimeUnit.SECONDS), configTypes.size()));

                } else {
                    //wait (and use new layout)
                    LOGGER.debug("sg index not exists (yet)");
                    retVal.putAll(validate(cl.load(configTypes.toArray(new CType[0]), 10, TimeUnit.SECONDS), configTypes.size()));
                }

            } catch (Exception e) {
                throw new ElasticsearchException(e);
            }
            
            if (logComplianceEvent && complianceConfig != null && complianceConfig.isEnabled()) {
                CType configurationType = configTypes.iterator().next();
                Map<String, String> fields = new HashMap<String, String>();
                fields.put(configurationType.toLCString(), Strings.toString(retVal.get(configurationType)));
                auditLog.logDocumentRead(this.searchguardIndex, configurationType.toLCString(), null, fields, complianceConfig);
            }
            
            return retVal;
    }
    
    public void update(Map<CType, Map<String, Object>> configTypeToConfigMap) throws ConfigUpdateException, ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        BulkRequest bulkRequest = new BulkRequest();

        bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        
        if (configTypeToConfigMap.isEmpty()) {
            throw new ConfigValidationException(new ValidationError(null, "No configuration given"));
        }

        for (Map.Entry<CType, Map<String, Object>> entry : configTypeToConfigMap.entrySet()) {
            CType ctype = entry.getKey();
            Map<String, Object> configMap = entry.getValue();

            if (configMap == null) {
                validationErrors.add(new InvalidAttributeValue(ctype.toLCString(), null, "A config JSON document"));
                continue;
            }

            try {
                SgDynamicConfiguration<?> configInstance = SgDynamicConfiguration.fromMap(configMap, ctype, null, null);
                configInstance.removeStatic();

                String id = ctype.toLCString();

                bulkRequest.add(new IndexRequest(this.searchguardIndex).id(id).source(id,
                        XContentHelper.toXContent(configInstance, XContentType.JSON, false)));
            } catch (ConfigValidationException e) {
                validationErrors.add(ctype.toLCString(), e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        validationErrors.throwExceptionForPresentErrors();
        
        if (!clusterService.state().getMetadata().hasConcreteIndex(searchguardIndex)) {
            boolean ok = client.admin().indices().create(
                    new CreateIndexRequest(searchguardIndex).settings(SG_INDEX_SETTINGS).mapping("_doc", SG_INDEX_MAPPING))
                    .actionGet().isAcknowledged();

            if (!ok) {
                throw new ConfigUpdateException("The creation of the Search Guard index was not acknowledged");
            }
        }

        try {

            BulkResponse bulkResponse = privilegedConfigClient.bulk(bulkRequest).actionGet();

            if (bulkResponse.hasFailures()) {
                throw new ConfigUpdateException("Updating the config failed", bulkResponse);
            }
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

    private Map<CType, SgDynamicConfiguration<?>> validate(Map<CType, SgDynamicConfiguration<?>> conf, int expectedSize) throws InvalidConfigException {

        if(conf == null || conf.size() != expectedSize) {
            throw new InvalidConfigException("Retrieved only partial configuration");
        }

        return conf;
    }

    private static String formatDate(long date) {
        return new SimpleDateFormat("yyyy-MM-dd", SgUtils.EN_Locale).format(new Date(date));
    }

    /**
     *
     * @return null if no license is needed
     */
    public SearchGuardLicense getLicense() {

        //TODO check spoof with cluster settings and elasticsearch.yml without node restart
        boolean enterpriseModulesEnabled = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true);

        if(!enterpriseModulesEnabled) {
            return null;
        }

        String licenseText = dynamicConfigFactory.getLicenseString();
        
        if(licenseText == null || licenseText.isEmpty()) {
            if(effectiveLicense != null) {
                return effectiveLicense;
            }
            return createOrGetTrial(null);
        } else {
            try {
                licenseText = LicenseHelper.validateLicense(licenseText);
                SearchGuardLicense retVal = new SearchGuardLicense(XContentHelper.convertToMap(XContentType.JSON.xContent(), licenseText, true), clusterService);
                effectiveLicense = retVal;
                return retVal;
            } catch (Exception e) {
                LOGGER.error("Unable to verify license", e);
                if(effectiveLicense != null) {
                    return effectiveLicense;
                }
                return createOrGetTrial("Unable to verify license due to "+ExceptionUtils.getRootCause(e));
            }
        }

    }

    private SearchGuardLicense createOrGetTrial(String msg) {
        
        final IndexMetadata sgIndexMetaData = clusterService.state().getMetadata().index(searchguardIndex);
        if(sgIndexMetaData == null) {
            LOGGER.error("Unable to retrieve trial license (or create  a new one) because {} index does not exist", searchguardIndex); 
            throw new RuntimeException(searchguardIndex+" does not exist");
        }
        
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Create or retrieve trial license from {} created with version {} and mapping type: {}", searchguardIndex, sgIndexMetaData.getCreationVersion(), sgIndexMetaData.mapping().type());
        }
        
        String type = "_doc";
        
        if(sgIndexMetaData.mapping().type().equals("sg")) {
            type = "sg";
        }
        
        long created = System.currentTimeMillis();
        ThreadContext threadContext = threadPool.getThreadContext();

        try(StoredContext ctx = threadContext.stashContext()) {
            threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
            GetResponse get = client.prepareGet(searchguardIndex, type, "tattr").get();
            if(get.isExists()) {
                created = (long) get.getSource().get("val");
            } else {
                try {
                    client.index(new IndexRequest(searchguardIndex)
                    .type(type)
                    .id("tattr")
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .create(true)
                    .source("{\"val\": "+System.currentTimeMillis()+"}", XContentType.JSON)).actionGet();
                } catch (VersionConflictEngineException e) {
                    //ignore
                } catch (Exception e) {
                    LOGGER.error("Unable to index tattr", e);
                }
            }
        }

        return SearchGuardLicense.createTrialLicense(formatDate(created), clusterService, msg);
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}