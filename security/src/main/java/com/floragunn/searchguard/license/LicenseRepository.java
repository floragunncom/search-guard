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

package com.floragunn.searchguard.license;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.engine.VersionConflictEngineException;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchguard.support.SgUtils;

public class LicenseRepository implements ComponentStateProvider {
    private static final Logger LOGGER = LogManager.getLogger(LicenseRepository.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final List<LicenseChangeListener> licenseChangeListeners = new ArrayList<LicenseChangeListener>();
    private final PrivilegedConfigClient privilegedClient;
    private final String searchguardIndex;
    private final ComponentState componentState = new ComponentState(2, null, "license_repository", LicenseRepository.class);

    private volatile SearchGuardLicenseKey effectiveLicense;
    private volatile SearchGuardLicenseKey configuredLicense;
    private volatile ValidationErrors validationErrors;

    public LicenseRepository(Settings settings, Client client, ClusterService clusterService, ConfigurationRepository configurationRepository) {
        this.settings = settings;
        this.searchguardIndex = settings.get(ConfigConstants.SEARCHGUARD_CONFIG_INDEX_NAME, ConfigConstants.SG_DEFAULT_CONFIG_INDEX);
        this.clusterService = clusterService;
        this.privilegedClient = PrivilegedConfigClient.adapt(client);

        configurationRepository.subscribeOnChange((configMap) -> {
            SgDynamicConfiguration<SearchGuardLicenseKey> config = configMap.get(CType.LICENSE_KEY);
            SgDynamicConfiguration<LegacySgConfig> legacyConfig = configMap.get(CType.CONFIG);

            if (config != null && config.getCEntry("default") != null) {
                useLicense(config.getCEntry("default"), config);
            } else if (legacyConfig != null && legacyConfig.getCEntry("sg_config") != null) {
                useLicense(legacyConfig.getCEntry("sg_config").getLicense(), legacyConfig);
            } else {
                useLicense(null, null);
            }
        });
    }

    public synchronized void subscribeOnLicenseChange(LicenseChangeListener licenseChangeListener) {
        if (licenseChangeListener != null) {
            this.licenseChangeListeners.add(licenseChangeListener);
        }
    }

    /**
     *
     * @return null if no license is needed
     */
    public SearchGuardLicenseKey getLicense() {
        if (configuredLicense != null) {
            configuredLicense.dynamicValidate(clusterService);
            return configuredLicense;
        } else if (effectiveLicense != null) {
            effectiveLicense.dynamicValidate(clusterService);
            return effectiveLicense;
        } else {
            return null;
        }
    }

    private SearchGuardLicenseKey createOrGetTrial(String msg) {

        final IndexMetadata sgIndexMetaData = clusterService.state().getMetadata().index(searchguardIndex);
        if (sgIndexMetaData == null) {
            LOGGER.error("Unable to retrieve trial license (or create  a new one) because {} index does not exist", searchguardIndex);
            throw new RuntimeException(searchguardIndex + " does not exist");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Create or retrieve trial license from {} created with version {} and mapping type: {}", searchguardIndex,
                    sgIndexMetaData.getCreationVersion(), sgIndexMetaData.mapping().type());
        }

        long created = System.currentTimeMillis();

        GetResponse get = privilegedClient.prepareGet(searchguardIndex, "_doc", "tattr").get();
        if (get.isExists()) {
            created = (long) get.getSource().get("val");
        } else {
            try {
                privilegedClient.index(new IndexRequest(searchguardIndex).id("tattr").setRefreshPolicy(RefreshPolicy.IMMEDIATE).create(true)
                        .source("{\"val\": " + System.currentTimeMillis() + "}", XContentType.JSON)).actionGet();
            } catch (VersionConflictEngineException e) {
                //ignore
            } catch (Exception e) {
                LOGGER.error("Unable to index tattr", e);
            }
        }

        SearchGuardLicenseKey result = SearchGuardLicenseKey.createTrialLicense(formatDate(created), msg);

        result.dynamicValidate(clusterService);

        return result;
    }

    private synchronized void notifyAboutLicenseChanges(SearchGuardLicenseKey license) {
        for (LicenseChangeListener listener : this.licenseChangeListeners) {
            listener.onChange(license);
        }
    }

    private static String formatDate(long date) {
        return new SimpleDateFormat("yyyy-MM-dd", SgUtils.EN_Locale).format(new Date(date));
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    private void useLicense(SearchGuardLicenseKey license, SgDynamicConfiguration<?> config) {        
        this.componentState.setConfigVersion(config != null ? config.getDocVersion() : -1);

        if (license != null) {
            ValidationErrors validationErrors = license.dynamicValidate(clusterService);

            if (!validationErrors.hasErrors()) {
                componentState.setState(State.INITIALIZED, "using_valid_license");

                this.effectiveLicense = license;
            } else {
                componentState.setState(State.INITIALIZED, "license_invalid");
                componentState.setDetailJson(validationErrors.toJsonString());
                this.effectiveLicense = license;
            }
            
            this.configuredLicense = configuredLicense;
            this.validationErrors = validationErrors;
        } else if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true)) {
            this.componentState.setState(State.INITIALIZED, "no_license");
            this.configuredLicense = null;
            this.effectiveLicense = createOrGetTrial(null);
        } else {
            this.componentState.setState(State.INITIALIZED, "no_license_necessary");
            this.configuredLicense = null;
            this.effectiveLicense = null;
        }
        
        notifyAboutLicenseChanges(effectiveLicense);
        printInfoText(effectiveLicense);
    }
    
    private void printInfoText(SearchGuardLicenseKey sgLicense) {
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

    public SearchGuardLicenseKey getConfiguredLicense() {
        return configuredLicense;
    }

    public ValidationErrors getValidationErrors() {
        return validationErrors;
    }

}
