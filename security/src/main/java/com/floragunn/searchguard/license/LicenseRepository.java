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
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;

public class LicenseRepository implements ComponentStateProvider {
    private static final Logger LOGGER = LogManager.getLogger(LicenseRepository.class);

    private final ClusterService clusterService;
    private final List<LicenseChangeListener> licenseChangeListeners = new ArrayList<LicenseChangeListener>();
    private final PrivilegedConfigClient privilegedClient;
    private final ConfigurationRepository configurationRepository;
    private final ComponentState componentState = new ComponentState(2, null, "license_repository", LicenseRepository.class);

    private volatile SearchGuardLicense effectiveLicense;
    private volatile SearchGuardLicense configuredLicense;
    private volatile ValidationErrors validationErrors;

    public LicenseRepository(Settings settings, Client client, ClusterService clusterService, ConfigurationRepository configurationRepository) {
        this.configurationRepository = configurationRepository;
        this.clusterService = clusterService;
        this.privilegedClient = PrivilegedConfigClient.adapt(client);

        if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true)) {
            configurationRepository.subscribeOnChange((configMap) -> {
                SgDynamicConfiguration<SearchGuardLicenseKey> config = configMap.get(CType.LICENSE_KEY);

                if (config != null && config.getCEntry("default") != null) {
                    useLicense(config.getCEntry("default"), config);
                } else {
                    useLicense(null, null);
                }
            });
        } else {
            this.componentState.setState(State.SUSPENDED, "enterprise_modules_disabled");
        }

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
    public SearchGuardLicense getLicense() {
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

    private SearchGuardLicense createOrGetTrial(String msg) {
        
        String searchguardIndex;
        try {
            searchguardIndex = this.configurationRepository.getEffectiveSearchGuardIndexAndCreateIfNecessary();
        } catch (ConfigUpdateException e1) {
            throw new RuntimeException(e1);
        }

        final Index sgIndex = clusterService.state().getMetadata().getIndicesLookup().get(searchguardIndex).getWriteIndex();
        if (sgIndex == null) {
            LOGGER.error("Unable to retrieve trial license (or create  a new one) because {} index does not exist", searchguardIndex);
            throw new RuntimeException(searchguardIndex + " does not exist");
        }

        final IndexMetadata sgIndexMetaData = clusterService.state().getMetadata().index(sgIndex);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Create or retrieve trial license from {} created with version {} and mapping type: {}", searchguardIndex,
                    sgIndexMetaData.getCreationVersion(), sgIndexMetaData.mapping().type());
        }

        long created = System.currentTimeMillis();

        GetResponse get = privilegedClient.prepareGet().setIndex(searchguardIndex).setId("tattr").get();

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

        SearchGuardLicense result = SearchGuardLicense.createTrialLicense(formatDate(created), msg);

        result.dynamicValidate(clusterService);

        return result;
    }

    private synchronized void notifyAboutLicenseChanges(SearchGuardLicense license) {
        for (LicenseChangeListener listener : this.licenseChangeListeners) {
            listener.onChange(license);
        }
    }

    private static String formatDate(long date) {
        return new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).format(new Date(date));
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    private void useLicense(SearchGuardLicenseKey licenseKey, SgDynamicConfiguration<?> config) {
        this.componentState.setConfigVersion(config != null ? config.getDocVersion() : -1);

        if (licenseKey != null) {
            SearchGuardLicense license = licenseKey.getLicense();
            ValidationErrors validationErrors = license.dynamicValidate(clusterService);

            if (!validationErrors.hasErrors()) {
                componentState.setState(State.INITIALIZED, "using_valid_license");
                componentState.setConfigProperty("license_uid", license.getUid());

                this.effectiveLicense = license;
            } else {
                componentState.setState(State.INITIALIZED, "license_invalid");
                componentState.addDetail(validationErrors);
                componentState.setConfigProperty("license_uid", license.getUid());

                this.effectiveLicense = license;
            }

            this.configuredLicense = configuredLicense;
            this.validationErrors = validationErrors;
        } else {
            this.componentState.setState(State.INITIALIZED, "no_license");
            this.configuredLicense = null;
            this.effectiveLicense = createOrGetTrial(null);
        }

        notifyAboutLicenseChanges(effectiveLicense);
        printInfoText(effectiveLicense);
    }

    private void printInfoText(SearchGuardLicense sgLicense) {
        if (sgLicense != null) {
            LOGGER.info("Search Guard License Type: " + sgLicense.getType() + ", " + (sgLicense.isValid() ? "valid" : "invalid"));

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

    public SearchGuardLicense getConfiguredLicense() {
        return configuredLicense;
    }

    public ValidationErrors getValidationErrors() {
        return validationErrors;
    }

}
