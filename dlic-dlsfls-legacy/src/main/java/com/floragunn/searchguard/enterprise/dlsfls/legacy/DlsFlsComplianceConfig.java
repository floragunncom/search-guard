/*
 * Copyright 2018-2022 floragunn GmbH
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

package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authc.legacy.LegacySgConfig;
import com.floragunn.searchguard.authz.config.AuthorizationConfig;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.license.LicenseChangeListener;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.license.SearchGuardLicense.Feature;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchsupport.StaticSettings;

public class DlsFlsComplianceConfig implements LicenseChangeListener {

    private final Logger log = LogManager.getLogger(getClass());
    private final Settings settings;

    private final byte[] salt16;
    private volatile boolean enabled = true;
    private final boolean localHashingEnabled;
    private byte[] salt2_16;
    private final byte[] maskPrefix;
    private final Client client;

    public DlsFlsComplianceConfig(Settings settings, ConfigurationRepository configRepository, Client client) {
        super();
        this.settings = settings;
        this.client = client;

        this.localHashingEnabled = this.settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_LOCAL_HASHING_ENABLED, false);

        final String saltAsString = settings.get(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT, ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT_DEFAULT);
        final byte[] saltAsBytes = saltAsString.getBytes(StandardCharsets.UTF_8);

        if (saltAsBytes.length < 16) {
            throw new ElasticsearchException(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT + " must at least contain 16 bytes");
        }

        if (saltAsBytes.length > 16) {
            log.warn(ConfigConstants.SEARCHGUARD_COMPLIANCE_SALT + " is greater than 16 bytes. Only the first 16 bytes are used for salting");
        }

        salt16 = Arrays.copyOf(saltAsBytes, 16);

        final String maskPrefixString = settings.get(ConfigConstants.SEARCHGUARD_COMPLIANCE_MASK_PREFIX, null);

        if (maskPrefixString == null || maskPrefixString.isEmpty()) {
            maskPrefix = null;
        } else {
            maskPrefix = maskPrefixString.getBytes(StandardCharsets.UTF_8);
        }

        configRepository.subscribeOnChange((configMap) -> {
            SgDynamicConfiguration<AuthorizationConfig> config = configMap.get(CType.AUTHZ);
            SgDynamicConfiguration<LegacySgConfig> legacyConfig = configMap.get(CType.CONFIG);

            if (config != null && config.getCEntry("default") != null) {
                AuthorizationConfig authzConfig = config.getCEntry("default");
                setFieldAnonymizationSalt2(authzConfig.getFieldAnonymizationSalt());
                log.info("Updated authz config:\n" + config);
                if (log.isDebugEnabled()) {
                    log.debug(authzConfig);
                }
            } else if (legacyConfig != null && legacyConfig.getCEntry("sg_config") != null) {
                try {
                    LegacySgConfig sgConfig = legacyConfig.getCEntry("sg_config");
                    AuthorizationConfig privilegesConfig = AuthorizationConfig.parseLegacySgConfig(sgConfig.getSource(), null,
                            new StaticSettings(settings, null));
                    setFieldAnonymizationSalt2(privilegesConfig.getFieldAnonymizationSalt());
                    log.info("Updated authz config (legacy):\n" + legacyConfig);
                    if (log.isDebugEnabled()) {
                        log.debug(privilegesConfig);
                    }
                } catch (ConfigValidationException e) {
                    log.error("Error while parsing sg_config:\n" + e);
                }
            }
        });
    }

    @Override
    public void onChange(SearchGuardLicense license) {

        if (license == null) {
            this.enabled = false;
        } else {
            if (license.hasFeature(Feature.COMPLIANCE)) {
                this.enabled = true;
            } else {
                this.enabled = false;
            }
        }
    }

    public boolean isEnabled() {
        return this.enabled;
    }

    public byte[] getSalt16() {
        return salt16.clone();
    }

    public boolean isLocalHashingEnabled() {
        return localHashingEnabled;
    }

    private void setFieldAnonymizationSalt2(String fieldAnonymizationSalt2) {

        if (log.isTraceEnabled()) {
            log.trace("ComplianceConfiguration#onChanged called");
            log.trace("isLocalHashingEnabled? " + isLocalHashingEnabled());
            log.trace("FieldAnonymizationSalt2: " + fieldAnonymizationSalt2);
        }

        if (isLocalHashingEnabled() && fieldAnonymizationSalt2 != null) {
            final String salt2AsString = fieldAnonymizationSalt2;

            if (salt2AsString != null && !salt2AsString.isEmpty()) {
                final byte[] salt2AsBytes = salt2AsString.getBytes(StandardCharsets.UTF_8);

                if (salt2AsBytes.length < 16) {
                    log.error("searchguard.dynamic.field_anonymization.salt2 must at least contain 16 bytes");
                }

                if (salt2AsBytes.length > 16) {
                    log.warn("searchguard.dynamic.field_anonymization.salt2 is greater than 16 bytes. Only the first 16 bytes are used");
                }
                final byte[] _salt2_16 = Arrays.copyOf(salt2AsBytes, 16);

                if (!Arrays.equals(salt2_16, _salt2_16)) {
                    log.debug("value of searchguard.dynamic.field_anonymization.salt2 changed");
                    salt2_16 = _salt2_16;
                    ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest();
                    clearIndicesCacheRequest.fieldDataCache(false);
                    //clearIndicesCacheRequest.fields(fields)
                    //clearIndicesCacheRequest.indices("");
                    clearIndicesCacheRequest.queryCache(false);
                    clearIndicesCacheRequest.requestCache(true);

                    client.admin().indices().clearCache(clearIndicesCacheRequest, new ActionListener<BroadcastResponse>() {

                        @Override
                        public void onResponse(BroadcastResponse response) {
                            log.debug("Cache cleared due to salt2 changed: " + Strings.toString(response));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            log.debug("Cache cleared due to salt2 changed: " + e, e);
                        }
                    });
                }

            } else {
                log.error(ConfigConstants.SEARCHGUARD_COMPLIANCE_LOCAL_HASHING_ENABLED
                        + " is enabled but searchguard.dynamic.field_anonymization.salt2 is not set");
            }
        }
    }

    public byte[] getSalt2_16() {
        return salt2_16 == null ? null : salt2_16.clone();
    }

    public byte[] getMaskPrefix() {
        return maskPrefix;
    }

}
