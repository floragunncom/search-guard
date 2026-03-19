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

package com.floragunn.searchguard.compliance;


import java.util.Collections;

import com.floragunn.searchsupport.meta.Meta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.license.LicenseChangeListener;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.license.SearchGuardLicense.Feature;
import com.floragunn.searchguard.support.ConfigConstants;

public class ComplianceConfig implements LicenseChangeListener {

    private final Logger log = LogManager.getLogger(getClass());
    private final Settings settings;
    private final Pattern immutableIndicesPatterns;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private volatile boolean enabled = true;

    public ComplianceConfig(Environment environment, ActionRequestIntrospector actionRequestIntrospector, ConfigurationRepository configRepository) {
        super();
        this.settings = environment.settings();
        this.actionRequestIntrospector = actionRequestIntrospector;

        try {
            immutableIndicesPatterns = Pattern
                    .create(settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_IMMUTABLE_INDICES, Collections.emptyList()));
        } catch (SettingsException | ConfigValidationException e1) {
            throw new RuntimeException("Invalid setting " + ConfigConstants.SEARCHGUARD_COMPLIANCE_IMMUTABLE_INDICES, e1);
        }
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

        log.info("Compliance features are "
                + (this.enabled ? "enabled" : "disabled. To enable them you need a special license. Please contact support for this."));

    }

    public boolean isEnabled() {
        return this.enabled;
    }

    //check for isEnabled
    public boolean isIndexImmutable(Action action, Object request) {

        if (!this.enabled) {
            return false;
        }

        if (immutableIndicesPatterns.isBlank()) {
            return false;
        }

        ResolvedIndices resolved = actionRequestIntrospector.getActionRequestInfo(action, request).getResolvedIndices();

        if (resolved.isLocalAll()) {
            return true;
        } else {
            return immutableIndicesPatterns.matches(resolved.getLocal().getDeepUnion().map(Meta.IndexLikeObject::nameForIndexPatternMatching));
        }
    }

}
