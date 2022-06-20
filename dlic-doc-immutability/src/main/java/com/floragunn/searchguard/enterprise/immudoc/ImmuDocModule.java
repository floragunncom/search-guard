/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.immudoc;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActionFilter;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.license.SearchGuardLicense.Feature;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

public class ImmuDocModule implements SearchGuardModule, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(ImmuDocModule.class);
    public static final StaticSettings.Attribute<Pattern> COMPLIANCE_IMMUTABLE_INDICES = StaticSettings.Attribute
            .define("searchguard.compliance.immutable_indices").withDefault(Pattern.blank()).asPattern();

    private final ComponentState componentState = new ComponentState(1000, null, "immudoc", ImmuDocModule.class).requiresEnterpriseLicense();

    private final AtomicBoolean hasLicense = new AtomicBoolean(false);
    private final AtomicReference<ImmuDocConfig> config = new AtomicReference<>();

    private ImmuDocActionFilter actionFilter;
    private Pattern staticImmutableIndicesPattern;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {

        this.staticImmutableIndicesPattern = baseDependencies.getStaticSettings().get(COMPLIANCE_IMMUTABLE_INDICES);

        if (!this.staticImmutableIndicesPattern.isBlank()) {
            log.warn(
                    "The option searchguard.compliance.immutable_indices is deprecated and will be removed in the next major version of Search Guard FLX. Please use the configuration sg_immutability instead");
        }

        this.config.set(new ImmuDocConfig(null, staticImmutableIndicesPattern, MetricsLevel.NONE));

        this.actionFilter = new ImmuDocActionFilter(new AdminDNs(baseDependencies.getSettings()), baseDependencies.getAuthInfoService(),
                baseDependencies.getAuditLog(), baseDependencies.getActionRequestIntrospector(), () -> config.get(), () -> hasLicense.get());

        baseDependencies.getConfigurationRepository().subscribeOnChange((ConfigMap configMap) -> {
            SgDynamicConfiguration<ImmuDocConfig> container = configMap.get(ImmuDocConfig.TYPE);

            if (container != null && container.getCEntry("default") != null) {
                config.set(container.getCEntry("default").withImmutableDocPattern(staticImmutableIndicesPattern));

                if (log.isDebugEnabled()) {
                    log.debug("New configuration for ImmuDocModule: " + container);
                }
            } else {
                config.set(new ImmuDocConfig(null, staticImmutableIndicesPattern, MetricsLevel.NONE));
            }
        });

        baseDependencies.getLicenseRepository().subscribeOnLicenseChange((SearchGuardLicense license) -> {
            hasLicense.set(license.hasFeature(Feature.COMPLIANCE));
        });

        componentState.initialized();

        return ImmutableList.empty();
    }

    boolean isEnabled() {
        return hasLicense.get();
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    @Override
    public ImmutableList<ActionFilter> getActionFilters() {
        return ImmutableList.of(actionFilter);
    }

    @Override
    public StaticSettings.AttributeSet getSettings() {
        return StaticSettings.AttributeSet.of(COMPLIANCE_IMMUTABLE_INDICES);
    }

}
