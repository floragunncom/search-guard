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

package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class FeMultiTenancyConfig implements PatchableDocument<FeMultiTenancyConfig> {


    public static CType<FeMultiTenancyConfig> TYPE = new CType<FeMultiTenancyConfig>("frontend_multi_tenancy", "Frontend Multi-Tenancy", 10001,
            FeMultiTenancyConfig.class, FeMultiTenancyConfig::parse, CType.Storage.OPTIONAL, CType.Arity.SINGLE);

    public static final FeMultiTenancyConfig DEFAULT = new FeMultiTenancyConfig(null, false,
            "kibanaserver", ".kibana", true, true, ImmutableList.empty());

    private final DocNode source;
    private final boolean enabled;
    private final String index;
    private final String serverUsername;
    private final boolean globalTenantEnabled;
    private final boolean privateTenantEnabled;
    private final List<String> preferredTenants;
    // TODO
    private final MetricsLevel metricsLevel = MetricsLevel.DETAILED;

    FeMultiTenancyConfig(DocNode source, boolean enabled, String serverUsername, String index,
                         boolean globalTenantEnabled, boolean privateTenantEnabled, List<String> preferredTenants) {
        this.source = source;
        this.enabled = enabled;
        this.serverUsername = serverUsername;
        this.index = index;
        this.globalTenantEnabled = globalTenantEnabled;
        this.privateTenantEnabled = privateTenantEnabled;
        this.preferredTenants = preferredTenants;
    }

    public static ValidationResult<FeMultiTenancyConfig> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        boolean enabled = vNode.get("enabled").withDefault(DEFAULT.isEnabled()).asBoolean();
        String index = vNode.get("index").withDefault(DEFAULT.getIndex()).asString();
        String serverUsername = vNode.get("server_user").withDefault(DEFAULT.getServerUsername()).asString();
        boolean globalTenantEnabled = vNode.get("global_tenant_enabled").withDefault(DEFAULT.isGlobalTenantEnabled()).asBoolean();
        boolean privateTenantEnabled = vNode.get("private_tenant_enabled").withDefault(DEFAULT.isPrivateTenantEnabled()).asBoolean();
        List<String> preferredTenants = vNode.get("preferred_tenants").asList().withEmptyListAsDefault().ofStrings();

        return new ValidationResult<>(new FeMultiTenancyConfig(docNode, enabled, serverUsername, index, globalTenantEnabled, privateTenantEnabled, preferredTenants), validationErrors);
    }

    public static FeMultiTenancyConfig parseLegacySgConfig(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        boolean enabled = vNode.get("dynamic.kibana.multitenancy_enabled").withDefault(DEFAULT.isEnabled()).asBoolean();
        String index = vNode.get("dynamic.kibana.index").withDefault(DEFAULT.getIndex()).asString();
        String serverUsername = vNode.get("dynamic.kibana.server_username").withDefault(DEFAULT.getServerUsername()).asString();
        boolean globalTenantEnabled = vNode.get("dynamic.kibana.global_tenant_enabled").withDefault(DEFAULT.isGlobalTenantEnabled()).asBoolean();
        boolean privateTenantEnabled = vNode.get("dynamic.kibana.private_tenant_enabled").withDefault(DEFAULT.isPrivateTenantEnabled()).asBoolean();
        List<String> preferredTenants = vNode.get("dynamic.kibana.preferred_tenants").asList().withEmptyListAsDefault().ofStrings();

        validationErrors.throwExceptionForPresentErrors();

        return new FeMultiTenancyConfig(null, enabled, serverUsername, index, globalTenantEnabled, privateTenantEnabled, preferredTenants);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getIndex() {
        return index;
    }

    public String getServerUsername() {
        return serverUsername;
    }

    public boolean isGlobalTenantEnabled() {
        return globalTenantEnabled;
    }

    public boolean isPrivateTenantEnabled() {
        return privateTenantEnabled;
    }

    public List<String> getPreferredTenants() {
        return preferredTenants;
    }

    @Override
    public Map<String, Object> toBasicObject() {
        if (source != null) {
            return source;
        } else {
            return ImmutableMap.of(
                    "enabled", enabled, "index", index, "server_user", serverUsername,
                    "global_tenant_enabled", globalTenantEnabled, "private_tenant_enabled", privateTenantEnabled,
                    "preferred_tenants", preferredTenants
            );
        }
    }

    @Override
    public String toString() {
        return "FeMultiTenancyConfig [source=" + source + ", enabled=" + enabled +
                ", index=" + index + ", serverUsername=" + serverUsername +
                ", globalTenantEnabled=" + globalTenantEnabled + ", privateTenantEnabled=" + privateTenantEnabled +
                ", preferredTenants=(" + String.join(", ", preferredTenants) + ")]";
    }

    @Override
    public FeMultiTenancyConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }

    public FeMultiTenancyConfig withEnabled(boolean enabled) {
        return new FeMultiTenancyConfig(source, enabled, serverUsername, index, globalTenantEnabled, privateTenantEnabled, preferredTenants);
    }
}
