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
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.google.common.collect.ImmutableMap;

public class FeMultiTenancyConfig implements PatchableDocument<FeMultiTenancyConfig> {

    public static CType<FeMultiTenancyConfig> TYPE = new CType<FeMultiTenancyConfig>("frontend_multi_tenancy", "Frontend Multi-Tenancy", 10001,
            FeMultiTenancyConfig.class, FeMultiTenancyConfig::parse, CType.Storage.OPTIONAL, CType.Arity.SINGLE);

    public static final FeMultiTenancyConfig DEFAULT = new FeMultiTenancyConfig(null, true, "kibanaserver", ".kibana");

    private final DocNode source;
    private final boolean enabled;
    private final String index;
    private final String serverUsername;
    // TODO 
    private final MetricsLevel metricsLevel = MetricsLevel.DETAILED;

    FeMultiTenancyConfig(DocNode source, boolean enabled, String serverUsername, String index) {
        this.source = source;
        this.enabled = enabled;
        this.serverUsername = serverUsername;
        this.index = index;
    }

    public static ValidationResult<FeMultiTenancyConfig> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        boolean enabled = vNode.get("enabled").withDefault(true).asBoolean();
        String index = vNode.get("index").withDefault(DEFAULT.getIndex()).asString();
        String serverUsername = vNode.get("server_user").withDefault(DEFAULT.getServerUsername()).asString();

        return new ValidationResult<>(new FeMultiTenancyConfig(docNode, enabled, serverUsername, index), validationErrors);
    }

    public static FeMultiTenancyConfig parseLegacySgConfig(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        boolean enabled = vNode.get("dynamic.kibana.multitenancy_enabled").withDefault(true).asBoolean();
        String index = vNode.get("dynamic.kibana.index").withDefault(DEFAULT.getIndex()).asString();
        String serverUsername = vNode.get("dynamic.kibana.server_username").withDefault(DEFAULT.getServerUsername()).asString();

        validationErrors.throwExceptionForPresentErrors();

        return new FeMultiTenancyConfig(null, enabled, serverUsername, index);
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

    @Override
    public Object toBasicObject() {
        if (source != null) {
            return source;
        } else {
            return ImmutableMap.of("enabled", enabled, "index", index, "server_user", serverUsername);
        }
    }

    @Override
    public String toString() {
        return "FeMultiTenancyConfig [source=" + source + ", enabled=" + enabled + ", index=" + index + ", serverUsername=" + serverUsername + "]";
    }

    @Override
    public FeMultiTenancyConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }
}
