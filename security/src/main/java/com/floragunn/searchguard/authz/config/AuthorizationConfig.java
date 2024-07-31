/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authz.config;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.StaticSettings;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

public class AuthorizationConfig implements PatchableDocument<AuthorizationConfig> {

    static final Pattern DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS = Pattern.createUnchecked("indices:data/read/*",
            "indices:admin/mappings/fields/get", "indices:admin/shards/search_shards", "indices:admin/search/search_shards", "indices:admin/resolve/index", "indices:admin/delete",
            "indices:admin/mapping/put", "indices:admin/settings/update", "indices:monitor/settings/get", "indices:monitor/stats",
            "indices:admin/upgrade", "indices:admin/refresh", "indices:admin/synced_flush", "indices:admin/aliases/get",
            "indices:admin/data_stream/get", "indices:admin/get", AnalyzeAction.NAME, "indices:admin/resolve/cluster");

    static final Pattern DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT = Pattern.createUnchecked("indices:data/read/*",
            "indices:admin/mappings/fields/get", "indices:admin/shards/search_shards", "indices:admin/search/search_shards", "indices:admin/resolve/index", "indices:monitor/settings/get",
            "indices:monitor/stats", "indices:admin/refresh", "indices:admin/synced_flush", "indices:admin/aliases/get",
            "indices:admin/data_stream/get", "indices:admin/get", "indices:admin/resolve/cluster");

    public static final AuthorizationConfig DEFAULT = new AuthorizationConfig(DocNode.EMPTY, true, DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS,
            DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT, null, RoleMapping.ResolutionMode.MAPPING_ONLY, false,
            MetricsLevel.BASIC);

    private final DocNode source;
    private final boolean ignoreUnauthorizedIndices;
    private final Pattern ignoreUnauthorizedIndicesActions;
    private final Pattern ignoreUnauthorizedIndicesActionsAllowingEmptyResult;

    private final String fieldAnonymizationSalt;
    private final boolean debugEnabled;
    private final MetricsLevel metricsLevel;
    private final RoleMapping.ResolutionMode roleMappingResolution;

    AuthorizationConfig(DocNode source, boolean ignoreUnauthorizedIndices, Pattern ignoreUnauthorizedIndicesActions,
            Pattern ignoreUnauthorizedIndicesActionsAllowingEmptyResult, String fieldAnonymizationSalt,
            RoleMapping.ResolutionMode roleMappingResolution, boolean debugEnabled, MetricsLevel metricsLevel) {
        this.source = source;

        this.ignoreUnauthorizedIndices = ignoreUnauthorizedIndices;
        this.ignoreUnauthorizedIndicesActions = ignoreUnauthorizedIndicesActions;
        this.ignoreUnauthorizedIndicesActionsAllowingEmptyResult = ignoreUnauthorizedIndicesActionsAllowingEmptyResult;
        this.fieldAnonymizationSalt = fieldAnonymizationSalt;
        this.roleMappingResolution = roleMappingResolution;
        this.debugEnabled = debugEnabled;
        this.metricsLevel = metricsLevel;
    }

    public static ValidationResult<AuthorizationConfig> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode;
        try {
            vNode = new ValidatingDocNode(docNode.splitDottedAttributeNamesToTree(), validationErrors);
        } catch (UnexpectedDocumentStructureException e) {
            return new ValidationResult<AuthorizationConfig>(e.getValidationErrors());
        }

        boolean ignoreUnauthorizedIndices = vNode.get("ignore_unauthorized_indices.enabled").withDefault(true).asBoolean();
        Pattern ignoreUnauthorizedIndicesActions = vNode.get("ignore_unauthorized_indices.affected_actions")
                .withDefault(DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS).by(Pattern::parse);
        Pattern ignoreUnauthorizedIndicesActionsAllowingEmptyResult = vNode.get("ignore_unauthorized_indices.empty_result_allowed_for_actions")
                .withDefault(DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT).by(Pattern::parse);
        String fieldAnonymizationSalt = vNode.get("field_anonymization.salt").asString();
        RoleMapping.ResolutionMode roleMappingResolution = vNode.get("role_mapping.resolution_mode")
                .withDefault(RoleMapping.ResolutionMode.MAPPING_ONLY).asEnum(RoleMapping.ResolutionMode.class);
        boolean debugEnabled = vNode.get("debug").withDefault(false).asBoolean();
        MetricsLevel metricsLevel = vNode.get("metrics").withDefault(MetricsLevel.BASIC).asEnum(MetricsLevel.class);

        if (!validationErrors.hasErrors()) {
            return new ValidationResult<AuthorizationConfig>(new AuthorizationConfig(docNode, ignoreUnauthorizedIndices,
                    ignoreUnauthorizedIndicesActions, ignoreUnauthorizedIndicesActionsAllowingEmptyResult, fieldAnonymizationSalt,
                    roleMappingResolution, debugEnabled, metricsLevel));
        } else {
            return new ValidationResult<AuthorizationConfig>(validationErrors);
        }
    }

    public static AuthorizationConfig parseLegacySgConfig(DocNode docNode, Parser.Context context, StaticSettings settings)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode.splitDottedAttributeNamesToTree(), validationErrors);

        String fieldAnonymizationSalt = vNode.get("dynamic.field_anonymization_salt2").asString();

        // Note: We do not convert dynamic.do_not_fail_on_forbidden to ignoreUnauthorizedIndices any more. 
        // It is now VERY strongly recommended to turn ignoreUnauthorizedIndices on in any case. Turning it off may have cause many 
        // operations fail due to insufficient permissions, and may even break existing systems. As using sg_config with FLX
        // shall be only a transitive state, it is okay to force this setting to users during this time. Users still may switch it off
        // when having migrated to sg_authz.yml
        validationErrors.throwExceptionForPresentErrors();

        return new AuthorizationConfig(docNode, true, DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS,
                DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT, fieldAnonymizationSalt, getRolesMappingResolution(settings), false,
                MetricsLevel.BASIC);
    }

    public boolean isIgnoreUnauthorizedIndices() {
        return ignoreUnauthorizedIndices;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public String toString() {
        return toJsonString();
    }

    public String getFieldAnonymizationSalt() {
        return fieldAnonymizationSalt;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    @Override
    public AuthorizationConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }

    public Pattern getIgnoreUnauthorizedIndicesActions() {
        return ignoreUnauthorizedIndicesActions;
    }

    public Pattern getIgnoreUnauthorizedIndicesActionsAllowingEmptyResult() {
        return ignoreUnauthorizedIndicesActionsAllowingEmptyResult;
    }

    /**
     * @deprecated only used for legacy config parsing 
     */
    private static RoleMapping.ResolutionMode getRolesMappingResolution(StaticSettings settings) {
        try {
            return RoleMapping.ResolutionMode.valueOf(settings.get(AuthorizationService.ROLES_MAPPING_RESOLUTION).toUpperCase());
        } catch (Exception e) {
            return RoleMapping.ResolutionMode.MAPPING_ONLY;
        }
    }

    public RoleMapping.ResolutionMode getRoleMappingResolution() {
        return roleMappingResolution;
    }
}
