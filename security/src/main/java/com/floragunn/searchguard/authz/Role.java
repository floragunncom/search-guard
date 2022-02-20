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

package com.floragunn.searchguard.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchsupport.util.ImmutableList;

public class Role implements Document<Role>, Hideable {

    public static ValidationResult<Role> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        boolean reserved = vNode.get("reserved").withDefault(false).asBoolean();
        boolean hidden = vNode.get("hidden").withDefault(false).asBoolean();

        // Just for validation:
        vNode.get("cluster_permissions").by(Pattern::parse);
        vNode.get("exclude_cluster_permissions").by(Pattern::parse);
        ImmutableList<String> clusterPermissions = ImmutableList.of(vNode.get("cluster_permissions").asList().withEmptyListAsDefault().ofStrings());
        ImmutableList<String> excludeClusterPermissions = ImmutableList
                .of(vNode.get("exclude_cluster_permissions").asList().withEmptyListAsDefault().ofStrings());
        
        ImmutableList<Index> indexPermissions = ImmutableList.of(vNode.get("index_permissions").asList().ofObjectsParsedBy(Index::new));
        ImmutableList<Tenant> tenantPermissions = ImmutableList.of(vNode.get("tenant_permissions").asList().ofObjectsParsedBy(Tenant::new));
        ImmutableList<ExcludeIndex> excludeIndexPermissions = ImmutableList
                .of(vNode.get("exclude_index_permissions").asList().ofObjectsParsedBy(ExcludeIndex::new));
        String description = vNode.get("description").asString();

        vNode.checkForUnusedAttributes();

        return new ValidationResult<Role>(new Role(docNode, reserved, hidden, description, clusterPermissions, indexPermissions, tenantPermissions,
                excludeClusterPermissions, excludeIndexPermissions), validationErrors);
    }

    private static final Logger log = LogManager.getLogger(RoleMapping.class);

    private final DocNode source;
    private final boolean reserved;
    private final boolean hidden;

    private final String description;
    private final ImmutableList<String> clusterPermissions;
    private final ImmutableList<Index> indexPermissions;
    private final ImmutableList<Tenant> tenantPermissions;
    private final ImmutableList<String> excludeClusterPermissions;
    private final ImmutableList<ExcludeIndex> excludeIndexPermissions;

    public Role(DocNode source, boolean reserved, boolean hidden, String description, ImmutableList<String> clusterPermissions,
            ImmutableList<Index> indexPermissions, ImmutableList<Tenant> tenantPermissions, ImmutableList<String> excludeClusterPermissions,
            ImmutableList<ExcludeIndex> excludeIndexPermissions) {
        super();
        this.source = source;
        this.reserved = reserved;
        this.hidden = hidden;
        this.description = description;
        this.clusterPermissions = clusterPermissions;
        this.indexPermissions = indexPermissions;
        this.tenantPermissions = tenantPermissions;
        this.excludeClusterPermissions = excludeClusterPermissions;
        this.excludeIndexPermissions = excludeIndexPermissions;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public boolean isReserved() {
        return reserved;
    }

    public boolean isHidden() {
        return hidden;
    }

    public static class Index {

        private final ImmutableList<String>  indexPatterns;
        private final String dls;
        private final ImmutableList<String> fls;
        private final ImmutableList<String> maskedFields;
        private final ImmutableList<String> allowedActions;

        Index(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            this.dls = vNode.get("dls").asString();
            this.fls = ImmutableList.of(vNode.get("fls").asListOfStrings());
            this.maskedFields = ImmutableList.of(vNode.get("masked_fields").asListOfStrings());
            
            // Just for validation: TODO Replace any placeholders by empty strings
            vNode.get("allowed_actions").by(Pattern::parse);
            vNode.get("index_patterns").by(Pattern::parse);
            this.allowedActions = ImmutableList.of(vNode.get("allowed_actions").asList().withEmptyListAsDefault().ofStrings());
            this.indexPatterns = ImmutableList.of(vNode.get("index_patterns").asList().withEmptyListAsDefault().ofStrings());

            validationErrors.throwExceptionForPresentErrors();
        }

        public ImmutableList<String>  getIndexPatterns() {
            return indexPatterns;
        }

        public String getDls() {
            return dls;
        }

        public ImmutableList<String> getFls() {
            return fls;
        }

        public ImmutableList<String> getMaskedFields() {
            return maskedFields;
        }

        public ImmutableList<String> getAllowedActions() {
            return allowedActions;
        }

    }

    public static class Tenant {

        private final Pattern tenantPatterns;
        private final Pattern allowedActions;

        public Tenant(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            this.tenantPatterns = vNode.get("tenant_patterns").by(Pattern::parse);
            this.allowedActions = vNode.get("allowed_actions").by(Pattern::parse);

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();
        }

    }

    public static class ExcludeIndex {

        private final Pattern indexPatterns;
        private final Pattern actions;

        public ExcludeIndex(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            this.indexPatterns = vNode.get("index_patterns").by(Pattern::parse);
            this.actions = vNode.get("actions").by(Pattern::parse);

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();
        }
    }

    public ImmutableList<String> getClusterPermissions() {
        return clusterPermissions;
    }

    public ImmutableList<String> getExcludeClusterPermissions() {
        return excludeClusterPermissions;
    }

    public ImmutableList<Index> getIndexPermissions() {
        return indexPermissions;
    }
}
