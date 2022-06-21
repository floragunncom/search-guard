/*
 * Copyright 2020-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;

public class RequestedPrivileges implements Writeable, ToXContentObject, Serializable {
    static final String RESTRICTION_ROLE = "_requested_privileges";
    static final ImmutableSet<String> RESTRICTION_ROLES = ImmutableSet.of(RESTRICTION_ROLE);
    private static final long serialVersionUID = 5862219250642101795L;
    private static final ImmutableList<String> WILDCARD_LIST = ImmutableList.of("*");
    private static final ImmutableList<Template<Pattern>> WILDCARD_TEMPLATE_PATTERN_LIST = ImmutableList.of(Template.constant(Pattern.wildcard(), "*"));
    
    public static final RequestedPrivileges ALL = new RequestedPrivileges(WILDCARD_LIST, IndexPermissions.ALL, TenantPermissions.ALL);
    private static final Logger log = LogManager.getLogger(AuthTokenService.class);

    private ImmutableList<String> clusterPermissions;
    private ImmutableList<IndexPermissions> indexPermissions;
    private ImmutableList<TenantPermissions> tenantPermissions;
    private ImmutableList<String> roles;
    private ImmutableList<String> excludedClusterPermissions;
    private ImmutableList<ExcludedIndexPermissions> excludedIndexPermissions;

    public RequestedPrivileges(StreamInput in) throws IOException {
        this.clusterPermissions = ImmutableList.of(in.readStringList());
        this.indexPermissions = ImmutableList.of(in.readList(IndexPermissions::new));
        this.tenantPermissions = ImmutableList.of(in.readList(TenantPermissions::new));
        this.excludedClusterPermissions = ImmutableList.of(in.readStringList());
        this.excludedIndexPermissions = ImmutableList.of(in.readList(ExcludedIndexPermissions::new));
        List<String> roles = in.readOptionalStringList();
        this.roles = roles != null ? ImmutableList.of(roles) : null;
    }

    RequestedPrivileges(List<String> clusterPermissions, List<IndexPermissions> indexPermissions, List<TenantPermissions> tenantPermissions) {
        this.clusterPermissions = ImmutableList.of(clusterPermissions);
        this.indexPermissions = ImmutableList.of(indexPermissions);
        this.tenantPermissions = ImmutableList.of(tenantPermissions);
        this.excludedClusterPermissions = ImmutableList.empty();
        this.excludedIndexPermissions = ImmutableList.empty();
    }

    private RequestedPrivileges() {
    }

    public ImmutableList<String> getClusterPermissions() {
        return clusterPermissions;
    }

    public ImmutableList<IndexPermissions> getIndexPermissions() {
        return indexPermissions;
    }

    public ImmutableList<TenantPermissions> getTenantPermissions() {
        return tenantPermissions;
    }

    public ImmutableList<String> getRoles() {
        return roles;
    }

    public ImmutableList<String> getExcludedClusterPermissions() {
        return excludedClusterPermissions;
    }

    public ImmutableList<ExcludedIndexPermissions> getExcludedIndexPermissions() {
        return excludedIndexPermissions;
    }

    public RequestedPrivileges excludeClusterPermissions(List<String> excludeAddionalClusterPermissions) {
        if (excludeAddionalClusterPermissions == null || excludeAddionalClusterPermissions.size() == 0) {
            return this;
        }

        RequestedPrivileges result = new RequestedPrivileges();
        result.clusterPermissions = this.clusterPermissions;
        result.indexPermissions = this.indexPermissions;
        result.tenantPermissions = this.tenantPermissions;
        result.roles = this.roles;
        result.excludedIndexPermissions = this.excludedIndexPermissions;
        result.excludedClusterPermissions = this.excludedClusterPermissions.with(excludeAddionalClusterPermissions);

        return result;
    }

    public RequestedPrivileges excludeIndexPermissions(List<ExcludedIndexPermissions> excludeAddionalIndexPermissions) {
        if (excludeAddionalIndexPermissions == null || excludeAddionalIndexPermissions.size() == 0) {
            return this;
        }

        RequestedPrivileges result = new RequestedPrivileges();
        result.clusterPermissions = this.clusterPermissions;
        result.indexPermissions = this.indexPermissions;
        result.tenantPermissions = this.tenantPermissions;
        result.roles = this.roles;
        result.excludedClusterPermissions = this.excludedClusterPermissions;
        result.excludedIndexPermissions = this.excludedIndexPermissions.with(excludeAddionalIndexPermissions);

        return result;
    }

    public boolean isTotalWildcard() {
        if (!clusterPermissions.contains("*")) {
            return false;
        }

        if (excludedClusterPermissions != null && excludedClusterPermissions.size() > 0) {
            return false;
        }

        if (excludedIndexPermissions != null && excludedIndexPermissions.size() > 0) {
            return false;
        }

        if (roles != null && roles.size() > 0) {
            return false;
        }

        if (indexPermissions.size() != 1) {
            return false;
        }

        if (!indexPermissions.get(0).isWildcard()) {
            return false;
        }

        if (tenantPermissions.size() != 1) {
            return false;
        }

        if (!tenantPermissions.get(0).isWildcard()) {
            return false;
        }

        return true;
    }

    SgDynamicConfiguration<Role> toRolesConfig() {
        ImmutableList<Role.Index> indexPermissions = this.indexPermissions.map((p) -> p.toRoleIndex());
        ImmutableList<Role.Tenant> tenantPermissions = this.tenantPermissions.map((p) -> p.toRoleTenant());
        ImmutableList<Role.ExcludeIndex> excludeIndexPermissions = this.excludedIndexPermissions.map((p) -> p.toRoleExcludeIndex());

        Role role = new Role(null, false, false, false, "requested privileges", clusterPermissions, indexPermissions, tenantPermissions,
                excludedClusterPermissions, excludeIndexPermissions);

        
        return SgDynamicConfiguration.of(CType.ROLES, RESTRICTION_ROLE, role);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeStringCollection(clusterPermissions);
        out.writeList(indexPermissions);
        out.writeList(tenantPermissions);
        out.writeStringCollection(excludedClusterPermissions);
        out.writeList(excludedIndexPermissions);
        out.writeOptionalStringCollection(roles);
    }

    public static class IndexPermissions implements Writeable, ToXContentObject, Serializable {

        public static final ImmutableList<IndexPermissions> ALL = ImmutableList.of(new IndexPermissions(WILDCARD_TEMPLATE_PATTERN_LIST, WILDCARD_LIST));

        private static final long serialVersionUID = -2567351561923741922L;
        private ImmutableList<Template<Pattern>> indexPatterns;
        private ImmutableList<String> allowedActions;

        IndexPermissions(ImmutableList<Template<Pattern>> indexPatterns, ImmutableList<String> allowedActions) {
            this.indexPatterns = indexPatterns;
            this.allowedActions = allowedActions;
        }

        IndexPermissions(StreamInput in) throws IOException {
            this.indexPatterns =  ImmutableList.map(in.readStringList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling " + s, e);
                    return null;
                }
            });
            this.allowedActions = ImmutableList.of(in.readStringList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indexPatterns.map((t) -> t.getSource()));
            out.writeStringCollection(allowedActions);
        }

        public static IndexPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<Template<Pattern>> indexPatterns = vJsonNode.get("index_patterns").required().asList().minElements(1).ofTemplates(Pattern::create);
            List<String> allowedActions = vJsonNode.get("allowed_actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new IndexPermissions(ImmutableList.of(indexPatterns), ImmutableList.of(allowedActions));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index_patterns", indexPatterns.map((t) -> t.getSource()));
            builder.field("allowed_actions", allowedActions);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "IndexPermissions [indexPatterns=" + indexPatterns + ", allowedActions=" + allowedActions + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((allowedActions == null) ? 0 : allowedActions.hashCode());
            result = prime * result + ((indexPatterns == null) ? 0 : indexPatterns.hashCode());
            return result;
        }
        
        public Role.Index toRoleIndex() {            
            return new Role.Index(indexPatterns, null, null, null, allowedActions);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            IndexPermissions other = (IndexPermissions) obj;
            if (allowedActions == null) {
                if (other.allowedActions != null)
                    return false;
            } else if (!allowedActions.equals(other.allowedActions))
                return false;
            if (indexPatterns == null) {
                if (other.indexPatterns != null)
                    return false;
            } else if (!indexPatterns.equals(other.indexPatterns))
                return false;
            return true;
        }

        public boolean isWildcard() {
            return allowedActions.contains("*") && indexPatterns.forAnyApplies((t) -> t.isConstant() && t.getConstantValue().isWildcard());
        }
    }

    public static class TenantPermissions implements Writeable, ToXContentObject, Serializable {
        public static final ImmutableList<TenantPermissions> ALL = ImmutableList.of(new TenantPermissions(WILDCARD_TEMPLATE_PATTERN_LIST, WILDCARD_LIST));

        private static final long serialVersionUID = 170036537583928629L;
        private ImmutableList<Template<Pattern>> tenantPatterns;
        
        private ImmutableList<String> allowedActions;

        TenantPermissions(ImmutableList<Template<Pattern>> tenantPatterns, ImmutableList<String> allowedActions) {
            this.tenantPatterns = tenantPatterns;
            this.allowedActions = allowedActions;
        }

        TenantPermissions(StreamInput in) throws IOException {
            this.tenantPatterns = ImmutableList.map(in.readStringList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling " + s, e);
                    return null;
                }
            });
            this.allowedActions = ImmutableList.of(in.readStringList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(tenantPatterns.map((t) -> t.getSource()));
            out.writeStringCollection(allowedActions);
        }

        public static TenantPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<Template<Pattern>> tenantPatterns = vJsonNode.get("tenant_patterns").required().asList().minElements(1).ofTemplates(Pattern::create);
            List<String> allowedActions = vJsonNode.get("allowed_actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new TenantPermissions(ImmutableList.of(tenantPatterns), ImmutableList.of(allowedActions));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("tenant_patterns", tenantPatterns.map((t) -> t.getSource()));
            builder.field("allowed_actions", allowedActions);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "TenantPermissions [tenantPatterns=" + tenantPatterns + ", allowedActions=" + allowedActions + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((allowedActions == null) ? 0 : allowedActions.hashCode());
            result = prime * result + ((tenantPatterns == null) ? 0 : tenantPatterns.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TenantPermissions other = (TenantPermissions) obj;
            if (allowedActions == null) {
                if (other.allowedActions != null)
                    return false;
            } else if (!allowedActions.equals(other.allowedActions))
                return false;
            if (tenantPatterns == null) {
                if (other.tenantPatterns != null)
                    return false;
            } else if (!tenantPatterns.equals(other.tenantPatterns))
                return false;
            return true;
        }

        public boolean isWildcard() {
            return allowedActions.contains("*") && tenantPatterns.forAnyApplies((t) -> t.isConstant() && t.getConstantValue().isWildcard());
        }

        Role.Tenant toRoleTenant() {
            return new Role.Tenant(tenantPatterns, allowedActions);
        }
    }

    public static class ExcludedIndexPermissions implements Writeable, ToXContentObject, Serializable {

        private static final long serialVersionUID = -2567351561923741922L;
        private ImmutableList<Template<Pattern>> indexPatterns;
        private ImmutableList<String> actions;

        ExcludedIndexPermissions(ImmutableList<Template<Pattern>> indexPatterns, ImmutableList<String> actions) {
            this.indexPatterns = indexPatterns;
            this.actions = actions;
        }

        ExcludedIndexPermissions(StreamInput in) throws IOException {
            this.indexPatterns =  ImmutableList.map(in.readStringList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling " + s, e);
                    return null;
                }
            });
            this.actions = ImmutableList.of(in.readStringList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indexPatterns.map((t) -> t.getSource()));
            out.writeStringCollection(actions);
        }

        public static ExcludedIndexPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<Template<Pattern>> indexPatterns = vJsonNode.get("index_patterns").required().asList().minElements(1).ofTemplates(Pattern::create);
            List<String> actions = vJsonNode.get("actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new ExcludedIndexPermissions(ImmutableList.of(indexPatterns), ImmutableList.of(actions));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index_patterns", indexPatterns);
            builder.field("actions", actions);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "ExcludedIndexPermissions [indexPatterns=" + indexPatterns + ", actions=" + actions + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((actions == null) ? 0 : actions.hashCode());
            result = prime * result + ((indexPatterns == null) ? 0 : indexPatterns.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            IndexPermissions other = (IndexPermissions) obj;
            if (actions == null) {
                if (other.allowedActions != null)
                    return false;
            } else if (!actions.equals(other.allowedActions))
                return false;
            if (indexPatterns == null) {
                if (other.indexPatterns != null)
                    return false;
            } else if (!indexPatterns.equals(other.indexPatterns))
                return false;
            return true;
        }
        
        Role.ExcludeIndex toRoleExcludeIndex() {
            return new Role.ExcludeIndex(indexPatterns, actions);
        }
    }

    public static RequestedPrivileges parse(DocNode jsonNode) throws ConfigValidationException {
        if (jsonNode.toBasicObject() instanceof String) {
            if (jsonNode.toString().equals("*")) {
                return totalWildcard();
            }
        }

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);
        RequestedPrivileges result = new RequestedPrivileges();

        result.clusterPermissions = vJsonNode.hasNonNull("cluster_permissions") ? ImmutableList.of(vJsonNode.get("cluster_permissions").asListOfStrings()) : null;
        result.indexPermissions =   vJsonNode.hasNonNull("index_permissions") ? ImmutableList.of(vJsonNode.get("index_permissions").asList(IndexPermissions::parse)) : null;
        result.tenantPermissions = vJsonNode.hasNonNull("tenant_permissions") ? ImmutableList.of(vJsonNode.get("tenant_permissions").asList(TenantPermissions::parse)) : null;
        result.excludedClusterPermissions = vJsonNode.hasNonNull("exclude_cluster_permissions") ? ImmutableList.of(vJsonNode.get("exclude_cluster_permissions").asListOfStrings()) : null;
        result.excludedIndexPermissions =  vJsonNode.hasNonNull("exclude_index_permissions") ? ImmutableList.of(vJsonNode.get("exclude_index_permissions").asList(ExcludedIndexPermissions::parse)) : null;
        result.roles = vJsonNode.hasNonNull("roles") ? ImmutableList.of(vJsonNode.get("roles").asListOfStrings()) : null;

        validationErrors.throwExceptionForPresentErrors();

        if (result.clusterPermissions == null && result.indexPermissions == null && result.tenantPermissions == null) {
            if (result.roles == null || result.roles.isEmpty()) {
                validationErrors.add(new ValidationError(null, "No permissions or roles have been specified"));
            } else {
                // If we have roles, assume an all wildcard permission requests
                result.clusterPermissions = WILDCARD_LIST;
                result.indexPermissions = IndexPermissions.ALL;
                result.tenantPermissions = TenantPermissions.ALL;

                return result;
            }
        }

        if (result.clusterPermissions == null) {
            result.clusterPermissions = ImmutableList.empty();
        }

        if (result.indexPermissions == null) {
            result.indexPermissions = ImmutableList.empty();
        }

        if (result.tenantPermissions == null) {
            result.tenantPermissions = ImmutableList.empty();
        }

        if (result.excludedClusterPermissions == null) {
            result.excludedClusterPermissions = ImmutableList.empty();
        }

        if (result.excludedIndexPermissions == null) {
            result.excludedIndexPermissions = ImmutableList.empty();
        }

        if (!validationErrors.hasErrors()) {
            if (result.clusterPermissions.isEmpty() && result.indexPermissions.isEmpty() && result.tenantPermissions.isEmpty()
                    && (result.roles == null || result.roles.isEmpty())) {
                validationErrors.add(new ValidationError(null, "No permissions or roles have been specified"));
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static RequestedPrivileges totalWildcard() {
        RequestedPrivileges result = new RequestedPrivileges();

        result.clusterPermissions = WILDCARD_LIST;
        result.indexPermissions = IndexPermissions.ALL;
        result.tenantPermissions = TenantPermissions.ALL;

        return result;
    }

    public static RequestedPrivileges parseYaml(String yaml) throws ConfigValidationException {
        return parse(DocNode.parse(Format.YAML).from(yaml));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (clusterPermissions != null && clusterPermissions.size() > 0) {
            builder.field("cluster_permissions", clusterPermissions);
        }

        if (indexPermissions != null && indexPermissions.size() > 0) {
            builder.field("index_permissions", indexPermissions);
        }

        if (tenantPermissions != null && tenantPermissions.size() > 0) {
            builder.field("tenant_permissions", tenantPermissions);
        }

        if (excludedClusterPermissions != null && excludedClusterPermissions.size() > 0) {
            builder.field("exclude_cluster_permissions", excludedClusterPermissions);
        }

        if ((excludedIndexPermissions != null && excludedIndexPermissions.size() > 0)) {
            builder.field("exclude_index_permissions", excludedIndexPermissions);
        }

        if (roles != null && roles.size() > 0) {
            builder.field("roles", roles);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterPermissions == null) ? 0 : clusterPermissions.hashCode());
        result = prime * result + ((excludedClusterPermissions == null) ? 0 : excludedClusterPermissions.hashCode());
        result = prime * result + ((excludedIndexPermissions == null) ? 0 : excludedIndexPermissions.hashCode());
        result = prime * result + ((indexPermissions == null) ? 0 : indexPermissions.hashCode());
        result = prime * result + ((roles == null) ? 0 : roles.hashCode());
        result = prime * result + ((tenantPermissions == null) ? 0 : tenantPermissions.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RequestedPrivileges other = (RequestedPrivileges) obj;
        if (clusterPermissions == null) {
            if (other.clusterPermissions != null) {
                return false;
            }
        } else if (!clusterPermissions.equals(other.clusterPermissions)) {
            return false;
        }
        if (excludedClusterPermissions == null) {
            if (other.excludedClusterPermissions != null) {
                return false;
            }
        } else if (!excludedClusterPermissions.equals(other.excludedClusterPermissions)) {
            return false;
        }
        if (excludedIndexPermissions == null) {
            if (other.excludedIndexPermissions != null) {
                return false;
            }
        } else if (!excludedIndexPermissions.equals(other.excludedIndexPermissions)) {
            return false;
        }
        if (indexPermissions == null) {
            if (other.indexPermissions != null) {
                return false;
            }
        } else if (!indexPermissions.equals(other.indexPermissions)) {
            return false;
        }
        if (roles == null) {
            if (other.roles != null) {
                return false;
            }
        } else if (!roles.equals(other.roles)) {
            return false;
        }
        if (tenantPermissions == null) {
            if (other.tenantPermissions != null) {
                return false;
            }
        } else if (!tenantPermissions.equals(other.tenantPermissions)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RequestedPrivileges [clusterPermissions=" + clusterPermissions + ", indexPermissions=" + indexPermissions + ", tenantPermissions="
                + tenantPermissions + ", roles=" + roles + ", excludedClusterPermissions=" + excludedClusterPermissions
                + ", excludedIndexPermissions=" + excludedIndexPermissions + "]";
    }

}
