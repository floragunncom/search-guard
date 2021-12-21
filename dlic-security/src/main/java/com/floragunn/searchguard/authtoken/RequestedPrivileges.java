/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocType;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;

public class RequestedPrivileges implements Writeable, ToXContentObject, Serializable {
    private static final long serialVersionUID = 5862219250642101795L;
    private static final List<String> WILDCARD_LIST = Collections.singletonList("*");
    private List<String> clusterPermissions;
    private List<IndexPermissions> indexPermissions;
    private List<TenantPermissions> tenantPermissions;
    private List<String> roles;
    private List<String> excludedClusterPermissions;
    private List<ExcludedIndexPermissions> excludedIndexPermissions;

    public RequestedPrivileges(StreamInput in) throws IOException {
        this.clusterPermissions = in.readStringList();
        this.indexPermissions = in.readList(IndexPermissions::new);
        this.tenantPermissions = in.readList(TenantPermissions::new);
        this.excludedClusterPermissions = in.readStringList();
        this.excludedIndexPermissions = in.readList(ExcludedIndexPermissions::new);
        this.roles = in.readOptionalStringList();
    }

    private RequestedPrivileges() {
    }

    public List<String> getClusterPermissions() {
        return clusterPermissions;
    }

    public List<IndexPermissions> getIndexPermissions() {
        return indexPermissions;
    }

    public List<TenantPermissions> getTenantPermissions() {
        return tenantPermissions;
    }

    public List<String> getRoles() {
        return roles;
    }

    public List<String> getExcludedClusterPermissions() {
        return excludedClusterPermissions;
    }

    public List<ExcludedIndexPermissions> getExcludedIndexPermissions() {
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

        List<String> newExcludedClusterPermissions = new ArrayList<>(this.excludedClusterPermissions);
        newExcludedClusterPermissions.addAll(excludeAddionalClusterPermissions);
        result.excludedClusterPermissions = Collections.unmodifiableList(newExcludedClusterPermissions);

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

        List<ExcludedIndexPermissions> newExcludedIndexPermissions = new ArrayList<>(this.excludedIndexPermissions);
        newExcludedIndexPermissions.addAll(excludeAddionalIndexPermissions);
        result.excludedIndexPermissions = Collections.unmodifiableList(newExcludedIndexPermissions);

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

    SgDynamicConfiguration<RoleV7> toRolesConfig() {
        SgDynamicConfiguration<RoleV7> roles = SgDynamicConfiguration.empty();

        RoleV7 role = new RoleV7();

        role.setCluster_permissions(new ArrayList<>(clusterPermissions));
        role.setExclude_cluster_permissions(new ArrayList<>(excludedClusterPermissions));

        List<RoleV7.Index> roleIndexPermissions = new ArrayList<>();

        for (IndexPermissions indexPermissionsEntry : this.indexPermissions) {
            RoleV7.Index roleIndex = new RoleV7.Index();

            roleIndex.setIndex_patterns(new ArrayList<>(indexPermissionsEntry.indexPatterns));
            roleIndex.setAllowed_actions(new ArrayList<>(indexPermissionsEntry.allowedActions));

            roleIndexPermissions.add(roleIndex);
        }

        role.setIndex_permissions(roleIndexPermissions);

        List<RoleV7.ExcludeIndex> roleExcludeIndexPermissions = new ArrayList<>();

        for (ExcludedIndexPermissions indexPermissionsEntry : this.excludedIndexPermissions) {
            RoleV7.ExcludeIndex roleExcludeIndex = new RoleV7.ExcludeIndex();

            roleExcludeIndex.setIndex_patterns(new ArrayList<>(indexPermissionsEntry.indexPatterns));
            roleExcludeIndex.setActions(new ArrayList<>(indexPermissionsEntry.actions));

            roleExcludeIndexPermissions.add(roleExcludeIndex);
        }

        role.setIndex_permissions(roleIndexPermissions);
        List<RoleV7.Tenant> roleTenantPermissions = new ArrayList<>();

        for (TenantPermissions tenantPermissionsEntry : this.tenantPermissions) {
            RoleV7.Tenant roleTenant = new RoleV7.Tenant();

            roleTenant.setTenant_patterns(new ArrayList<>(tenantPermissionsEntry.tenantPatterns));
            roleTenant.setAllowed_actions(new ArrayList<>(tenantPermissionsEntry.allowedActions));

            roleTenantPermissions.add(roleTenant);
        }

        role.setTenant_permissions(roleTenantPermissions);

        roles.putCEntry("_requested_privileges", role);

        return roles;
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

        private static final long serialVersionUID = -2567351561923741922L;
        private List<String> indexPatterns;
        private List<String> allowedActions;

        IndexPermissions(List<String> indexPatterns, List<String> allowedActions) {
            this.indexPatterns = indexPatterns;
            this.allowedActions = allowedActions;
        }

        IndexPermissions(StreamInput in) throws IOException {
            this.indexPatterns = in.readStringList();
            this.allowedActions = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indexPatterns);
            out.writeStringCollection(allowedActions);
        }

        public static IndexPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<String> indexPatterns = vJsonNode.get("index_patterns").required().asList().minElements(1).ofStrings();
            List<String> allowedActions = vJsonNode.get("allowed_actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new IndexPermissions(indexPatterns, allowedActions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index_patterns", indexPatterns);
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
            return indexPatterns.contains("*") & allowedActions.contains("*");
        }
    }

    public static class TenantPermissions implements Writeable, ToXContentObject, Serializable {

        private static final long serialVersionUID = 170036537583928629L;
        private List<String> tenantPatterns;
        private List<String> allowedActions;

        TenantPermissions(List<String> tenantPatterns, List<String> allowedActions) {
            this.tenantPatterns = tenantPatterns;
            this.allowedActions = allowedActions;
        }

        TenantPermissions(StreamInput in) throws IOException {
            this.tenantPatterns = in.readStringList();
            this.allowedActions = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(tenantPatterns);
            out.writeStringCollection(allowedActions);
        }

        public static TenantPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<String> tenantPatterns = vJsonNode.get("tenant_patterns").required().asList().minElements(1).ofStrings();
            List<String> allowedActions = vJsonNode.get("allowed_actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new TenantPermissions(tenantPatterns, allowedActions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("tenant_patterns", tenantPatterns);
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
            return tenantPatterns.contains("*") & allowedActions.contains("*");
        }

    }

    public static class ExcludedIndexPermissions implements Writeable, ToXContentObject, Serializable {

        private static final long serialVersionUID = -2567351561923741922L;
        private List<String> indexPatterns;
        private List<String> actions;

        ExcludedIndexPermissions(List<String> indexPatterns, List<String> actions) {
            this.indexPatterns = indexPatterns;
            this.actions = actions;
        }

        ExcludedIndexPermissions(StreamInput in) throws IOException {
            this.indexPatterns = in.readStringList();
            this.actions = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indexPatterns);
            out.writeStringCollection(actions);
        }

        public static ExcludedIndexPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<String> indexPatterns = vJsonNode.get("index_patterns").required().asList().minElements(1).ofStrings();
            List<String> actions = vJsonNode.get("actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new ExcludedIndexPermissions(indexPatterns, actions);
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

        result.clusterPermissions = vJsonNode.get("cluster_permissions").asListOfStrings();
        result.indexPermissions = vJsonNode.get("index_permissions").asList(IndexPermissions::parse);
        result.tenantPermissions = vJsonNode.get("tenant_permissions").asList(TenantPermissions::parse);
        result.excludedClusterPermissions = vJsonNode.get("exclude_cluster_permissions").asListOfStrings();
        result.excludedIndexPermissions = vJsonNode.get("exclude_index_permissions").asList(ExcludedIndexPermissions::parse);
        result.roles = vJsonNode.get("roles").asListOfStrings();

        validationErrors.throwExceptionForPresentErrors();

        if (result.clusterPermissions == null && result.indexPermissions == null && result.tenantPermissions == null) {
            if (result.roles == null || result.roles.isEmpty()) {
                validationErrors.add(new ValidationError(null, "No permissions or roles have been specified"));
            } else {
                // If we have roles, assume an all wildcard permission requests
                result.clusterPermissions = WILDCARD_LIST;
                result.indexPermissions = Arrays.asList(new IndexPermissions(WILDCARD_LIST, WILDCARD_LIST));
                result.tenantPermissions = Arrays.asList(new TenantPermissions(WILDCARD_LIST, WILDCARD_LIST));

                return result;
            }
        }

        if (result.clusterPermissions == null) {
            result.clusterPermissions = Collections.emptyList();
        }

        if (result.indexPermissions == null) {
            result.indexPermissions = Collections.emptyList();
        }

        if (result.tenantPermissions == null) {
            result.tenantPermissions = Collections.emptyList();
        }

        if (result.excludedClusterPermissions == null) {
            result.excludedClusterPermissions = Collections.emptyList();
        }

        if (result.excludedIndexPermissions == null) {
            result.excludedIndexPermissions = Collections.emptyList();
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
        result.indexPermissions = Arrays.asList(new IndexPermissions(WILDCARD_LIST, WILDCARD_LIST));
        result.tenantPermissions = Arrays.asList(new TenantPermissions(WILDCARD_LIST, WILDCARD_LIST));

        return result;
    }

    public static RequestedPrivileges parseYaml(String yaml) throws ConfigValidationException {
        return parse(DocNode.parse(DocType.YAML).from(yaml));
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
