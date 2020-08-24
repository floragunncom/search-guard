package com.floragunn.searchguard.authtoken;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class RequestedPrivileges implements Writeable, ToXContentObject, Serializable {
    private static final long serialVersionUID = 5862219250642101795L;
    private List<String> clusterPermissions;
    private List<IndexPermissions> indexPermissions;
    private List<TenantPermissions> tenantPermissions;
    private List<String> roles;

    public RequestedPrivileges(List<String> clusterPermissions, List<IndexPermissions> indexPermissions, List<TenantPermissions> tenantPermissions,
            List<String> roles) {
        super();
        this.clusterPermissions = clusterPermissions;
        this.indexPermissions = indexPermissions;
        this.tenantPermissions = tenantPermissions;
        this.roles = roles;
    }

    public RequestedPrivileges(StreamInput in) throws IOException {
        this.clusterPermissions = in.readStringList();
        this.indexPermissions = in.readList(IndexPermissions::new);
        this.tenantPermissions = in.readList(TenantPermissions::new);
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

    SgDynamicConfiguration<RoleV7> toRolesConfig() {
        SgDynamicConfiguration<RoleV7> roles = SgDynamicConfiguration.empty();

        RoleV7 role = new RoleV7();

        role.setCluster_permissions(new ArrayList<>(clusterPermissions));

        List<RoleV7.Index> roleIndexPermissions = new ArrayList<>();

        for (IndexPermissions indexPermissionsEntry : this.indexPermissions) {
            RoleV7.Index roleIndex = new RoleV7.Index();

            roleIndex.setIndex_patterns(new ArrayList<>(indexPermissionsEntry.indexPatterns));
            roleIndex.setAllowed_actions(new ArrayList<>(indexPermissionsEntry.allowedActions));

            roleIndexPermissions.add(roleIndex);
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

        public static IndexPermissions parse(JsonNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

            List<String> indexPatterns = vJsonNode.requiredStringList("index_patterns", 1);
            List<String> allowedActions = vJsonNode.requiredStringList("allowed_actions", 1);

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

        public static TenantPermissions parse(JsonNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

            List<String> tenantPatterns = vJsonNode.requiredStringList("tenant_patterns", 1);
            List<String> allowedActions = vJsonNode.requiredStringList("allowed_actions", 1);

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
    }

    public static RequestedPrivileges parse(JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);
        RequestedPrivileges result = new RequestedPrivileges();

        result.clusterPermissions = vJsonNode.stringList("cluster_permissions");
        result.indexPermissions = vJsonNode.list("index_permissions", IndexPermissions::parse);
        result.tenantPermissions = vJsonNode.list("tenant_permissions", TenantPermissions::parse);
        result.roles = vJsonNode.stringList("roles");

        if (result.clusterPermissions == null) {
            result.clusterPermissions = Collections.emptyList();
        }

        if (result.indexPermissions == null) {
            result.indexPermissions = Collections.emptyList();
        }

        if (result.tenantPermissions == null) {
            result.tenantPermissions = Collections.emptyList();
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

    public static RequestedPrivileges parseYaml(String yaml) throws ConfigValidationException {
        return parse(ValidatingJsonParser.readYamlTree(yaml));
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

        if (roles != null && roles.size() > 0) {
            builder.field("roles", roles);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "RequestedPrivileges [clusterPermissions=" + clusterPermissions + ", indexPermissions=" + indexPermissions + ", tenantPermissions="
                + tenantPermissions + ", roles=" + roles + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterPermissions == null) ? 0 : clusterPermissions.hashCode());
        result = prime * result + ((indexPermissions == null) ? 0 : indexPermissions.hashCode());
        result = prime * result + ((roles == null) ? 0 : roles.hashCode());
        result = prime * result + ((tenantPermissions == null) ? 0 : tenantPermissions.hashCode());
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
        RequestedPrivileges other = (RequestedPrivileges) obj;
        if (clusterPermissions == null) {
            if (other.clusterPermissions != null)
                return false;
        } else if (!clusterPermissions.equals(other.clusterPermissions))
            return false;
        if (indexPermissions == null) {
            if (other.indexPermissions != null)
                return false;
        } else if (!indexPermissions.equals(other.indexPermissions))
            return false;
        if (roles == null) {
            if (other.roles != null)
                return false;
        } else if (!roles.equals(other.roles))
            return false;
        if (tenantPermissions == null) {
            if (other.tenantPermissions != null)
                return false;
        } else if (!tenantPermissions.equals(other.tenantPermissions))
            return false;
        return true;
    }

}
