package com.floragunn.searchguard.authtoken;

import java.io.IOException;
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
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7.Index;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class RequestedPrivileges implements Writeable, ToXContentObject {
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
    
    SgDynamicConfiguration<RoleV7> toRolesConfig()  {
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

    public static class IndexPermissions implements Writeable, ToXContentObject {
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
    }

    public static class TenantPermissions implements Writeable, ToXContentObject {
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

        if (result.roles == null) {
            result.roles = Collections.emptyList();
        }

        if (!validationErrors.hasErrors()) {
            if (result.clusterPermissions.isEmpty() && result.indexPermissions.isEmpty() && result.tenantPermissions.isEmpty()
                    && result.roles.isEmpty()) {
                validationErrors.add(new ValidationError(null, "No permissions or roles have been specified"));
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (clusterPermissions != null) {
            builder.field("cluster_permissions", clusterPermissions);
        }

        if (indexPermissions != null) {
            builder.field("index_permissions", indexPermissions);
        }

        if (tenantPermissions != null) {
            builder.field("tenant_permissions", tenantPermissions);
        }
        
        if (roles != null) {
            builder.field("roles", roles);
        }

        builder.endObject();
        return builder;
    }

}
