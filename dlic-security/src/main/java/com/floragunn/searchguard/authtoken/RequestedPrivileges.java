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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

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
    
    public static final RequestedPrivileges ALL = new RequestedPrivileges(WILDCARD_LIST, IndexPermissions.ALL, TenantPermissions.ALL, AliasPermissions.ALL, DataStreamPermissions.ALL);
    private static final Logger log = LogManager.getLogger(AuthTokenService.class);

    private ImmutableList<String> clusterPermissions;
    private ImmutableList<IndexPermissions> indexPermissions;
    private ImmutableList<TenantPermissions> tenantPermissions;
    private ImmutableList<AliasPermissions> aliasPermissions;
    private ImmutableList<DataStreamPermissions> dataStreamPermissions;
    private ImmutableList<String> roles;
    private ImmutableList<String> excludedClusterPermissions;
    @Deprecated
    private ImmutableList<ExcludedIndexPermissions> excludedIndexPermissions;

    public RequestedPrivileges(StreamInput in) throws IOException {
        this.clusterPermissions = ImmutableList.of(in.readStringCollectionAsList());
        this.indexPermissions = ImmutableList.of(in.readCollectionAsList(IndexPermissions::new));
        this.tenantPermissions = ImmutableList.of(in.readCollectionAsList(TenantPermissions::new));
        this.excludedClusterPermissions = ImmutableList.of(in.readStringCollectionAsList());
        this.excludedIndexPermissions = ImmutableList.of(in.readCollectionAsList(ExcludedIndexPermissions::new));
        List<String> roles = in.readOptionalStringCollectionAsList();
        this.roles = roles != null ? ImmutableList.of(roles) : null;
        this.aliasPermissions = ImmutableList.of(in.readCollectionAsList(AliasPermissions::new));
        this.dataStreamPermissions = ImmutableList.of(in.readCollectionAsList(DataStreamPermissions::new));
    }

    RequestedPrivileges(List<String> clusterPermissions, List<IndexPermissions> indexPermissions, List<TenantPermissions> tenantPermissions,
                        List<AliasPermissions> aliasPermissions, List<DataStreamPermissions> dataStreamPermissions) {
        this.clusterPermissions = ImmutableList.of(clusterPermissions);
        this.indexPermissions = ImmutableList.of(indexPermissions);
        this.tenantPermissions = ImmutableList.of(tenantPermissions);
        this.aliasPermissions = ImmutableList.of(aliasPermissions);
        this.dataStreamPermissions = ImmutableList.of(dataStreamPermissions);
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

    public ImmutableList<AliasPermissions> getAliasPermissions() {
        return aliasPermissions;
    }

    public ImmutableList<DataStreamPermissions> getDataStreamPermissions() {
        return dataStreamPermissions;
    }

    public ImmutableList<String> getRoles() {
        return roles;
    }

    public ImmutableList<String> getExcludedClusterPermissions() {
        return excludedClusterPermissions;
    }

    public RequestedPrivileges excludeClusterPermissions(List<String> excludeAddionalClusterPermissions) {
        if (excludeAddionalClusterPermissions == null || excludeAddionalClusterPermissions.size() == 0) {
            return this;
        }

        RequestedPrivileges result = new RequestedPrivileges();
        result.clusterPermissions = this.clusterPermissions;
        result.indexPermissions = this.indexPermissions;
        result.tenantPermissions = this.tenantPermissions;
        result.aliasPermissions = this.aliasPermissions;
        result.dataStreamPermissions = this.dataStreamPermissions;
        result.roles = this.roles;
        result.excludedIndexPermissions = this.excludedIndexPermissions;
        result.excludedClusterPermissions = this.excludedClusterPermissions.with(excludeAddionalClusterPermissions);

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

        if (aliasPermissions.size() != 1) {
            return false;
        }

        if (!aliasPermissions.get(0).isWildcard()) {
            return false;
        }

        if (dataStreamPermissions.size() != 1) {
            return false;
        }

        if (!dataStreamPermissions.get(0).isWildcard()) {
            return false;
        }

        return true;
    }

    SgDynamicConfiguration<Role> toRolesConfig() {
        ImmutableList<Role.Index> indexPermissions = this.indexPermissions.map((p) -> p.toRoleIndex());
        ImmutableList<Role.Tenant> tenantPermissions = this.tenantPermissions.map((p) -> p.toRoleTenant());
        ImmutableList<Role.Alias> aliasPermissions = this.aliasPermissions.map((p) -> p.toRoleAlias());
        ImmutableList<Role.DataStream> dataStreamPermissions = this.dataStreamPermissions.map((p) -> p.toRoleDataStream());

        Role role = new Role(null, false, false, false, "requested privileges", clusterPermissions, indexPermissions, aliasPermissions,
                dataStreamPermissions, tenantPermissions, excludedClusterPermissions);
        
        return SgDynamicConfiguration.of(CType.ROLES, RESTRICTION_ROLE, role);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeStringCollection(clusterPermissions);
        out.writeCollection(indexPermissions);
        out.writeCollection(tenantPermissions);
        out.writeStringCollection(excludedClusterPermissions);
        out.writeCollection(excludedIndexPermissions);
        out.writeOptionalStringCollection(roles);
        out.writeCollection(aliasPermissions);
        out.writeCollection(dataStreamPermissions);
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
            this.indexPatterns =  ImmutableList.map(in.readStringCollectionAsList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling " + s, e);
                    return null;
                }
            });
            this.allowedActions = ImmutableList.of(in.readStringCollectionAsList());
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
            this.tenantPatterns = ImmutableList.map(in.readStringCollectionAsList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling " + s, e);
                    return null;
                }
            });
            this.allowedActions = ImmutableList.of(in.readStringCollectionAsList());
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

    public static class AliasPermissions implements Writeable, ToXContentObject, Serializable {

        public static final ImmutableList<AliasPermissions> ALL = ImmutableList.of(new AliasPermissions(WILDCARD_TEMPLATE_PATTERN_LIST, WILDCARD_LIST));

        private static final long serialVersionUID = 9149970087895696366L;
        private ImmutableList<Template<Pattern>> aliasPatterns;
        private ImmutableList<String> allowedActions;

        AliasPermissions(ImmutableList<Template<Pattern>> aliasPatterns, ImmutableList<String> allowedActions) {
            this.aliasPatterns = aliasPatterns;
            this.allowedActions = allowedActions;
        }

        AliasPermissions(StreamInput in) throws IOException {
            this.aliasPatterns =  ImmutableList.map(in.readStringCollectionAsList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling {}", s, e);
                    return null;
                }
            });
            this.allowedActions = ImmutableList.of(in.readStringCollectionAsList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(aliasPatterns.map((t) -> t.getSource()));
            out.writeStringCollection(allowedActions);
        }

        public static AliasPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<Template<Pattern>> aliasPatterns = vJsonNode.get("alias_patterns").required().asList().minElements(1).ofTemplates(Pattern::create);
            List<String> allowedActions = vJsonNode.get("allowed_actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new AliasPermissions(ImmutableList.of(aliasPatterns), ImmutableList.of(allowedActions));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("alias_patterns", aliasPatterns.map((t) -> t.getSource()));
            builder.field("allowed_actions", allowedActions);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "AliasPermissions [aliasPatterns=" + aliasPatterns + ", allowedActions=" + allowedActions + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((allowedActions == null) ? 0 : allowedActions.hashCode());
            result = prime * result + ((aliasPatterns == null) ? 0 : aliasPatterns.hashCode());
            return result;
        }

        public Role.Alias toRoleAlias() {
            return new Role.Alias(aliasPatterns, null, null, null, allowedActions);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AliasPermissions other = (AliasPermissions) obj;
            if (allowedActions == null) {
                if (other.allowedActions != null)
                    return false;
            } else if (!allowedActions.equals(other.allowedActions))
                return false;
            if (aliasPatterns == null) {
                if (other.aliasPatterns != null)
                    return false;
            } else if (!aliasPatterns.equals(other.aliasPatterns))
                return false;
            return true;
        }

        public boolean isWildcard() {
            return allowedActions.contains("*") && aliasPatterns.forAnyApplies((t) -> t.isConstant() && t.getConstantValue().isWildcard());
        }
    }

    public static class DataStreamPermissions implements Writeable, ToXContentObject, Serializable {

        public static final ImmutableList<DataStreamPermissions> ALL = ImmutableList.of(new DataStreamPermissions(WILDCARD_TEMPLATE_PATTERN_LIST, WILDCARD_LIST));

        private static final long serialVersionUID = 6013062077364983299L;
        private ImmutableList<Template<Pattern>> dataStreamPatterns;
        private ImmutableList<String> allowedActions;

        DataStreamPermissions(ImmutableList<Template<Pattern>> dataStreamPatterns, ImmutableList<String> allowedActions) {
            this.dataStreamPatterns = dataStreamPatterns;
            this.allowedActions = allowedActions;
        }

        DataStreamPermissions(StreamInput in) throws IOException {
            this.dataStreamPatterns =  ImmutableList.map(in.readStringCollectionAsList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling " + s, e);
                    return null;
                }
            });
            this.allowedActions = ImmutableList.of(in.readStringCollectionAsList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(dataStreamPatterns.map((t) -> t.getSource()));
            out.writeStringCollection(allowedActions);
        }

        public static DataStreamPermissions parse(DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            List<Template<Pattern>> dataStreamPatterns = vJsonNode.get("data_stream_patterns").required().asList().minElements(1).ofTemplates(Pattern::create);
            List<String> allowedActions = vJsonNode.get("allowed_actions").required().asList().minElements(1).ofStrings();

            validationErrors.throwExceptionForPresentErrors();

            return new DataStreamPermissions(ImmutableList.of(dataStreamPatterns), ImmutableList.of(allowedActions));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("data_stream_patterns", dataStreamPatterns.map((t) -> t.getSource()));
            builder.field("allowed_actions", allowedActions);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "DataStreamPermissions [dataStreamPatterns=" + dataStreamPatterns + ", allowedActions=" + allowedActions + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((allowedActions == null) ? 0 : allowedActions.hashCode());
            result = prime * result + ((dataStreamPatterns == null) ? 0 : dataStreamPatterns.hashCode());
            return result;
        }

        public Role.DataStream toRoleDataStream() {
            return new Role.DataStream(dataStreamPatterns, null, null, null, allowedActions);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DataStreamPermissions other = (DataStreamPermissions) obj;
            if (allowedActions == null) {
                if (other.allowedActions != null)
                    return false;
            } else if (!allowedActions.equals(other.allowedActions))
                return false;
            if (dataStreamPatterns == null) {
                if (other.dataStreamPatterns != null)
                    return false;
            } else if (!dataStreamPatterns.equals(other.dataStreamPatterns))
                return false;
            return true;
        }

        public boolean isWildcard() {
            return allowedActions.contains("*") && dataStreamPatterns.forAnyApplies((t) -> t.isConstant() && t.getConstantValue().isWildcard());
        }
    }

    /**
     * @deprecated no longer supported. This is only left for parsing StreamInput.
     */
    public static class ExcludedIndexPermissions implements Writeable, ToXContentObject, Serializable {

        private static final long serialVersionUID = -2567351561923741922L;
        private ImmutableList<Template<Pattern>> indexPatterns;
        private ImmutableList<String> actions;

        ExcludedIndexPermissions(StreamInput in) throws IOException {
            this.indexPatterns =  ImmutableList.map(in.readStringCollectionAsList(), (s) -> {
                try {
                    return new Template<>(s, Pattern::create);
                } catch (ConfigValidationException e) {
                    log.error("Error compiling " + s, e);
                    return null;
                }
            });
            this.actions = ImmutableList.of(in.readStringCollectionAsList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indexPatterns.map((t) -> t.getSource()));
            out.writeStringCollection(actions);
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

        result.clusterPermissions = vJsonNode.hasNonNull("cluster_permissions") ? ImmutableList.of(vJsonNode.get("cluster_permissions").asListOfStrings()) : null;
        result.indexPermissions =   vJsonNode.hasNonNull("index_permissions") ? ImmutableList.of(vJsonNode.get("index_permissions").asList(IndexPermissions::parse)) : null;
        result.tenantPermissions = vJsonNode.hasNonNull("tenant_permissions") ? ImmutableList.of(vJsonNode.get("tenant_permissions").asList(TenantPermissions::parse)) : null;
        result.aliasPermissions = vJsonNode.hasNonNull("alias_permissions") ? ImmutableList.of(vJsonNode.get("alias_permissions").asList(AliasPermissions::parse)) : null;
        result.dataStreamPermissions = vJsonNode.hasNonNull("data_stream_permissions") ? ImmutableList.of(vJsonNode.get("data_stream_permissions").asList(DataStreamPermissions::parse)) : null;
        result.excludedClusterPermissions = vJsonNode.hasNonNull("exclude_cluster_permissions") ? ImmutableList.of(vJsonNode.get("exclude_cluster_permissions").asListOfStrings()) : null;
        result.excludedIndexPermissions =  null;
        result.roles = vJsonNode.hasNonNull("roles") ? ImmutableList.of(vJsonNode.get("roles").asListOfStrings()) : null;

        validationErrors.throwExceptionForPresentErrors();

        if (result.clusterPermissions == null && result.indexPermissions == null && result.tenantPermissions == null
                && result.aliasPermissions == null && result.dataStreamPermissions == null) {
            if (result.roles == null || result.roles.isEmpty()) {
                validationErrors.add(new ValidationError(null, "No permissions or roles have been specified"));
            } else {
                // If we have roles, assume an all wildcard permission requests
                result.clusterPermissions = WILDCARD_LIST;
                result.indexPermissions = IndexPermissions.ALL;
                result.tenantPermissions = TenantPermissions.ALL;
                result.aliasPermissions = AliasPermissions.ALL;
                result.dataStreamPermissions = DataStreamPermissions.ALL;

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

        if (result.aliasPermissions == null) {
            result.aliasPermissions = ImmutableList.empty();
        }

        if (result.dataStreamPermissions == null) {
            result.dataStreamPermissions = ImmutableList.empty();
        }

        if (result.excludedClusterPermissions == null) {
            result.excludedClusterPermissions = ImmutableList.empty();
        }

        if (result.excludedIndexPermissions == null) {
            result.excludedIndexPermissions = ImmutableList.empty();
        }

        if (!validationErrors.hasErrors()) {
            if (result.clusterPermissions.isEmpty() && result.indexPermissions.isEmpty() && result.tenantPermissions.isEmpty()
                    && result.aliasPermissions.isEmpty() && result.dataStreamPermissions.isEmpty()
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
        result.aliasPermissions = AliasPermissions.ALL;
        result.dataStreamPermissions = DataStreamPermissions.ALL;

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

        if (aliasPermissions != null && aliasPermissions.size() > 0) {
            builder.field("alias_permissions", aliasPermissions);
        }

        if (dataStreamPermissions != null && dataStreamPermissions.size() > 0) {
            builder.field("data_stream_permissions", dataStreamPermissions);
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
        result = prime * result + ((aliasPermissions == null) ? 0 : aliasPermissions.hashCode());
        result = prime * result + ((dataStreamPermissions == null) ? 0 : dataStreamPermissions.hashCode());
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
        if (aliasPermissions == null) {
            if (other.aliasPermissions != null) {
                return false;
            }
        } else if (!aliasPermissions.equals(other.aliasPermissions)) {
            return false;
        }
        if (dataStreamPermissions == null) {
            if (other.dataStreamPermissions != null) {
                return false;
            }
        } else if (!dataStreamPermissions.equals(other.dataStreamPermissions)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RequestedPrivileges [clusterPermissions=" + clusterPermissions + ", indexPermissions=" + indexPermissions + ", tenantPermissions="
                + tenantPermissions + ", aliasPermissions=" + aliasPermissions + ", dataStreamPermissions=" + dataStreamPermissions
                + ", roles=" + roles + ", excludedClusterPermissions=" + excludedClusterPermissions + ", excludedIndexPermissions="
                + excludedIndexPermissions + "]";
    }

}
