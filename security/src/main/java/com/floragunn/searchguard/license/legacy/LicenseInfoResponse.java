/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.license.legacy;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.searchguard.license.SearchGuardLicense;

@Deprecated
public class LicenseInfoResponse extends BaseNodesResponse<LicenseInfoNodeResponse> implements ToXContent {

    public LicenseInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public LicenseInfoResponse(final ClusterName clusterName, List<LicenseInfoNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<LicenseInfoNodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readCollectionAsList(LicenseInfoNodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<LicenseInfoNodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        final List<LicenseInfoNodeResponse> allNodes = getNodes();

        if (allNodes.isEmpty()) {
            throw new IOException("All nodes failed");
        }

        final List<LicenseInfoNodeResponse> nonNullLicenseNodes = allNodes.stream().filter(r -> r != null && r.getLicense() != null)
                .collect(Collectors.toList());

        builder.startObject("sg_license");

        if (nonNullLicenseNodes.size() != allNodes.size() && nonNullLicenseNodes.size() > 0) {

            final SearchGuardLicense license = nonNullLicenseNodes.get(0).getLicense();

            builder.field("uid", license.getUid());
            builder.field("type", license.getType());
            builder.field("features", license.getFeatures());
            builder.field("issue_date", license.getIssueDate());
            builder.field("expiry_date", license.getExpiryDate());
            builder.field("issued_to", license.getIssuedTo());
            builder.field("issuer", license.getIssuer());
            builder.field("start_date", license.getStartDate());
            builder.field("major_version", license.getMajorVersion());
            builder.field("cluster_name", license.getClusterName());
            builder.field("msgs", new String[] { "License mismatch across some nodes" });
            builder.field("expiry_in_days", license.getExpiresInDays());
            builder.field("is_expired", license.isExpired());
            builder.field("is_valid", false);
            builder.field("action", "Enable or disable enterprise modules on all your nodes");
            builder.field("prod_usage", "No");
            builder.field("license_required", true);
            builder.field("allowed_node_count_per_cluster",
                    license.getAllowedNodeCount() > 1500 ? "unlimited" : String.valueOf(license.getAllowedNodeCount()));

        } else if (nonNullLicenseNodes.size() == 0) {
            builder.field("msgs", new String[] { "No license required because enterprise modules not enabled." });
            builder.field("license_required", false);
        } else {

            final SearchGuardLicense license = nonNullLicenseNodes.get(0).getLicense();

            builder.field("uid", license.getUid());
            builder.field("type", license.getType());
            builder.field("features", license.getFeatures());
            builder.field("issue_date", license.getIssueDate());
            builder.field("expiry_date", license.getExpiryDate());
            builder.field("issued_to", license.getIssuedTo());
            builder.field("issuer", license.getIssuer());
            builder.field("start_date", license.getStartDate());
            builder.field("major_version", license.getMajorVersion());
            builder.field("cluster_name", license.getClusterName());
            builder.field("msgs", license.getMsgs());
            builder.field("expiry_in_days", license.getExpiresInDays());
            builder.field("is_expired", license.isExpired());
            builder.field("is_valid", license.isValid());
            builder.field("action", license.getAction());
            builder.field("prod_usage", license.getProdUsage());
            builder.field("license_required", true);
            builder.field("allowed_node_count_per_cluster",
                    license.getAllowedNodeCount() > 1500 ? "unlimited" : String.valueOf(license.getAllowedNodeCount()));
        }

        builder.endObject();

        builder.startObject("modules");

        List<ModuleInfo> mod0 = new LinkedList<>(allNodes.get(0).getModules());

        Set<String> encounteredTypes = new HashSet<>();

        for (ModuleInfo moduleInfo : mod0) {
            Map<String, String> infoAsMap = moduleInfo.getAsMap();

            String type = moduleInfo.getModuleType().name();

            int count = 0;

            while (encounteredTypes.contains(type)) {
                count++;
                type = type + "_" + count;
            }

            encounteredTypes.add(type);

            builder.field(type, infoAsMap);
        }

        boolean mismatch = false;
        List<String> mismatchedNodes = new LinkedList<>();
        for (LicenseInfoNodeResponse node : allNodes) {
            for (ModuleInfo nodeModuleInfo : node.getModules()) {
                if (!mod0.contains(nodeModuleInfo)) {
                    mismatch = true;
                    mismatchedNodes.add(node.getNode().getName());
                    break;
                }
            }
        }
        builder.endObject();

        builder.startObject("compatibility");
        builder.field("modules_mismatch", mismatch);
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Deprecated
    static enum ModuleType {
        REST_MANAGEMENT_API("REST Management API", "com.floragunn.searchguard.dlic.rest.api.SearchGuardRestApiActions", Boolean.TRUE),
        DLSFLS("Document- and Field-Level Security", "com.floragunn.searchguard.dlsfls.legacy.lucene.SearchGuardFlsDlsIndexSearcherWrapper", Boolean.TRUE),
        AUDITLOG("Audit Logging", "com.floragunn.searchguard.auditlog.impl.AuditLogImpl", Boolean.TRUE),
        MULTITENANCY("Kibana Multitenancy", "com.floragunn.searchguard.enterprise.femt.FeMultiTenancyModule", Boolean.TRUE),
        LDAP_AUTHENTICATION_BACKEND("LDAP authentication backend", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthenticationBackend", Boolean.TRUE),
        LDAP_AUTHORIZATION_BACKEND("LDAP authorization backend", "com.floragunn.dlic.auth.ldap.backend.LDAPAuthorizationBackend", Boolean.TRUE),
        KERBEROS_AUTHENTICATION_BACKEND("Kerberos authentication backend", "com.floragunn.searchguard.enterprise.auth.kerberos.HTTPSpnegoAuthenticator", Boolean.TRUE),
        JWT_AUTHENTICATION_BACKEND("JWT authentication backend", "com.floragunn.dlic.auth.http.jwt.HTTPJwtAuthenticator", Boolean.TRUE),
        OPENID_AUTHENTICATION_BACKEND("OpenID authentication backend", "com.floragunn.dlic.auth.http.jwt.keybyoidc.HTTPJwtKeyByOpenIdConnectAuthenticator", Boolean.TRUE),
        SAML_AUTHENTICATION_BACKEND("SAML authentication backend", "com.floragunn.dlic.auth.http.saml.HTTPSamlAuthenticator", Boolean.TRUE),
        INTERNAL_USERS_AUTHENTICATION_BACKEND("Internal users authentication backend", "", Boolean.FALSE),
        HTTP_BASIC_AUTHENTICATOR("HTTP Basic Authenticator", "", Boolean.FALSE),
        HTTP_CLIENTCERT_AUTHENTICATOR("HTTP Client Certificate Authenticator", "", Boolean.FALSE),
        CUSTOM_HTTP_AUTHENTICATOR("Custom HTTP authenticator", null, Boolean.TRUE),
        CUSTOM_AUTHENTICATION_BACKEND("Custom authentication backend", null, Boolean.TRUE),
        CUSTOM_AUTHORIZATION_BACKEND("Custom authorization backend", null, Boolean.TRUE),
        CUSTOM_INTERCLUSTER_REQUEST_EVALUATOR("Intercluster Request Evaluator", null, Boolean.FALSE),
        CUSTOM_PRINCIPAL_EXTRACTOR("TLS Principal Extractor", null, Boolean.FALSE),
        AUTH_TOKEN_AUTHENTICATION_BACKEND("Search Guard Auth Token authentication backend", "com.floragunn.searchguard.authtoken.AuthTokenAuthenticationBackend", Boolean.TRUE),
        AUTH_TOKEN_HTTP_AUTHENTICATOR("Search Guard Auth Token HTTP authenticator", "com.floragunn.searchguard.authtoken.AuthTokenHttpJwtAuthenticator", Boolean.TRUE),
        SG_STD_MODULE("Search Guard Standard Module", null, Boolean.FALSE),
        UNKNOWN("Unknown type", null, Boolean.TRUE);

        private String description;
        private String defaultImplClass;
        private Boolean isEnterprise = Boolean.TRUE;
       

        private ModuleType(String description, String defaultImplClass, Boolean isEnterprise) {
            this.description = description;
            this.defaultImplClass = defaultImplClass;
            this.isEnterprise = isEnterprise;
        }

        public String getDescription() {
            return this.description;
        }

        public String getDefaultImplClass() {
            return defaultImplClass;
        }

        public Boolean isEnterprise() {
            return isEnterprise;
        }
    }

    
    @Deprecated
    static class ModuleInfo implements Writeable{
                
        private ModuleType moduleType;
        private String classname;
        private String classpath = "";
        private String version = "";
        private String buildTime = "";
        private String gitsha1 = "";
        
        public ModuleInfo(ModuleType moduleType, String classname) {
            assert(moduleType != null);
            this.moduleType = moduleType;
            this.classname = classname;
        }

        public ModuleInfo(final StreamInput in) throws IOException {
            moduleType = in.readEnum(ModuleType.class);
            classname = in.readString();
            classpath = in.readString();
            version = in.readString();
            buildTime = in.readString();
            gitsha1 = in.readString();
            assert(moduleType != null);
        }

        public void setClasspath(String classpath) {
            this.classpath = classpath;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public void setBuildTime(String buildTime) {
            this.buildTime = buildTime;
        }
        
        public String getGitsha1() {
            return gitsha1;
        }

        public void setGitsha1(String gitsha1) {
            this.gitsha1 = gitsha1;
        }

        public ModuleType getModuleType() {
            return moduleType;
        }
        
        public Map<String, String> getAsMap() {
            Map<String, String> infoMap = new HashMap<>();
            infoMap.put("type", moduleType.name());
            infoMap.put("description", moduleType.getDescription());
            infoMap.put("is_enterprise", moduleType.isEnterprise().toString());
            infoMap.put("default_implementation", moduleType.getDefaultImplClass());
            infoMap.put("actual_implementation", this.classname);
            //infoMap.put("classpath", this.classpath); //this can disclose file locations
            infoMap.put("version", this.version);
            infoMap.put("buildTime", this.buildTime);
            infoMap.put("gitsha1", this.gitsha1);
            return infoMap;
        }
        
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(moduleType);
            out.writeString(classname);
            out.writeString(classpath);
            out.writeString(version);
            out.writeString(buildTime);
            out.writeString(gitsha1);
        }
        
     

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((buildTime == null) ? 0 : buildTime.hashCode());
            result = prime * result + ((classname == null) ? 0 : classname.hashCode());
            result = prime * result + ((moduleType == null) ? 0 : moduleType.hashCode());
            result = prime * result + ((version == null) ? 0 : version.hashCode());
            result = prime * result + ((gitsha1 == null) ? 0 : gitsha1.hashCode());
            return result;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof ModuleInfo)) {
                return false;
            }
            ModuleInfo other = (ModuleInfo) obj;
            if (buildTime == null) {
                if (other.buildTime != null) {
                    return false;
                }
            } else if (!buildTime.equals(other.buildTime)) {
                return false;
            }
            if (classname == null) {
                if (other.classname != null) {
                    return false;
                }
            } else if (!classname.equals(other.classname)) {
                return false;
            }
            if (!moduleType.equals(other.moduleType)) {
                return false;
            }
            if (version == null) {
                if (other.version != null) {
                    return false;
                }
            } else if (!version.equals(other.version)) {
                return false;
            }
            if (gitsha1 == null) {
                if (other.gitsha1 != null) {
                    return false;
                }
            } else if (!gitsha1.equals(other.gitsha1)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "Module [type=" + this.moduleType.name() + ", implementing class=" + this.classname + "]";
        }
        
    }

}
