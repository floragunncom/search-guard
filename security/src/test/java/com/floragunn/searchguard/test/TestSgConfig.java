/*
 * Copyright 2021-2024 floragunn GmbH
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

package com.floragunn.searchguard.test;

import static com.floragunn.searchguard.test.RestMatchers.distinctNodesAt;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.hamcrest.Matcher;

import com.floragunn.codova.config.temporal.DurationFormat;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.patch.MergePatch;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.variables.ConfigVarRefreshAction;
import com.floragunn.searchguard.configuration.variables.ConfigVarRefreshAction.Response;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.EsClientProvider.UserCredentialsHolder;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.NestedValueMap;
import com.floragunn.searchguard.test.helper.cluster.NestedValueMap.Path;

public class TestSgConfig {
    private static final Logger log = LogManager.getLogger(TestSgConfig.class);

    private String resourceFolder = null;
    private NestedValueMap overrideSgConfigSettings;
    private NestedValueMap overrideUserSettings;
    private NestedValueMap overrideRoleSettings;
    private NestedValueMap overrideRoleMappingSettings;
    private NestedValueMap overrideFrontendConfigSettings;
    private NestedValueMap overrideFrontendMultiTenancyConfigSettings;
    private NestedValueMap overrideTenantSettings;
    private Authc authc;
    private DlsFls dlsFls;
    private Authz privileges;
    private Sessions sessions;
    private AuthTokenService authTokenService;
    private String indexName = ".searchguard";
    private Map<String, Supplier<Object>> variableSuppliers = new HashMap<>();

    public TestSgConfig() {

    }

    public TestSgConfig configIndexName(String configIndexName) {
        this.indexName = configIndexName;
        return this;
    }

    public TestSgConfig resources(String resourceFolder) {
        this.resourceFolder = resourceFolder;
        return this;
    }

    public TestSgConfig var(String name, Supplier<Object> variableSupplier) {
        this.variableSuppliers.put(name, variableSupplier);
        return this;
    }

    public TestSgConfig sgConfigSettings(String keyPath, Object value, Object... more) {
        if (overrideSgConfigSettings == null) {
            overrideSgConfigSettings = new NestedValueMap();
        }

        overrideSgConfigSettings.put(NestedValueMap.Path.parse(keyPath), value);

        for (int i = 0; i < more.length - 1; i += 2) {
            overrideSgConfigSettings.put(NestedValueMap.Path.parse(String.valueOf(more[i])), more[i + 1]);
        }

        return this;
    }

    public TestSgConfig xff(String proxies) {
        if (overrideSgConfigSettings == null) {
            overrideSgConfigSettings = new NestedValueMap();
        }

        overrideSgConfigSettings.put(new NestedValueMap.Path("sg_config", "dynamic", "http", "xff"),
                NestedValueMap.of("enabled", true, "internalProxies", proxies));

        return this;
    }

    public TestSgConfig authc(Authc authc) {
        this.authc = authc;
        return this;
    }

    public TestSgConfig dlsFls(DlsFls dlsFls) {
        this.dlsFls = dlsFls;
        return this;
    }

    public TestSgConfig frontendAuthc(FrontendAuthc... frontendAuthcz) {
        return frontendAuthc("default", frontendAuthcz);
    }

    public TestSgConfig frontendAuthc(String configId, FrontendAuthc... frontendAuthc) {
        if (overrideFrontendConfigSettings == null) {
            overrideFrontendConfigSettings = new NestedValueMap();
        }

        NestedValueMap mergedConfigs = new NestedValueMap();

        for (FrontendAuthc authc : frontendAuthc) {
            mergedConfigs.putAll(NestedValueMap.copy(authc.toMap()));
        }

        overrideFrontendConfigSettings.put(configId, mergedConfigs);

        return this;
    }

    public TestSgConfig frontendMultiTenancy(FrontendMultiTenancy frontendMultiTenancy) {
        if (overrideFrontendMultiTenancyConfigSettings == null) {
            overrideFrontendMultiTenancyConfigSettings = new NestedValueMap();
        }

        overrideFrontendMultiTenancyConfigSettings.putAllFromAnyMap(frontendMultiTenancy.toJsonMap());

        return this;
    }

    public TestSgConfig tenants(Tenant... tenants) {
        if (overrideTenantSettings == null) {
            overrideTenantSettings = new NestedValueMap();
        }

        for (Tenant tenant : tenants) {
            overrideTenantSettings.putAllFromAnyMap(tenant.toJsonMap());
        }

        return this;
    }

    public TestSgConfig frontendAuthcDebug(boolean debug) {
        return frontendAuthcDebug("default", debug);
    }

    public TestSgConfig frontendAuthcDebug(String configId, boolean debug) {
        if (overrideFrontendConfigSettings == null) {
            overrideFrontendConfigSettings = new NestedValueMap();
        }

        overrideFrontendConfigSettings.put(new Path(configId, "debug"), debug);

        return this;
    }

    public TestSgConfig user(User user) {
        if (user.roleNames != null) {
            return this.user(user.name, user.password, user.attributes, user.roleNames);
        } else {
            return this.user(user.name, user.password, user.attributes, user.roles);
        }
    }

    public TestSgConfig user(String name, UserPassword password, String... sgRoles) {
        return user(name, password, null, sgRoles);
    }

    public TestSgConfig user(String name, UserPassword password, Map<String, Object> attributes, String... sgRoles) {
        if (overrideUserSettings == null) {
            overrideUserSettings = new NestedValueMap();
        }

        overrideUserSettings.put(new NestedValueMap.Path(name, "hash"), password.passwordValueForConfiguration());

        if (sgRoles != null && sgRoles.length > 0) {
            overrideUserSettings.put(new NestedValueMap.Path(name, "search_guard_roles"), sgRoles);
        }

        if (attributes != null && attributes.size() != 0) {
            for (Map.Entry<String, Object> attr : attributes.entrySet()) {
                overrideUserSettings.put(new NestedValueMap.Path(name, "attributes", attr.getKey()), attr.getValue());
            }
        }

        return this;
    }

    public TestSgConfig user(String name, UserPassword password, Role... sgRoles) {
        return user(name, password, null, sgRoles);
    }

    public TestSgConfig user(String name, UserPassword password, Map<String, Object> attributes, Role... sgRoles) {
        if (overrideUserSettings == null) {
            overrideUserSettings = new NestedValueMap();
        }
        overrideUserSettings.put(new NestedValueMap.Path(name, "hash"), password.passwordValueForConfiguration());

        if (sgRoles != null && sgRoles.length > 0) {
            String roleNamePrefix = "user_" + name + "__";

            overrideUserSettings.put(new NestedValueMap.Path(name, "search_guard_roles"),
                    asList(sgRoles).stream().map((r) -> roleNamePrefix + r.name).collect(Collectors.toList()));
            roles(roleNamePrefix, sgRoles);
        }

        if (attributes != null && attributes.size() != 0) {
            for (Map.Entry<String, Object> attr : attributes.entrySet()) {
                overrideUserSettings.put(new NestedValueMap.Path(name, "attributes", attr.getKey()), attr.getValue());
            }
        }

        return this;
    }

    public TestSgConfig roles(Role... roles) {
        return roles("", roles);
    }

    public TestSgConfig roles(String roleNamePrefix, Role... roles) {
        if (overrideRoleSettings == null) {
            overrideRoleSettings = new NestedValueMap();
        }

        for (Role role : roles) {
            overrideRoleSettings.putAllFromAnyMap(role.toJsonMap(roleNamePrefix));
        }

        return this;
    }

    public TestSgConfig roleMapping(RoleMapping... roleMappings) {
        if (overrideRoleMappingSettings == null) {
            overrideRoleMappingSettings = new NestedValueMap();
        }

        for (RoleMapping roleMapping : roleMappings) {
            overrideRoleMappingSettings.putAllFromAnyMap(roleMapping.toJsonMap());
        }

        return this;
    }

    public TestSgConfig roleToRoleMapping(Role role, String... backendRoles) {
        return this.roleMapping(new RoleMapping(role.name).backendRoles(backendRoles));
    }

    public TestSgConfig authFailureListener(AuthFailureListener authFailureListener) {
        if (overrideSgConfigSettings == null) {
            overrideSgConfigSettings = new NestedValueMap();
        }

        overrideSgConfigSettings.put(new NestedValueMap.Path("sg_config", "dynamic", "auth_failure_listeners"), authFailureListener.toMap());

        return this;
    }

    public TestSgConfig ignoreUnauthorizedIndices(boolean ignoreUnauthorizedIndices) {
        if (this.privileges == null) {
            this.privileges = new Authz();
        }

        this.privileges.ignoreUnauthorizedIndices(ignoreUnauthorizedIndices);
        return this;
    }

    public TestSgConfig authzDebug(boolean debug) {
        if (this.privileges == null) {
            this.privileges = new Authz();
        }

        this.privileges.debug(debug);
        return this;
    }

    public TestSgConfig sessions(Sessions sessions) {
        this.sessions = sessions;
        return this;
    }

    public TestSgConfig authTokenService(AuthTokenService authTokenService) {
        this.authTokenService = authTokenService;
        return this;
    }

    public TestSgConfig clone() {
        TestSgConfig result = new TestSgConfig();

        result.resourceFolder = resourceFolder;
        result.indexName = indexName;
        result.overrideRoleSettings = overrideRoleSettings != null ? overrideRoleSettings.clone() : null;
        result.overrideSgConfigSettings = overrideSgConfigSettings != null ? overrideSgConfigSettings.clone() : null;
        result.overrideUserSettings = overrideUserSettings != null ? overrideUserSettings.clone() : null;
        result.overrideRoleMappingSettings = overrideRoleMappingSettings != null ? overrideRoleMappingSettings.clone() : null;
        result.overrideFrontendConfigSettings = overrideFrontendConfigSettings != null ? overrideFrontendConfigSettings.clone() : null;
        result.overrideFrontendMultiTenancyConfigSettings = overrideFrontendMultiTenancyConfigSettings != null ? overrideFrontendMultiTenancyConfigSettings.clone() : null;
        result.overrideTenantSettings = overrideTenantSettings != null ? overrideTenantSettings.clone() : null;

        return result;
    }

    public void initIndex(Client client) {
        Map<String, Object> settings = new HashMap<>();
        if (indexName.startsWith(".")) {
            settings.put("index.hidden", true);
        }
        client.admin().indices().create(new CreateIndexRequest(indexName).settings(settings)).actionGet();

        writeOptionalConfigToIndex(client, CType.CONFIG, "sg_config.yml", overrideSgConfigSettings);
        writeOptionalConfigToIndex(client, CType.ROLES, "sg_roles.yml", overrideRoleSettings);
        writeOptionalConfigToIndex(client, CType.INTERNALUSERS, "sg_internal_users.yml", overrideUserSettings);
        writeOptionalConfigToIndex(client, CType.ROLESMAPPING, "sg_roles_mapping.yml", overrideRoleMappingSettings);
        writeConfigToIndex(client, CType.ACTIONGROUPS, "sg_action_groups.yml");
        writeOptionalConfigToIndex(client, CType.TENANTS, "sg_tenants.yml", overrideTenantSettings);
        writeOptionalConfigToIndex(client, CType.BLOCKS, "sg_blocks.yml", null);
        writeOptionalConfigToIndex(client, CType.FRONTEND_AUTHC, "sg_frontend_authc.yml", overrideFrontendConfigSettings);
        writeOptionalConfigToIndex(client, "frontend_multi_tenancy", "sg_frontend_multi_tenancy.yml", overrideFrontendMultiTenancyConfigSettings);

        if (authc != null) {
            writeConfigToIndex(client, CType.AUTHC, authc);
        } else {
            writeOptionalConfigToIndex(client, CType.AUTHC, "sg_authc.yml", null);
        }

        if (privileges != null) {
            writeConfigToIndex(client, CType.AUTHZ, privileges);
        } else {
            writeOptionalConfigToIndex(client, CType.AUTHZ, "sg_privileges.yml", null);
        }

        if (sessions != null) {
            writeConfigToIndex(client, "sessions", sessions);
        }

        if (dlsFls != null) {
            writeConfigToIndex(client, "authz_dlsfls", dlsFls);
        }

        if (authTokenService != null) {
            writeConfigToIndex(client, "auth_token_service", authTokenService);
        }

        if (variableSuppliers.size() != 0) {
            writeConfigVars(client, variableSuppliers);
        }

        ConfigUpdateResponse configUpdateResponse = client
                .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]))).actionGet();

        if (configUpdateResponse.hasFailures()) {
            throw new RuntimeException("ConfigUpdateResponse produced failures: " + configUpdateResponse.failures());
        }
    }

    public void initByConfigRestApi(GenericRestClient client) throws Exception {

        DocNode request = DocNode.EMPTY;

        request = request.with(getConfigDocNode(CType.CONFIG, "sg_config.yml", overrideSgConfigSettings));
        request = request.with(getConfigDocNode(CType.ROLES, "sg_roles.yml", overrideRoleSettings));
        request = request.with(getConfigDocNode(CType.INTERNALUSERS, "sg_internal_users.yml", overrideUserSettings));
        request = request.with(getConfigDocNode(CType.ROLESMAPPING, "sg_roles_mapping.yml", overrideRoleMappingSettings));
        request = request.with(getConfigDocNode(CType.ACTIONGROUPS, "sg_action_groups.yml", null));
        request = request.with(getConfigDocNode(CType.TENANTS, "sg_tenants.yml", null));
        request = request.with(getConfigDocNode(CType.BLOCKS, "sg_blocks.yml", null));
        request = request.with(getConfigDocNode(CType.FRONTEND_AUTHC, "sg_frontend_authc.yml", overrideFrontendConfigSettings));

        request = request.with(ConfigDocument.bulkUpdateMap(authc != null ? authc : Authc.DEFAULT, privileges, sessions, dlsFls, authTokenService));

        if (variableSuppliers.size() != 0) {
            Map<String, Object> values = new HashMap<>();

            for (Map.Entry<String, Supplier<Object>> entry : variableSuppliers.entrySet()) {
                values.put(entry.getKey(), DocNode.of("value", entry.getValue().get()));
            }

            request = request.with("config_vars", DocNode.of("content", values));
        }

        GenericRestClient.HttpResponse response = client.putJson("/_searchguard/config", request);

        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Config update failed: " + response + " (using " + client.getUser() + ")");
        }
    }

    private void writeConfigToIndex(Client client, CType<?> configType, String file) {
        writeConfigToIndex(client, configType, file, (NestedValueMap) null);
    }

    private void writeConfigToIndex(Client client, CType<?> configType, String file, NestedValueMap overrides) {
        try {
            NestedValueMap config;

            if (resourceFolder != null) {
                config = NestedValueMap.fromYaml(openFile(file));
            } else {
                config = NestedValueMap.of(new NestedValueMap.Path("_sg_meta", "type"), configType.toLCString(),
                        new NestedValueMap.Path("_sg_meta", "config_version"), 2);
            }

            if (overrides != null) {
                config.overrideLeafs(overrides);
            }

            log.debug("Writing " + configType + ":\n" + config.toYamlString());

            client.index(new IndexRequest(indexName).id(configType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(configType.toLCString(), BytesReference.fromByteBuffer(ByteBuffer.wrap(config.toJsonString().getBytes("utf-8")))))
                    .actionGet();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        }
    }

    private void writeOptionalConfigToIndex(Client client, CType<?> configType, String file, NestedValueMap overrides) {
        try {
            DocNode config = getMergedConfig(configType, file, overrides);

            log.debug("Writing SearchGuard configuration of type " + configType + " to index:\n" + config.toYamlString());

            client.index(new IndexRequest(indexName).id(configType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(configType.toLCString(), BytesReference.fromByteBuffer(ByteBuffer.wrap(config.toJsonString().getBytes("utf-8")))))
                    .actionGet();
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        }
    }

    private DocNode getMergedConfig(CType<?> configType, String file, NestedValueMap overrides) {
        try {
            DocNode config = null;

            if (resourceFolder != null) {
                try {
                    config = DocNode.parse(Format.YAML).from(openFile(file));
                } catch (FileNotFoundException e) {
                    // ignore
                }
            }

            if (config == null) {
                config = DocNode.EMPTY;
            }

            if (overrides != null) {
                config = new MergePatch(DocNode.wrap(overrides)).apply(config);
            }

            return config;
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        }
    }

    private DocNode getConfigDocNode(CType<?> configType, String file, NestedValueMap overrides) {
        return DocNode.of(configType.getName(), DocNode.of("content", getMergedConfig(configType, file, overrides)));
    }

    private void writeOptionalConfigToIndex(Client client, String configType, String file, NestedValueMap overrides) {
        try {
            NestedValueMap config = null;

            if (resourceFolder != null) {
                try {
                    config = NestedValueMap.fromYaml(openFile(file));
                } catch (FileNotFoundException e) {
                    // ingore
                }
            }

            if (config == null) {
                config = NestedValueMap.of(new NestedValueMap.Path("_sg_meta", "type"), configType,
                        new NestedValueMap.Path("_sg_meta", "config_version"), 2);
            }

            if (overrides != null) {
                config.overrideLeafs(overrides);
            }

            log.info("Writing " + configType + ":\n" + config.toYamlString());

            client.index(new IndexRequest(indexName).id(configType).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(configType,
                    BytesReference.fromByteBuffer(ByteBuffer.wrap(config.toJsonString().getBytes("utf-8"))))).actionGet();
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        }
    }

    private void writeConfigToIndex(Client client, CType<?> configType, Document<?> document) {
        try {
            log.info("Writing " + configType + ":\n" + document.toYamlString());

            client.index(
                    new IndexRequest(indexName).id(configType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(configType.toLCString(),
                            BytesReference.fromByteBuffer(ByteBuffer.wrap(DocNode.of("default", document).toJsonString().getBytes("utf-8")))))
                    .actionGet();
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        }
    }

    private void writeConfigToIndex(Client client, String configType, Document<?> document) {
        try {
            log.info("Writing " + configType + ":\n" + document.toYamlString());

            client.index(new IndexRequest(indexName).id(configType).setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(configType,
                    BytesReference.fromByteBuffer(ByteBuffer.wrap(DocNode.of("default", document).toJsonString().getBytes("utf-8"))))).actionGet();
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        }
    }

    private void writeConfigVars(Client client, Map<String, Supplier<Object>> configVars) {
        BulkRequest bulkRequest = new BulkRequest(".searchguard_config_vars").setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        for (Map.Entry<String, Supplier<Object>> entry : configVars.entrySet()) {
            bulkRequest.add(new IndexRequest(".searchguard_config_vars").id(entry.getKey()).source("value", entry.getValue().get(), "updated",
                    java.time.Instant.now()));
        }

        client.bulk(bulkRequest).actionGet();

        ConfigVarRefreshAction.send(client, new ActionListener<Response>() {

            @Override
            public void onResponse(Response response) {

            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while refreshing secrets");
            }

        });
    }

    private InputStream openFile(String file) throws IOException {

        String path;

        if (resourceFolder == null || resourceFolder.length() == 0 || resourceFolder.equals("/")) {
            path = "/" + file;
        } else {
            path = "/" + resourceFolder + "/" + file;
        }

        InputStream is = FileHelper.class.getResourceAsStream(path);

        if (is == null) {
            throw new FileNotFoundException("Could not find resource in class path: " + path);
        }

        return is;
    }

    public static NestedValueMap fromYaml(String yamlString) {
        try {
            return NestedValueMap.fromYaml(yamlString);
        } catch (IOException | DocumentParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static class UserPassword {

        private final String password;

        private final boolean passwordNeedsToBeHashed;

        private UserPassword(String password, boolean passwordNeedsToBeHashed) {
            this.password = Objects.requireNonNull(password, "Password is required");
            this.passwordNeedsToBeHashed = passwordNeedsToBeHashed;
        }

        public static UserPassword of(String password) {
            return new UserPassword(password, true);
        }

        public static UserPassword fromExpression(String expression) {
            return new UserPassword(expression, false);
        }

        String passwordValueForConfiguration() {
            if (passwordNeedsToBeHashed) {
                return hash(password.toCharArray());
            }
            return password;
        }

        String loginPassword() {
            if (passwordNeedsToBeHashed) {
                return password;
            }
            throw new IllegalStateException("Password expression was used, cannot retrieve password.");
        }
    }

    public static class User implements UserCredentialsHolder {
        private String name;
        private UserPassword password;
        private Role[] roles;
        private String[] roleNames;
        private String description;
        private Map<String, Object> attributes = new HashMap<>();
        private BiFunction<String, List<String>, Matcher<HttpResponse>> restMatcher;
        private Map<String, IndexApiMatchers.IndexMatcher> indexMatchers = new HashMap<>();
        private Map<String, Matcher<HttpResponse>> documentFieldValueMatchers = new LinkedHashMap<>();
        private boolean adminCertUser = false;

        public User(String name) {
            this.name = name;
            this.password = UserPassword.of("secret");
        }

        public User description(String description) {
            this.description = description;
            return this;
        }

        public User password(String password) {
            this.password = UserPassword.of(password);
            return this;
        }

        public User passwordExpression(String passwordExpression) {
            this.password = UserPassword.fromExpression(passwordExpression);
            return this;
        }

        public User roles(Role... roles) {
            this.roles = roles;
            return this;
        }

        public User roles(String... roles) {
            this.roleNames = roles;
            return this;
        }

        public User attr(String key, Object value) {
            this.attributes.put(key, value);
            return this;
        }

        public User restMatcher(BiFunction<String, List<String>, Matcher<HttpResponse>> restMatcher) {
            this.restMatcher = restMatcher;
            return this;
        }

        public Matcher<HttpResponse> restMatcher(String jsonPath, String... params) {
            return this.restMatcher.apply(jsonPath, ImmutableList.ofArray(params));
        }

        public User indexMatcher(String key, IndexApiMatchers.IndexMatcher indexMatcher) {
            this.indexMatchers.put(key, indexMatcher);
            return this;
        }

        public IndexApiMatchers.IndexMatcher indexMatcher(String key) {
            IndexApiMatchers.IndexMatcher result = this.indexMatchers.get(key);

            if (result != null) {
                return result;
            } else {
                throw new RuntimeException("Unknown index matcher " + key + " in user " + this.name);
            }
        }

        public String getName() {
            return name;
        }

        public String getPassword() {
            return password.loginPassword();
        }

        public Set<String> getRoleNames() {
            ImmutableSet<String> result = ImmutableSet.empty();

            if (roleNames != null) {
                result = result.with(roleNames);
            }

            if (roles != null) {
                result = result.with(ImmutableSet.ofArray(roles).map((r) -> r.name));
            }

            return result;
        }

        public String getDescription() {
            return description;
        }

        public boolean isAdminCertUser() {
            return adminCertUser;
        }

        public User adminCertUser() {
            this.adminCertUser = true;
            return this;
        }

        public User addFieldValueMatcher(String fieldJsonPath, boolean multipleDocuments, Matcher<?>...matchers) {
            Objects.requireNonNull(fieldJsonPath, "Document field JSON patch name must not be null");
            if(documentFieldValueMatchers.containsKey(fieldJsonPath)) {
                throw new IllegalArgumentException("Field matcher for " + fieldJsonPath + " already exists");
            }
            if(matchers == null || matchers.length == 0) {
                throw new IllegalArgumentException("At least one pattern must be provided");
            }
            Matcher<? super Iterable<? extends String>>[] everyFieldValueMatchers = Arrays.stream(matchers) //
                .map(m -> multipleDocuments ? everyItem(m) : m) //
                .toArray(Matcher[]::new);
            Matcher<GenericRestClient.HttpResponse> fieldValueMatcher = json(distinctNodesAt(fieldJsonPath, allOf(not(emptyIterable()), allOf(everyFieldValueMatchers))));
            documentFieldValueMatchers.put(fieldJsonPath, fieldValueMatcher);
            return this;
        }

        public Matcher<HttpResponse> matcherForField(String fieldJsonPath) {
            Objects.requireNonNull(fieldJsonPath, "Field JSON path must not be null");
            Matcher<HttpResponse> matcher = documentFieldValueMatchers.get(fieldJsonPath);
            return Objects.requireNonNull(matcher, "No field matcher found for " + fieldJsonPath);
        }

        @Override
        public String toString() {
            return "User name '" + name + '\'';
        }
    }

    public static class TenantPermission implements Document<TenantPermission> {
        private List<String> tenantPatterns;//tenant_patterns
        private final List<String> allowedActions; //allowed_actions
        private Role role;

        TenantPermission(Role role, String... allowedActions) {
            this.tenantPatterns = ImmutableList.empty();
            this.allowedActions = asList(allowedActions);
            this.role = role;
        }



        public TenantPermission(List<String> tenantPatterns, List<String> allowedActions) {
            this.tenantPatterns = Objects.requireNonNull(tenantPatterns, "Tenant patterns must not be null");
            this.allowedActions = Objects.requireNonNull(allowedActions, "Tenant allowed actions must not be null");
        }

        public Role on(String... tenantPatterns) {
            this.tenantPatterns = asList(tenantPatterns);
            this.role.tenantPermissions.add(this);
            return this.role;
        }

        NestedValueMap asNestedValueMap() {
            return NestedValueMap.of("tenant_patterns", tenantPatterns, "allowed_actions", allowedActions);
        }

        @Override
        public Object toBasicObject() {
            return DocNode.of("tenant_patterns", tenantPatterns, "allowed_actions", allowedActions);
        }
    }

    public static class Role implements Document<Role> {
        public static Role ALL_ACCESS = new Role("all_access").clusterPermissions("*").indexPermissions("*").on("*");

        private String name;
        private List<String> clusterPermissions = new ArrayList<>();
        private List<String> excludedClusterPermissions = new ArrayList<>();

        private List<IndexLikePermission> indexPermissions = new ArrayList<>();
        private List<IndexLikePermission> aliasPermissions = new ArrayList<>();
        private List<IndexLikePermission> dataStreamPermissions = new ArrayList<>();

        private List<TenantPermission> tenantPermissions = new ArrayList<>();

        public Role(String name) {
            this.name = name;
        }

        public Role tenantPermission(Collection<String> tenantPatterns, Collection<String> allowedActions) {
            tenantPermissions.add(new TenantPermission(new ArrayList<>(tenantPatterns), new ArrayList<>(allowedActions)));
            return this;
        }

        public Role tenantPermission(String tenantPattern, String... allowedActions) {
            return tenantPermission(Collections.singletonList(tenantPattern), Arrays.asList(allowedActions));
        }

        public Role clusterPermissions(String... clusterPermissions) {
            this.clusterPermissions.addAll(asList(clusterPermissions));
            return this;
        }

        public Role excludeClusterPermissions(String... clusterPermissions) {
            this.excludedClusterPermissions.addAll(asList(clusterPermissions));
            return this;
        }

        public IndexLikePermission indexPermissions(String... indexPermissions) {
            return new IndexLikePermission(this, this.indexPermissions, "index_patterns", indexPermissions);
        }

        public IndexLikePermission aliasPermissions(String... aliasPermissions) {
            return new IndexLikePermission(this, this.aliasPermissions, "alias_patterns", aliasPermissions);
        }

        public IndexLikePermission dataStreamPermissions(String... dataStreamPermissions) {
            return new IndexLikePermission(this, this.dataStreamPermissions, "data_stream_patterns", dataStreamPermissions);
        }

        public TenantPermission withTenantPermission(String... tenantPermissions) {
            return new TenantPermission(this, tenantPermissions);
        }

        public String getName() {
            return name;
        }

        public NestedValueMap toJsonMap() {
            return toJsonMap("");
        }

        public NestedValueMap toJsonMap(String roleNamePrefix) {
            NestedValueMap map = new NestedValueMap();
            String name = roleNamePrefix + this.name;

            if (!this.clusterPermissions.isEmpty()) {
                map.put(new NestedValueMap.Path(name, "cluster_permissions"), this.clusterPermissions);
            }

            if (!this.indexPermissions.isEmpty()) {
                map.put(new NestedValueMap.Path(name, "index_permissions"),
                    this.indexPermissions.stream().map((p) -> p.toJsonMap()).collect(Collectors.toList()));
            }

            if (this.aliasPermissions.size() > 0) {
                map.put(new NestedValueMap.Path(name, "alias_permissions"),
                        this.aliasPermissions.stream().map((p) -> p.toJsonMap()).collect(Collectors.toList()));
            }

            if (this.dataStreamPermissions.size() > 0) {
                map.put(new NestedValueMap.Path(name, "data_stream_permissions"),
                        this.dataStreamPermissions.stream().map((p) -> p.toJsonMap()).collect(Collectors.toList()));
            }

            if (this.excludedClusterPermissions.size() > 0) {
                map.put(new NestedValueMap.Path(name, "exclude_cluster_permissions"), this.excludedClusterPermissions);
            }

            if (this.tenantPermissions.size() > 0) {
                map.put(new NestedValueMap.Path(name, "tenant_permissions"), this.tenantPermissions.stream()
                    .map(TenantPermission::asNestedValueMap)
                    .collect(Collectors.toList()));
            }
            return map;
        }

        public com.floragunn.searchguard.authz.config.Role toActualRole() throws ConfigValidationException {
            return toActualRole(new ConfigurationRepository.Context(null, null, null, null, null));
        }

        public com.floragunn.searchguard.authz.config.Role toActualRole(ConfigurationRepository.Context parserContext) throws ConfigValidationException {
            return com.floragunn.searchguard.authz.config.Role.parse(DocNode.wrap(this.toDeepBasicObject()), parserContext).get();
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap
                    .ofNonNull("cluster_permissions", this.clusterPermissions, "index_permissions", this.indexPermissions, "alias_permissions",
                            this.aliasPermissions, "data_stream_permissions", this.dataStreamPermissions)
                    .with(ImmutableMap.ofNonNull("tenant_permissions", this.tenantPermissions, "exclude_cluster_permissions",
                            this.excludedClusterPermissions));
        }

        public static SgDynamicConfiguration<com.floragunn.searchguard.authz.config.Role> toActualRole(ConfigurationRepository.Context parserContext,
                TestSgConfig.Role... roles) throws ConfigValidationException {
            Map<String, com.floragunn.searchguard.authz.config.Role> parsedRoles = new HashMap<>();

            for (TestSgConfig.Role role : roles) {
                parsedRoles.put(role.getName(),
                        com.floragunn.searchguard.authz.config.Role.parse(DocNode.wrap(role.toDeepBasicObject()), parserContext).get());
            }

            return SgDynamicConfiguration.of(CType.ROLES, parsedRoles);
        }

        public static SgDynamicConfiguration<com.floragunn.searchguard.authz.config.Role> toActualRole(TestSgConfig.Role... roles)
                throws ConfigValidationException {
            return toActualRole(new ConfigurationRepository.Context(null, null, null, null, null), roles);
        }

    }

    public static class RoleMapping {
        private String name;
        private List<String> backendRoles = new ArrayList<>();
        private List<String> users = new ArrayList<>();

        private List<String> hosts = new ArrayList<>();

        private List<String> ips = new ArrayList<>();

        public RoleMapping(String name) {
            this.name = name;
        }

        public RoleMapping backendRoles(String... backendRoles) {
            this.backendRoles.addAll(asList(backendRoles));
            return this;
        }

        public RoleMapping users(String... users) {
            this.users.addAll(asList(users));
            return this;
        }

        public RoleMapping hosts(String... hosts) {
            this.hosts.addAll(asList(hosts));
            return this;
        }

        public RoleMapping ips(String... ips) {
            this.ips.addAll(asList(ips));
            return this;
        }

        public String getName() {
            return name;
        }

        public NestedValueMap toJsonMap() {
            NestedValueMap map = new NestedValueMap();

            String name = this.name;

            if (this.backendRoles.size() > 0) {
                map.put(new NestedValueMap.Path(name, "backend_roles"), this.backendRoles);
            }

            if (this.users.size() > 0) {
                map.put(new NestedValueMap.Path(name, "users"), this.users);
            }

            if (this.hosts.size() > 0) {
                map.put(new NestedValueMap.Path(name, "hosts"), this.hosts);
            }

            if (this.ips.size() > 0) {
                map.put(new NestedValueMap.Path(name, "ips"), this.ips);
            }
            return map;
        }
    }

    public static class IndexLikePermission implements Document<IndexLikePermission> {
        private List<String> allowedActions;
        private List<String> indexPatterns;
        private Role role;
        private String dlsQuery;
        private List<String> fls;
        private List<String> maskedFields;
        private List<IndexLikePermission> targetList;
        private String patternAttributeName;

        IndexLikePermission(Role role, List<IndexLikePermission> targetList, String patternAttributeName, String... allowedActions) {
            this.allowedActions = asList(allowedActions);
            this.role = role;
            this.targetList = targetList;
            this.patternAttributeName = patternAttributeName;
        }

        public IndexLikePermission dls(String dlsQuery) {
            this.dlsQuery = dlsQuery;
            return this;
        }

        public IndexLikePermission dls(Map<String, Object> dlsQuery) {
            this.dlsQuery = DocWriter.json().writeAsString(dlsQuery);
            return this;
        }

        public IndexLikePermission fls(String... fls) {
            this.fls = asList(fls);
            return this;
        }

        public IndexLikePermission maskedFields(String... maskedFields) {
            this.maskedFields = asList(maskedFields);
            return this;
        }

        public Role on(String... indexPatterns) {
            this.indexPatterns = asList(indexPatterns);
            this.targetList.add(this);
            return this.role;
        }

        public NestedValueMap toJsonMap() {
            NestedValueMap result = new NestedValueMap();

            result.put(patternAttributeName, indexPatterns);
            result.put("allowed_actions", allowedActions);

            if (dlsQuery != null) {
                result.put("dls", dlsQuery);
            }

            if (fls != null) {
                result.put("fls", fls);
            }

            if (maskedFields != null) {
                result.put("masked_fields", maskedFields);
            }

            return result;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull(patternAttributeName, indexPatterns, "allowed_actions", allowedActions, "dls", dlsQuery, "fls", fls,
                    "masked_fields", maskedFields);
        }

    }

    public static class Tenant {
        private String name;
        private boolean reserved;
        private boolean hidden;
        private boolean isStatic;
        private String description;

        public Tenant(String name) {
            this.name = Objects.requireNonNull(name, "Name is required");
        }


        public Tenant reserved(boolean reserved) {
            this.reserved = reserved;
            return this;
        }

        public Tenant hidden(boolean hidden) {
            this.hidden = hidden;
            return this;
        }

        public Tenant isStatic(boolean isStatic) {
            this.isStatic = isStatic;
            return this;
        }

        public Tenant description(String description) {
            this.description = description;
            return this;
        }

        public String getName() {
            return name;
        }

        public NestedValueMap toJsonMap() {
            NestedValueMap map = new NestedValueMap();

            map.put(new NestedValueMap.Path( this.name, "reserved"), this.reserved);
            map.put(new NestedValueMap.Path( this.name, "hidden"), this.hidden);
            map.put(new NestedValueMap.Path( this.name, "static"), this.isStatic);
            map.put(new NestedValueMap.Path( this.name, "description"), this.description);

            return map;
        }
    }

    public static class Authc extends ConfigDocument<Authc> {

        public static final Authc DEFAULT = new Authc(new Authc.Domain("basic/internal_users_db"));

        private List<Domain> domains;
        private List<String> trustedProxies;
        private boolean debug;
        private boolean userCacheEnabled = true;

        public Authc(Domain... domains) {
            this.domains = ImmutableList.ofArray(domains);
        }

        public Authc trustedProxies(String... trustedProxies) {
            this.trustedProxies = asList(trustedProxies);
            return this;
        }

        public Authc debug() {
            this.debug = true;
            return this;
        }

        public Authc userCacheEnabled(boolean userCacheEnabled) {
            this.userCacheEnabled = userCacheEnabled;
            return this;
        }

        public static class Domain implements Document<Domain> {

            private final String type;
            private String id;
            private String description;
            private List<String> acceptIps = null;
            private List<String> acceptOriginatingIps = null;
            private List<String> skipIps = null;
            private List<String> skipOriginatingIps = null;
            private List<String> acceptUsers = null;
            private List<String> skipUsers = null;
            private List<AdditionalUserInformation> additionalUserInformation = null;
            private UserMapping userMapping;
            private DocNode backendConfig;
            private DocNode frontendConfig;

            private JwtDomain jwt;

            public Domain jwt(JwtDomain jwt) {
                this.jwt = jwt;
                return this;
            }

            public Domain(String type) {
                this.type = type;
            }

            public Domain id(String id) {
                this.id = id;
                return this;
            }

            public Domain description(String description) {
                this.description = description;
                return this;
            }

            public Domain frontend(DocNode frontendConfig) {
                this.frontendConfig = frontendConfig;
                return this;
            }

            public Domain backend(DocNode backendConfig) {
                this.backendConfig = backendConfig;
                return this;
            }

            public Domain userMapping(UserMapping userMapping) {
                this.userMapping = userMapping;
                return this;
            }

            public Domain additionalUserInformation(AdditionalUserInformation... additionalUserInformation) {
                if (this.additionalUserInformation == null) {
                    this.additionalUserInformation = new ArrayList<>(asList(additionalUserInformation));
                } else {
                    this.additionalUserInformation.addAll(asList(additionalUserInformation));
                }
                return this;
            }

            public Domain acceptIps(String... ips) {
                if (acceptIps == null) {
                    acceptIps = new ArrayList<>(asList(ips));
                } else {
                    acceptIps.addAll(asList(ips));
                }
                return this;
            }

            public Domain acceptOriginatingIps(String... ips) {
                if (acceptOriginatingIps == null) {
                    acceptOriginatingIps = new ArrayList<>(asList(ips));
                } else {
                    acceptOriginatingIps.addAll(asList(ips));
                }
                return this;
            }

            public Domain skipIps(String... ips) {
                if (skipIps == null) {
                    skipIps = new ArrayList<>(asList(ips));
                } else {
                    skipIps.addAll(asList(ips));
                }
                return this;
            }

            public Domain skipOriginatingIps(String... ips) {
                if (skipOriginatingIps == null) {
                    skipOriginatingIps = new ArrayList<>(asList(ips));
                } else {
                    skipOriginatingIps.addAll(asList(ips));
                }
                return this;
            }

            public Domain skipUsers(String... users) {
                skipUsers = asList(users);
                return this;
            }

            public Domain acceptUsers(String... users) {
                acceptUsers = asList(users);
                return this;
            }

            @Override
            public Object toBasicObject() {
                Map<String, Object> result = new LinkedHashMap<>();

                result.put("type", type);

                if (id != null) {
                    result.put("id", id);
                }

                if (description != null) {
                    result.put("description", description);
                }

                if (frontendConfig != null) {
                    int slash = type.indexOf('/');
                    result.put(type.substring(0, slash != -1 ? slash : type.length()), frontendConfig);
                }

                if (backendConfig != null) {
                    result.put(type.substring(type.indexOf('/') + 1), backendConfig);
                }

                if (acceptIps != null || acceptUsers != null || acceptOriginatingIps != null) {
                    result.put("accept", ImmutableMap.ofNonNull("ips", acceptIps, "users", acceptUsers, "originating_ips", acceptOriginatingIps));
                }

                if (skipIps != null || skipUsers != null || skipOriginatingIps != null) {
                    result.put("skip", ImmutableMap.ofNonNull("ips", skipIps, "users", skipUsers, "originating_ips", skipOriginatingIps));
                }

                if (additionalUserInformation != null) {
                    result.put("additional_user_information", additionalUserInformation);
                }

                if (userMapping != null) {
                    result.put("user_mapping", userMapping.toBasicObject());
                }

                if (jwt != null) {
                    result.put("jwt", jwt.toBasicObject());
                }

                return result;
            }

            public static class AdditionalUserInformation implements Document<AdditionalUserInformation> {
                private String type;
                private DocNode config;

                public AdditionalUserInformation(String type) {
                    this.type = type;
                    this.config = null;
                }

                public AdditionalUserInformation(String type, DocNode config) {
                    this.type = type;
                    this.config = config;
                }

                @Override
                public Object toBasicObject() {
                    return ImmutableMap.ofNonNull("type", type, type, config);
                }

            }

            public static class UserMapping implements Document<UserMapping> {
                private List<DocNode> userNameFrom = new ArrayList<>();
                private List<String> userNameStatic = new ArrayList<>();
                private List<DocNode> userNameFromBackend = new ArrayList<>();
                private List<DocNode> rolesFrom = new ArrayList<>();
                private List<DocNode> rolesFromCommaSeparatedString = new ArrayList<>();
                private List<String> rolesStatic = new ArrayList<>();
                private Map<String, String> attrsFrom = new HashMap<>();
                private Map<String, String> attrsStatic = new HashMap<>();

                public UserMapping userNameFrom(String sourcePath) {
                    userNameFrom.add(DocNode.wrap(sourcePath));
                    return this;
                }

                public UserMapping userNameFrom(DocNode docNode) {
                    userNameFrom.add(docNode);
                    return this;
                }

                public UserMapping userNameStatic(String userName) {
                    userNameStatic.add(userName);
                    return this;
                }

                public UserMapping userNameFromBackend(String sourcePath) {
                    userNameFromBackend.add(DocNode.wrap(sourcePath));
                    return this;
                }

                public UserMapping userNameFromBackend(DocNode docNode) {
                    userNameFromBackend.add(docNode);
                    return this;
                }

                public UserMapping rolesFrom(String sourcePath) {
                    rolesFrom.add(DocNode.wrap(sourcePath));
                    return this;
                }

                public UserMapping rolesFromCommaSeparatedString(String sourcePath) {
                    rolesFromCommaSeparatedString.add(DocNode.wrap(sourcePath));
                    return this;
                }

                public UserMapping rolesFrom(DocNode docNode) {
                    rolesFrom.add(docNode);
                    return this;
                }

                public UserMapping rolesStatic(String... roles) {
                    rolesStatic.addAll(asList(roles));
                    return this;
                }

                public UserMapping attrsFrom(String target, String sourcePath) {
                    this.attrsFrom.put(target, sourcePath);
                    return this;
                }

                public UserMapping attrsStatic(String target, String value) {
                    this.attrsStatic.put(target, value);
                    return this;
                }

                @Override
                public Object toBasicObject() {
                    Map<String, Object> result = new LinkedHashMap<>();

                    if (userNameFrom.size() != 0 || userNameStatic.size() != 0 || userNameFromBackend.size() != 0) {
                        Map<String, Object> userName = new LinkedHashMap<>();

                        if (userNameFrom.size() == 1) {
                            userName.put("from", userNameFrom.get(0));
                        } else if (userNameFrom.size() > 1) {
                            userName.put("from", userNameFrom);
                        }

                        if (userNameStatic.size() == 1) {
                            userName.put("static", userNameStatic.get(0));
                        } else if (userNameStatic.size() > 1) {
                            userName.put("static", userNameStatic);
                        }

                        if (userNameFromBackend.size() == 1) {
                            userName.put("from_backend", userNameFromBackend.get(0));
                        } else if (userNameFromBackend.size() > 1) {
                            userName.put("from_backend", userNameFromBackend);
                        }

                        result.put("user_name", userName);
                    }

                    if (rolesFrom.size() != 0 || rolesStatic.size() != 0 || rolesFromCommaSeparatedString.size() != 0) {
                        Map<String, Object> roles = new LinkedHashMap<>();

                        if (rolesFrom.size() == 1) {
                            roles.put("from", rolesFrom.get(0));
                        } else if (rolesFrom.size() > 1) {
                            roles.put("from", rolesFrom);
                        }

                        if (rolesFromCommaSeparatedString.size() == 1) {
                            roles.put("from_comma_separated_string", rolesFromCommaSeparatedString.get(0));
                        } else if (rolesFromCommaSeparatedString.size() > 1) {
                            roles.put("from_comma_separated_string", rolesFromCommaSeparatedString);
                        }

                        if (rolesStatic.size() == 1) {
                            roles.put("static", rolesStatic.get(0));
                        } else if (rolesStatic.size() > 1) {
                            roles.put("static", rolesStatic);
                        }

                        result.put("roles", roles);
                    }

                    if (attrsFrom.size() != 0 || attrsStatic.size() != 0) {
                        Map<String, Object> attrs = new LinkedHashMap<>();

                        if (attrsFrom.size() != 0) {
                            attrs.put("from", attrsFrom);
                        }

                        if (attrsStatic.size() != 0) {
                            attrs.put("static", attrsStatic);
                        }

                        result.put("attrs", attrs);
                    }

                    return result;
                }

            }
        }

        @Override
        public Object toBasicObject() {
            Map<String, Object> result = new LinkedHashMap<>();

            result.put("auth_domains", domains);

            if (trustedProxies != null) {
                result.put("network", ImmutableMap.of("trusted_proxies", trustedProxies));
            }

            result.put("debug", this.debug);

            if (!userCacheEnabled) {
                result.put("user_cache", ImmutableMap.of("enabled", false));
            }

            return ImmutableMap.of(result);
        }

        @Override
        public String configType() {
            return "authc";
        }
    }

    public static class FrontendAuthc {

        private List<FrontendAuthDomain> authDomains = new ArrayList<>();
        private FrontendLoginPage loginPage;

        public FrontendAuthc() {
        }

        public FrontendAuthc authDomain(FrontendAuthDomain authDomain) {
            this.authDomains.add(authDomain);
            return this;
        }

        public FrontendAuthc loginPage(FrontendLoginPage loginPage) {
            this.loginPage = loginPage;
            return this;
        }

        NestedValueMap toMap() {
            NestedValueMap result = new NestedValueMap();
            result.put("auth_domains", authDomains.stream()
                    .map(FrontendAuthDomain::toMap)
                    .collect(Collectors.toList()));
            if(loginPage != null) {
                result.put("login_page", loginPage.toMap());
            }
            return result;
        }
    }

    public static class FrontendAuthDomain {
        private final String type;
        private String label;
        private NestedValueMap moreProperties = new NestedValueMap();

        public FrontendAuthDomain(String type) {
            this.type = type;
        }

        public FrontendAuthDomain label(String label) {
            this.label = label;
            return this;
        }

        public FrontendAuthDomain config(String key, Object value) {
            this.moreProperties.put(Path.parse(key), value);
            return this;
        }

        public FrontendAuthDomain config(String key, Object value, Object... kvPairs) {
            this.moreProperties.put(Path.parse(key), value);

            if (kvPairs != null && kvPairs.length >= 2) {
                for (int i = 0; i < kvPairs.length; i += 2) {
                    this.moreProperties.put(Path.parse(String.valueOf(kvPairs[i])), kvPairs[i + 1]);
                }
            }

            return this;
        }

        NestedValueMap toMap() {
            NestedValueMap result = this.moreProperties.clone();
            result.put("type", type);
            result.put("label", label);
            return result;
        }
    }

    public static class FrontendLoginPage {

        private String brandImage;

        public FrontendLoginPage brandImage(String brandImage) {
            this.brandImage = brandImage;
            return this;
        }

        NestedValueMap toMap() {
            NestedValueMap result = new NestedValueMap();
            if (brandImage != null) {
                result.put("brand_image", brandImage);
            }
            return result;
        }
    }

    public static class FrontendMultiTenancy {
        private boolean enabled;
        private String index;
        private String serverUser;
        private boolean globalTenantEnabled = true;
        private List<String> preferredTenants = new ArrayList<>();

        public FrontendMultiTenancy(boolean enabled) {
            this.enabled = enabled;
        }

        public FrontendMultiTenancy index(String index) {
            this.index = index;
            return this;
        }

        public FrontendMultiTenancy serverUser(String serverUser) {
            this.serverUser = serverUser;
            return this;
        }

        public FrontendMultiTenancy globalTenantEnabled(boolean globalTenantEnabled) {
            this.globalTenantEnabled = globalTenantEnabled;
            return this;
        }

        public FrontendMultiTenancy preferredTenants(String... preferredTenants) {
            this.preferredTenants.addAll(asList(preferredTenants));
            return this;
        }

        NestedValueMap toJsonMap() {
            NestedValueMap result = new NestedValueMap();
            result.put("enabled", enabled);
            result.put("index", index);
            result.put("server_user", serverUser);
            result.put("global_tenant_enabled", globalTenantEnabled);
            result.put("preferred_tenants", preferredTenants);
            return NestedValueMap.of("default", result);
        }
    }

    public static class DlsFls extends ConfigDocument<DlsFls> {

        private Boolean debug;
        private String metrics;
        private Boolean dlsAllowNow;
        private String mode;
        private String fieldAnonymizationPrefix;

        public DlsFls() {
        }

        public DlsFls metrics(String metrics) {
            this.metrics = metrics;
            return this;
        }

        public DlsFls mode(String mode) {
            this.mode = mode;
            return this;
        }

        public DlsFls fieldAnonymizationPrefix(String prefix) {
            this.fieldAnonymizationPrefix = prefix;
            return this;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull("debug", debug, "metrics", metrics, "dls",
                    ImmutableMap.ofNonNull("allow_now", dlsAllowNow, "mode", mode),
                    "field_anonymization", ImmutableMap.ofNonNull("prefix", fieldAnonymizationPrefix));
        }

        @Override
        public String configType() {
            return "authz_dlsfls";
        }
    }

    public static class JwtDomain implements Document<JsonWebKey> {
        private Signing signing;

        public JwtDomain signing(Signing signing) {
            this.signing = signing;
            return this;
        }

        @Override
        public Object toBasicObject() {
            Map<String, Object> result = new LinkedHashMap<>();
            if (signing != null) {
                result.put("signing", signing.toBasicObject());
            }
            return result;
        }
    }

    public static class Signing implements Document<JsonWebKey> {
        private Jwks jwks;

        public Signing jwks(Jwks jwks) {
            this.jwks = jwks;
            return this;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("jwks", jwks.toBasicObject());
        }
    }

    public static class Jwks implements Document<JsonWebKey> {
        private List<JsonWebKey> keys = new ArrayList<>();

        public Jwks keys(List<JsonWebKey> keys) {
            this.keys = keys;
            return this;
        }

        public Jwks addKey(JsonWebKey jsonWebKey) {
            Objects.requireNonNull(jsonWebKey, "Json web key is required");
            this.keys.add(jsonWebKey);
            return this;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("keys", keys);
        }
    }

    public static class JsonWebKey implements Document<JsonWebKey> {

        private String kty;
        private String kid;
        private String d;
        private String use;
        private String crv;
        private String x;
        private String y;
        private String alg;
        private String k;

        public JsonWebKey kty(String kty) {
            this.kty = kty;
            return this;
        }

        public JsonWebKey kid(String kid) {
            this.kid = kid;
            return this;
        }

        public JsonWebKey d(String d) {
            this.d = d;
            return this;
        }

        public JsonWebKey use(String use) {
            this.use = use;
            return this;
        }

        public JsonWebKey crv(String crv) {
            this.crv = crv;
            return this;
        }

        public JsonWebKey x(String x) {
            this.x = x;
            return this;
        }

        public JsonWebKey y(String y) {
            this.y = y;
            return this;
        }

        public JsonWebKey alg(String alg) {
            this.alg = alg;
            return this;
        }

        public JsonWebKey k(String k) {
            this.k = k;
            return this;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull("kty", kty, "d", d, "use", use, "crv", crv, "x", x)
                .with(ImmutableMap.ofNonNull("y", y, "alg", alg)).with(ImmutableMap.of("kid", kid, "k", k));
        }
    }

    public static class AuthTokenService extends ConfigDocument<AuthTokenService> {

        private Boolean enabled;
        private String metrics;
        private String jwtSigningKeyHs512;

        public AuthTokenService() {
        }

        public AuthTokenService enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public AuthTokenService metrics(String metrics) {
            this.metrics = metrics;
            return this;
        }

        public AuthTokenService jwtSigningKeyHs512(String jwtSigningKeyHs512) {
            this.jwtSigningKeyHs512 = jwtSigningKeyHs512;
            return this;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull("enabled", enabled, "metrics", metrics, "jwt_signing_key_hs512", jwtSigningKeyHs512);
        }

        @Override
        public String configType() {
            return "auth_token_service";
        }
    }

    public static class AuthFailureListener {
        private final String id;
        private final String type;
        private int allowedTries;
        private int timeWindowSeconds = 3600;
        private int blockExpirySeconds = 600;

        public AuthFailureListener(String id, String type) {
            this.id = id;
            this.type = type;
            this.allowedTries = 3;
        }

        public AuthFailureListener(String id, String type, int allowedTries) {
            this.id = id;
            this.type = type;
            this.allowedTries = allowedTries;
        }

        NestedValueMap toMap() {
            NestedValueMap result = new NestedValueMap();
            result.put("type", type);
            result.put("allowed_tries", allowedTries);
            result.put("time_window_seconds", timeWindowSeconds);
            result.put("block_expiry_seconds", blockExpirySeconds);

            return NestedValueMap.of(id, result);
        }
    }

    public static class Authz extends ConfigDocument<Authz> {
        private boolean ignoreUnauthorizedIndices = true;
        private boolean debug = false;

        public Authz() {

        }

        public boolean isIgnoreUnauthorizedIndices() {
            return ignoreUnauthorizedIndices;
        }

        public Authz ignoreUnauthorizedIndices(boolean ignoreUnauthorizedIndices) {
            this.ignoreUnauthorizedIndices = ignoreUnauthorizedIndices;
            return this;
        }

        public Authz debug(boolean debug) {
            this.debug = debug;
            return this;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("ignore_unauthorized_indices.enabled", ignoreUnauthorizedIndices, "debug", debug);
        }

        @Override
        public String configType() {
            return "authz";
        }
    }

    public static class Sessions extends ConfigDocument<Sessions> {

        private Duration inactivityTimeout;
        private Boolean refreshSessionActivityIndex;
        private String jwtSigningKeyHs512;
        private String jwtAudience;

        public Sessions inactivityTimeout(Duration inactivityTimeout) {
            this.inactivityTimeout = inactivityTimeout;
            return this;
        }

        public Sessions refreshSessionActivityIndex(boolean refreshSessionActivityIndex) {
            this.refreshSessionActivityIndex = refreshSessionActivityIndex;
            return this;
        }

        public Sessions jwtSigningKeyHs512(String jwtSigningKeyHs512) {
            this.jwtSigningKeyHs512 = jwtSigningKeyHs512;
            return this;
        }

        public Sessions jwtAudience(String jwtAudience) {
            this.jwtAudience = jwtAudience;
            return this;
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.ofNonNull("inactivity_timeout", inactivityTimeout != null ? DurationFormat.INSTANCE.format(inactivityTimeout) : null,
                    "refresh_session_activity_index", refreshSessionActivityIndex, "jwt_signing_key_hs512", jwtSigningKeyHs512, "jwt_audience",
                    jwtAudience);
        }

        @Override
        public String configType() {
            return "sessions";
        }

    }

    public static abstract class ConfigDocument<C> implements Document<C> {
        public abstract String configType();

        public static DocNode bulkUpdateMap(ConfigDocument<?> ...configDocuments) {
            DocNode result = DocNode.EMPTY;

            for (ConfigDocument<?> configDocument : configDocuments) {
                if (configDocument != null) {
                    result = result.with(configDocument.configType(), DocNode.of("content", configDocument.toDeepBasicObject()));
                }
            }

            return result;
        }
    }

    private static String hash(final char[] clearTextPassword) {
        final byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        final String hash = OpenBSDBCrypt.generate((Objects.requireNonNull(clearTextPassword)), salt, 12);
        Arrays.fill(salt, (byte) 0);
        Arrays.fill(clearTextPassword, '\0');
        return hash;
    }
}
