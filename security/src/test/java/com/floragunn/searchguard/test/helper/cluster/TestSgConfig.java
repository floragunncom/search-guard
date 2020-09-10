package com.floragunn.searchguard.test.helper.cluster;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.tools.Hasher;

public class TestSgConfig {

    private String resourceFolder;
    private NestedValueMap overrideSgConfigSettings;
    private NestedValueMap overrideUserSettings;
    private NestedValueMap overrideRoleSettings;
    private String indexName = "searchguard";

    public TestSgConfig resources(String resourceFolder) {
        this.resourceFolder = resourceFolder;
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

    public TestSgConfig user(String name, String password, String... sgRoles) {
        if (overrideUserSettings == null) {
            overrideUserSettings = new NestedValueMap();
        }

        overrideUserSettings.put(new NestedValueMap.Path(name, "hash"), Hasher.hash(password.toCharArray()));

        if (sgRoles != null && sgRoles.length > 0) {
            overrideUserSettings.put(new NestedValueMap.Path(name, "search_guard_roles"), sgRoles);
        }

        return this;
    }

    public TestSgConfig user(String name, String password, Role... sgRoles) {
        if (overrideUserSettings == null) {
            overrideUserSettings = new NestedValueMap();
        }

        overrideUserSettings.put(new NestedValueMap.Path(name, "hash"), Hasher.hash(password.toCharArray()));

        if (sgRoles != null && sgRoles.length > 0) {
            overrideUserSettings.put(new NestedValueMap.Path(name, "search_guard_roles"),
                    Arrays.asList(sgRoles).stream().map((r) -> r.name).collect(Collectors.toList()));
            roles(sgRoles);

        }


        return this;
    }

    public TestSgConfig roles(Role... roles) {
        if (overrideRoleSettings == null) {
            overrideRoleSettings = new NestedValueMap();
        }

        for (Role role : roles) {

            if (role.clusterPermissions.size() > 0) {
                overrideRoleSettings.put(new NestedValueMap.Path(role.name, "cluster_permissions"), role.clusterPermissions);
            }

            if (role.indexPermissions.size() > 0) {
                overrideRoleSettings.put(new NestedValueMap.Path(role.name, "index_permissions"),
                        role.indexPermissions.stream()
                                .map((p) -> NestedValueMap.of("index_patterns", p.indexPatterns, "allowed_actions", p.allowedActions))
                                .collect(Collectors.toList()));
            }
        }

        return this;
    }

    void initIndex(Client client) {
        client.admin().indices().create(new CreateIndexRequest("searchguard")).actionGet();

        writeConfigToIndex(client, CType.CONFIG, "sg_config.yml", overrideSgConfigSettings);
        System.out.println(overrideRoleSettings);
        writeConfigToIndex(client, CType.ROLES, "sg_roles.yml", overrideRoleSettings);
        writeConfigToIndex(client, CType.INTERNALUSERS, "sg_internal_users.yml", overrideUserSettings);
        writeConfigToIndex(client, CType.ROLESMAPPING, "sg_roles_mapping.yml", null);
        writeConfigToIndex(client, CType.ACTIONGROUPS, "sg_action_groups.yml", null);
        writeConfigToIndex(client, CType.TENANTS, "sg_roles_tenants.yml", null);
        writeConfigToIndex(client, CType.BLOCKS, "sg_blocks.yml", null);

        ConfigUpdateResponse configUpdateResponse = client
                .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]))).actionGet();

        if (configUpdateResponse.hasFailures()) {
            throw new RuntimeException("ConfigUpdateResponse produced failures: " + configUpdateResponse.failures());
        }
    }

    private void writeConfigToIndex(Client client, CType configType, String file, NestedValueMap overrides) {
        try {
            NestedValueMap config = NestedValueMap.fromYaml(openFile(file));

            if (overrides != null) {
                config.overrideLeafs(overrides);
            }

            System.out.println(config.toJsonString());
            
            client.index(new IndexRequest(indexName).id(configType.toLCString()).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(configType.toLCString(), BytesReference.fromByteBuffer(ByteBuffer.wrap(config.toJsonString().getBytes("utf-8")))))
                    .actionGet();

        } catch (Exception e) {
            throw new RuntimeException("Error while initializing config for " + indexName, e);
        }
    }

    private InputStream openFile(String file) throws IOException {

        String path;

        if (resourceFolder == null || resourceFolder.length() == 0) {
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

    public static class Role {
        private String name;
        private List<String> clusterPermissions = new ArrayList<>();
        private List<IndexPermission> indexPermissions = new ArrayList<>();

        public Role(String name) {
            this.name = name;
        }

        public Role clusterPermissions(String... clusterPermissions) {
            this.clusterPermissions.addAll(Arrays.asList(clusterPermissions));
            return this;
        }

        public IndexPermission indexPermissions(String... indexPermissions) {
            return new IndexPermission(this, indexPermissions);
        }

    }

    public static class IndexPermission {
        private List<String> allowedActions;
        private List<String> indexPatterns;
        private Role role;

        IndexPermission(Role role, String... allowedActions) {
            this.allowedActions = Arrays.asList(allowedActions);
            this.role = role;
        }

        public Role on(String... indexPatterns) {
            this.indexPatterns = Arrays.asList(indexPatterns);
            this.role.indexPermissions.add(this);
            return this.role;
        }

    }
}
