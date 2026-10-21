/*
 * Copyright 2023-2024 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.cluster;

import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.TestSgConfig.RoleMapping;
import com.floragunn.searchguard.test.TestSgConfig.User;

import java.util.Objects;

/**
 * The class can be used to temporally update configuration related to
 * <ul>
 *     <li>Roles</li>
 *     <li>Roles mapping</li>
 * </ul>
 * <b>The configuration is not validated</b> so that invalid configuration can be uploaded to the cluster on purpose. The same thing
 * cannot be achieved via usage of REST API because validation is performed on the REST layer.
 */
public class ConfigurationUpdater {

    private final LocalCluster.Embedded localCluster;
    private final User user;

    public ConfigurationUpdater(LocalCluster.Embedded localCluster, User user) {
        this.localCluster = Objects.requireNonNull(localCluster, "Local cluster must not be null");
        this.user = Objects.requireNonNull(user, "User is required");
    }

    /**
     * The method:
     * <ol>
     *     <li>Add to the cluster configuration role from parameter <code>desiredRole</code></li>
     *     <li>Invokes {@link ClientCallable} after configuration updates</li>
     *     <li>Roll back changes related to roles configuration</li>
     * </ol>
     * @param desiredRole role which will be added to the configuration. The role can be invalid, <b>role validation is omitted</b>.
     * @param clientCallable {@link ClientCallable} for the user {@link #user} which will be invoked after configuration update
     */
    public <T> T callWithRole(Role desiredRole, ClientCallable<T> clientCallable) throws Exception {
        return localCluster.callAndRestoreConfig(CType.ROLES, () -> {
            localCluster.updateRolesConfig(desiredRole);
            try (GenericRestClient client = localCluster.getRestClient(user)) {
                return clientCallable.callClient(client);
            }
        });
    }

    /**
     * The method:
     * <ol>
     *     <li>Add to the cluster configuration role mapping from parameter <code>desiredRole</code></li>
     *     <li>Invokes {@link ClientCallable} after configuration updates</li>
     *     <li>Roll back changes related to roles configuration</li>
     * </ol>
     * @param roleMapping role which will be added to the configuration. The role can be invalid, <b>role validation is omitted</b>.
     * @param clientCallable {@link ClientCallable} for the user {@link #user} which will be invoked after configuration update
     */
    public <T> T callWithMapping(RoleMapping roleMapping, ClientCallable<T> clientCallable) throws Exception {
        return localCluster.callAndRestoreConfig(CType.ROLESMAPPING, () -> {
            localCluster.updateRolesMappingsConfig(roleMapping);
            try (GenericRestClient client = localCluster.getRestClient(user)) {
                return clientCallable.callClient(client);
            }
        });
    }

    /**
     * Function which receives a {@link GenericRestClient} connected to the cluster
     */
    @FunctionalInterface
    public interface ClientCallable<T> {
        T callClient(GenericRestClient client) throws Exception;
    }
}
