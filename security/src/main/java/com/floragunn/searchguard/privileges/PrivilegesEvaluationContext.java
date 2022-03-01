/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.privileges;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;

import com.floragunn.searchguard.user.User;

public class PrivilegesEvaluationContext {
    private boolean resolveLocalAll = true;
    private final User user;
    private final IndexNameExpressionResolver resolver;
    private final ClusterService clusterService;

    PrivilegesEvaluationContext(User user, IndexNameExpressionResolver resolver, ClusterService clusterService) {
        this.user = user;
        this.resolver = resolver;
        this.clusterService = clusterService;
    }

    public User getUser() {
        return user;
    }

    public IndexNameExpressionResolver getResolver() {
        return resolver;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public boolean isResolveLocalAll() {
        return resolveLocalAll;
    }

    public void setResolveLocalAll(boolean resolveLocalAll) {
        this.resolveLocalAll = resolveLocalAll;
    }
}
