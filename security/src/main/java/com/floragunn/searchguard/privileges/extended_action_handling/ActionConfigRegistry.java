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

package com.floragunn.searchguard.privileges.extended_action_handling;

import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.always;
import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.ifNotEmpty;
import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.xContentInstantFromMillis;
import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Scope.CLUSTER;
import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Scope.INDEX;
import static com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors.objectAttr;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;

public class ActionConfigRegistry {

    public static final ActionConfigRegistry INSTANCE = new ActionConfigRegistry(
            ActionConfig.of("indices:data/read/async_search/submit").scope(CLUSTER)
                    .createsResource("async_search", objectAttr("id"), xContentInstantFromMillis("expiration_time_in_millis")).build(),
            ActionConfig.of("indices:data/read/async_search/get").scope(CLUSTER).usesResource("async_search", objectAttr("id")).build(),
            ActionConfig.of("indices:data/read/async_search/delete").scope(CLUSTER).deletesResource("async_search", objectAttr("id")).build(),
            ActionConfig.of("indices:data/read/sql").scope(CLUSTER).build(),
            ActionConfig.of("indices:data/read/sql/translate").scope(CLUSTER).build(),
            ActionConfig.of("indices:data/read/sql/close_cursor").scope(CLUSTER).build(), //
            ActionConfig.of("indices:admin/shards/search_shards").scope(INDEX).requiresAdditionalPrivileges(always(), "indices:data/read/search") // 
                    .build(),
            ActionConfig.of("indices:admin/aliases").scope(INDEX).requestType(IndicesAliasesRequest.class)//
                    .requestItems(IndicesAliasesRequest::getAliasActions, IndicesAliasesRequest.AliasActions::actionType)//
                    .requiresAdditionalPrivilegesForItemType(AliasActions.Type.REMOVE_INDEX, "indices:admin/delete").build(),
            ActionConfig.of("indices:admin/create").scope(INDEX).requestType(CreateIndexRequest.class)//
                    .requiresAdditionalPrivileges(ifNotEmpty(CreateIndexRequest::aliases), "indices:admin/aliases").build(),
            ActionConfig.of(TransportShardBulkAction.ACTION_NAME).scope(INDEX).requestType(BulkShardRequest.class)
                    .requestItemsA(BulkShardRequest::items, (item) -> item.request().opType())
                    .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.DELETE, "indices:data/write/delete")
                    .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.INDEX, "indices:data/write/index")
                    .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.CREATE, "indices:data/write/index")
                    .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.UPDATE, "indices:data/write/index").build(),
            ActionConfig.of("cluster:admin/snapshot/restore").scope(CLUSTER).requestType(RestoreSnapshotRequest.class)
                    .requiresAdditionalPrivileges(always(), "indices:admin/create", "indices:data/write/index").build()

    );

    private Map<String, ActionConfig<?, ?, ?>> actionsByNameMap;
    private Set<String> clusterActions;
    private Set<String> tenantActions;

    public ActionConfigRegistry(ActionConfig<?, ?, ?>... actionConfigs) {
        Map<String, ActionConfig<?, ?, ?>> actionsByNameMap = new HashMap<>();
        Set<String> clusterActions = new HashSet<>();
        Set<String> tenantActions = new HashSet<>();

        for (ActionConfig<?, ?, ?> actionConfig : actionConfigs) {
            actionsByNameMap.put(actionConfig.getActionName(), actionConfig);

            if (actionConfig.getScope() == ActionConfig.Scope.CLUSTER) {
                clusterActions.add(actionConfig.getActionName());
            } else if (actionConfig.getScope() == ActionConfig.Scope.TENANT) {
                tenantActions.add(actionConfig.getActionName());
            }
        }

        this.actionsByNameMap = Collections.unmodifiableMap(actionsByNameMap);
        this.clusterActions = Collections.unmodifiableSet(clusterActions);
        this.tenantActions = Collections.unmodifiableSet(tenantActions);
    }

    public ActionConfig<?, ?, ?> get(String actionName) {
        return actionsByNameMap.get(actionName);
    }

    public <Request extends ActionRequest> ActionConfig<Request, ?, ?> get(String actionName, Request request) {
        ActionConfig<?, ?, ?> config = actionsByNameMap.get(actionName);

        if (config != null) {
            // Ensure type
            config.cast(request);
            @SuppressWarnings("unchecked")
            ActionConfig<Request, ?, ?> result = (ActionConfig<Request, ?, ?>) config;
            return result;
        } else {
            return null;
        }
    }

    public boolean isClusterAction(String actionName) {
        return clusterActions.contains(actionName);
    }

    public boolean isTenantAction(String actionName) {
        return tenantActions.contains(actionName);
    }
}
