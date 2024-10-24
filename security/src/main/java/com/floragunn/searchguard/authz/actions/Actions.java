/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authz.actions;

import static com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors.objectAttr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.whoami.WhoAmIAction;
import com.floragunn.searchguard.authc.LoginPrivileges;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersConfigApi;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.authc.session.backend.SessionApi;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.AdditionalPrivileges;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.NewResource;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.RequestPropertyModifier;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.Resource;
import com.floragunn.searchguard.configuration.api.BulkConfigApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarRefreshAction;
import com.floragunn.searchguard.modules.api.GetComponentStateAction;
import com.floragunn.searchsupport.xcontent.AttributeValueFromXContent;
import com.floragunn.searchsupport.xcontent.XContentObjectConverter;

public class Actions {
    private final ImmutableMap<String, Action> actionMap;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> indexActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> clusterActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> tenantActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> openActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> allActions;

    private Builder builder = new Builder();

    public Actions(SearchGuardModulesRegistry modulesRegistry) {
        // We define here "well-known" actions. 
        //
        // Having well-known actions allows us to pre-cache a hash table of allowed actions for roles,
        // which can significantly improve performance of privilege checks.
        //
        // Additionally, extended settings are applied for some actions, such as additionally needed privileges.
        
        index("indices:data/write/index");
        index("indices:data/read/get");
        index("indices:data/read/tv");
        index("indices:data/write/delete");
        index("indices:data/write/update");
        index("indices:data/read/search");
        index("indices:data/read/open_point_in_time");
        index("indices:data/read/explain");
        index("indices:admin/resolve/index");
        index("indices:admin/resolve/cluster");

        index("indices:data/write/update/byquery");
        index("indices:data/write/delete/byquery");

        index("indices:data/write/bulk[s]")//
                .requestType(BulkShardRequest.class)//
                .requestItemsA(BulkShardRequest::items, (item) -> item.request().opType())
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.DELETE, "indices:data/write/delete")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.INDEX, "indices:data/write/index")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.CREATE, "indices:data/write/index")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.UPDATE, "indices:data/write/index");

        index("indices:admin/shards/search_shards") //
                .requiresAdditionalPrivileges(always(), "indices:data/read/search");
        index("indices:admin/search/search_shards") //
                .requiresAdditionalPrivileges(always(), "indices:data/read/search");

        cluster("indices:data/read/mget");
        cluster("indices:data/write/bulk");
        cluster("indices:data/read/scroll");
        cluster("indices:data/read/msearch");
        cluster("indices:data/read/mtv");

        cluster("indices:data/read/search/template");
        cluster("indices:data/read/msearch/template");

        index("indices:monitor/stats");
        index("indices:monitor/segments");
        index("indices:monitor/shard_stores");
        index("indices:admin/create") //
                .requestType(CreateIndexRequest.class)//
                .requiresAdditionalPrivileges(ifNotEmpty(CreateIndexRequest::aliases), "indices:admin/aliases");

        index("indices:admin/resize");
        index( "indices:admin/rollover");
        index("indices:admin/delete");
        index("indices:admin/get");
        index("indices:admin/open");
        index("indices:admin/close");
        index("indices:admin/block/add");
        index("indices:admin/mappings/get");
        index("indices:admin/mappings/fields/get");
        index("indices:admin/mapping/put");
        index("indices:admin/mapping/auto_put");

        index("indices:admin/aliases") //
                .requestType(IndicesAliasesRequest.class)//
                .requestItems(IndicesAliasesRequest::getAliasActions, IndicesAliasesRequest.AliasActions::actionType)//
                .requiresAdditionalPrivilegesForItemType(AliasActions.Type.REMOVE_INDEX, "indices:admin/delete");

        index("indices:admin/settings/update");
        index("indices:admin/analyze");
        index("indices:admin/auto_create");

        cluster("indices:data/read/scroll/clear");
        cluster("indices:monitor/recovery");
        cluster("cluster:admin/nodes/reload_secure_settings");

        cluster("indices:data/read/async_search/submit") //
                .createsResource("async_search", objectAttr("id"), xContentInstantFromMillis("expiration_time_in_millis"));

        cluster("indices:data/read/async_search/get") //
                .uses(new Resource("async_search", objectAttr("id")).ownerCheckBypassPermission("indices:searchguard:async_search/_all_owners"));

        cluster("indices:data/read/async_search/delete") //
                .deletes(new Resource("async_search", objectAttr("id")).ownerCheckBypassPermission("indices:searchguard:async_search/_all_owners"));
        
        cluster("indices:searchguard:async_search/_all_owners");

        cluster("indices:data/read/sql");
        cluster("indices:data/read/sql/translate");
        cluster("indices:data/read/sql/close_cursor");

        cluster("cluster:monitor/main");
        cluster("cluster:monitor/nodes/info");
        cluster("cluster:monitor/remote/info");
        cluster("cluster:monitor/nodes/stats");
        cluster("cluster:monitor/nodes/usage");
        cluster("cluster:monitor/nodes/hot_threads");
        cluster("cluster:monitor/tasks/lists");
        cluster("cluster:monitor/task/get");
        cluster("cluster:admin/tasks/cancel");

        cluster("cluster:admin/voting_config/add_exclusions");
        cluster("cluster:admin/voting_config/clear_exclusions");
        cluster("cluster:monitor/allocation/explain");
        cluster("cluster:monitor/stats");
        cluster("cluster:monitor/state");
        cluster("cluster:monitor/health");
        cluster("cluster:admin/settings/update");
        cluster("cluster:admin/reroute");
        cluster("cluster:monitor/task");
        cluster("cluster:admin/repository/put");
        cluster("cluster:admin/repository/get");
        cluster("cluster:admin/repository/delete");
        cluster("cluster:admin/repository/verify");
        cluster("cluster:admin/repository/_cleanup");
        cluster("cluster:admin/snapshot/get");
        cluster("cluster:admin/snapshot/delete");
        cluster("cluster:admin/snapshot/create");
        cluster("cluster:admin/snapshot/clone");

        cluster("cluster:admin/snapshot/restore")//
                .requestType(RestoreSnapshotRequest.class)//
                .requiresAdditionalPrivileges(always(), "indices:admin/create", "indices:data/write/index");

        cluster("cluster:admin/snapshot/status");

        cluster("indices:data/write/reindex");

        cluster("indices:admin/template/put");
        cluster("indices:admin/template/get");
        cluster("indices:admin/template/delete");
        cluster("cluster:admin/component_template/put");
        cluster("cluster:admin/component_template/get");
        cluster("cluster:admin/component_template/delete");
        cluster("indices:admin/index_template/put");
        cluster("indices:admin/index_template/get");
        cluster("indices:admin/index_template/delete");
        cluster("indices:admin/index_template/simulate_index");
        cluster("indices:admin/index_template/simulate");

        index("indices:admin/validate/query");
        index("indices:admin/refresh");
        index("indices:admin/refresh[s]");
        index("indices:admin/flush");
        index("indices:admin/forcemerge");
        index("indices:admin/cache/clear");

        index("indices:admin/aliases/get");
        index("indices:monitor/settings/get");

        index("indices:data/read/field_caps");

        cluster("cluster:admin/script/put");
        cluster("cluster:admin/script/get");
        cluster("cluster:admin/script/delete");
        cluster("cluster:admin/script_context/get");
        cluster("cluster:admin/script_language/get");

        cluster("cluster:admin/ingest/pipeline/put");
        cluster("cluster:admin/ingest/pipeline/get");
        cluster("cluster:admin/ingest/pipeline/delete");
        cluster("cluster:admin/ingest/pipeline/simulate");

        cluster( "cluster:admin/persistent/start");
        cluster("cluster:admin/persistent/update_status");
        cluster("cluster:admin/persistent/completion");
        cluster("cluster:admin/persistent/remove");

        cluster("cluster:admin/indices/dangling/list");
        cluster("cluster:admin/indices/dangling/import");
        cluster("cluster:admin/indices/dangling/delete");
        cluster("cluster:admin/indices/dangling/find");

        cluster("cluster:admin/snapshot/status[nodes]");

        open("indices:admin/seq_no/add_retention_lease");
        open("indices:admin/seq_no/renew_retention_lease");
        open("indices:admin/seq_no/remove_retention_lease");

        cluster(ConfigUpdateAction.INSTANCE);
        cluster(GetComponentStateAction.INSTANCE);
        cluster(BulkConfigApi.GetAction.INSTANCE);
        cluster(BulkConfigApi.UpdateAction.INSTANCE);
        cluster(ConfigVarRefreshAction.INSTANCE);
        cluster(ConfigVarApi.GetAction.INSTANCE);
        cluster(ConfigVarApi.UpdateAction.INSTANCE);
        cluster(ConfigVarApi.DeleteAction.INSTANCE);
        cluster(ConfigVarApi.GetAllAction.INSTANCE);
        cluster(ConfigVarApi.UpdateAllAction.INSTANCE);
        cluster(InternalUsersConfigApi.GetAction.INSTANCE);
        cluster(InternalUsersConfigApi.DeleteAction.INSTANCE);
        cluster(InternalUsersConfigApi.PutAction.INSTANCE);
        cluster(InternalUsersConfigApi.PatchAction.INSTANCE);
        cluster(GetActivatedFrontendConfigAction.INSTANCE);
        cluster(SessionApi.CreateAction.INSTANCE);
        cluster(SessionApi.DeleteAction.INSTANCE);
        cluster(SessionApi.GetExtendedInfoAction.INSTANCE);

        cluster(LoginPrivileges.SESSION);

        tenant("kibana:saved_objects/_/read");
        tenant("kibana:saved_objects/_/write");

        open("cluster:admin/searchguard/license/info");
        open(WhoAmIAction.INSTANCE);

        if (modulesRegistry != null) {
            for (ActionHandler<?, ?> action : modulesRegistry.getActions()) {
                builder.action(action.getAction());
            }
        }

        this.actionMap = builder.build();

        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> clusterActions = new ImmutableSet.Builder<>(actionMap.size());
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> indexActions = new ImmutableSet.Builder<>(actionMap.size());
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> tenantActions = new ImmutableSet.Builder<>();
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> openActions = new ImmutableSet.Builder<>();
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> allActions = new ImmutableSet.Builder<>(actionMap.size());

        for (Action action : actionMap.values()) {
            if (action.isOpen()) {
                openActions.add((WellKnownAction<?, ?, ?>) action);
            } else if (action.isClusterPrivilege()) {
                clusterActions.add((WellKnownAction<?, ?, ?>) action);
            } else if (action.isTenantPrivilege()) {
                tenantActions.add((WellKnownAction<?, ?, ?>) action);
            } else {
                indexActions.add((WellKnownAction<?, ?, ?>) action);
            }
            
            allActions.add((WellKnownAction<?, ?, ?>) action);
        }

        this.clusterActions = clusterActions.build();
        this.indexActions = indexActions.build();
        this.tenantActions = tenantActions.build();
        this.openActions = openActions.build();
        this.allActions = allActions.build();
    }

    public Action get(String actionName) {
        Action result = actionMap.get(actionName);

        if (result != null) {
            return result;
        } else {
            return new Action.OtherAction(actionName, getScope(actionName));
        }
    }

    public ImmutableSet<WellKnownAction<?, ?, ?>> clusterActions() {
        return clusterActions;
    }

    public ImmutableSet<WellKnownAction<?, ?, ?>> indexActions() {
        return indexActions;
    }

    public ImmutableSet<WellKnownAction<?, ?, ?>> tenantActions() {
        return tenantActions;
    }
    
    public ImmutableSet<WellKnownAction<?, ?, ?>> openActions() {
        return openActions;
    }
    
    public ImmutableSet<WellKnownAction<?, ?, ?>> allActions() {
        return allActions;
    }

    private static Scope getScope(String action) {
        if (action.startsWith("cluster:admin:searchguard:tenant:") || action.startsWith("kibana:saved_objects/")) {
            return Scope.TENANT;
        } else if (action.startsWith("searchguard:cluster:") || action.startsWith("cluster:")) {
            return Scope.CLUSTER;
        } else {
            return Scope.INDEX;
        }
    }

    private <RequestType extends ActionRequest> ActionBuilder<RequestType, Void, Void> cluster(ActionType<?> actionType) {
        return builder.cluster(actionType);
    }

    private ActionBuilder<?, ?, ?> cluster(String action) {
        return builder.cluster(action);
    }

    private ActionBuilder<?, ?, ?> index(String action) {
        return builder.index(action);
    }

    private ActionBuilder<?, ?, ?> tenant(String action) {
        return builder.tenant(action);
    }

    private <RequestType extends ActionRequest> ActionBuilder<RequestType, Void, Void> open(ActionType<?> actionType) {
        return builder.open(actionType);
    }

    private ActionBuilder<?, ?, ?> open(String action) {
        return builder.open(action);
    }

    class Builder {

        private Map<String, ActionBuilder<?, ?, ?>> builders = new HashMap<>(300);

        <RequestType extends ActionRequest> ActionBuilder<RequestType, Void, Void> cluster(ActionType<?> actionType) {
            ActionBuilder<RequestType, Void, Void> builder = new ActionBuilder<RequestType, Void, Void>(actionType.name(), Scope.CLUSTER);
            builders.put(actionType.name(), builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> cluster(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.CLUSTER);
            builders.put(action, builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> index(ActionType<?> actionType) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(actionType.name(), Scope.INDEX);
            builders.put(actionType.name(), builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> index(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.INDEX);
            builders.put(action, builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> tenant(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.TENANT);
            builders.put(action, builder);
            return builder;
        }

        <RequestType extends ActionRequest> ActionBuilder<RequestType, Void, Void> open(ActionType<?> actionType) {
            ActionBuilder<RequestType, Void, Void> builder = new ActionBuilder<RequestType, Void, Void>(actionType.name(), Scope.OPEN);
            builders.put(actionType.name(), builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> open(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.OPEN);
            builders.put(action, builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> action(ActionType<?> actionType) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(actionType.name(), getScope(actionType.name()));
            builders.put(actionType.name(), builder);
            return builder;
        }

        ImmutableMap<String, Action> build() {
            ImmutableMap.Builder<String, Action> result = new ImmutableMap.Builder<>(builders.size());

            for (ActionBuilder<?, ?, ?> builder : builders.values()) {
                Action action = builder.build();

                result.with(builder.actionName, action);
            }

            builders = null;

            return result.build();
        }

    }

    public static enum Scope {
        INDEX, CLUSTER, TENANT, OPEN;
    }

    class ActionBuilder<RequestType extends ActionRequest, RequestItem, RequestItemType> {

        private Class<RequestType> requestType;
        private String requestTypeName;

        private String actionName;
        private Class<Enum<?>> requestItemTypeEnum;
        private NewResource createsResource;
        private List<Resource> usesResources = new ArrayList<>();
        private List<RequestPropertyModifier<?>> requestProperyModifiers = new ArrayList<>();
        private List<AdditionalPrivileges<RequestType, RequestItem>> additionalPrivileges = new ArrayList<>();
        private Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType;
        private Scope scope;
        private Function<RequestType, Collection<RequestItem>> requestItemFunction;
        private Function<RequestItem, RequestItemType> requestItemTypeFunction;

        ActionBuilder(String actionName, Scope scope) {
            this.actionName = actionName;
            this.scope = scope;
        }

        <NewRequestType extends ActionRequest> ActionBuilder<NewRequestType, RequestItem, RequestItemType> requestType(
                Class<NewRequestType> requestType) {
            if (this.requestType != null && !this.requestType.equals(requestType)) {
                throw new IllegalStateException("Request type was already set: " + requestType + " vs " + this.requestType);
            }

            @SuppressWarnings("unchecked")
            ActionBuilder<NewRequestType, RequestItem, RequestItemType> newRequestTypeBuilder = (ActionBuilder<NewRequestType, RequestItem, RequestItemType>) this;
            newRequestTypeBuilder.requestType = requestType;
            return newRequestTypeBuilder;
        }

        @SuppressWarnings("unchecked")
        ActionBuilder<RequestType, RequestItem, RequestItemType> requestType(String requestType) {
            try {
                this.requestType = (Class<RequestType>) Class.forName(requestType);
            } catch (ClassNotFoundException e) {
                requestTypeName = requestType;
            }
            return this;
        }

        <NewRequestItem> ActionBuilder<RequestType, NewRequestItem, RequestItemType> requestItems(
                Function<RequestType, Collection<NewRequestItem>> function) {
            @SuppressWarnings("unchecked")
            ActionBuilder<RequestType, NewRequestItem, RequestItemType> newRequestTypeBuilder = (ActionBuilder<RequestType, NewRequestItem, RequestItemType>) this;

            newRequestTypeBuilder.requestItemFunction = function;

            return newRequestTypeBuilder;
        }

        <NewRequestItem, NewRequestItemType> ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> requestItems(
                Function<RequestType, Collection<NewRequestItem>> function, Function<NewRequestItem, NewRequestItemType> requestItemTypeFunction) {
            @SuppressWarnings("unchecked")
            ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> newRequestTypeBuilder = (ActionBuilder<RequestType, NewRequestItem, NewRequestItemType>) this;

            newRequestTypeBuilder.requestItemFunction = function;
            newRequestTypeBuilder.requestItemTypeFunction = requestItemTypeFunction;

            return newRequestTypeBuilder;
        }

        <NewRequestItem, NewRequestItemType> ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> requestItemsA(
                Function<RequestType, NewRequestItem[]> function, Function<NewRequestItem, NewRequestItemType> requestItemTypeFunction) {
            @SuppressWarnings("unchecked")
            ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> newRequestTypeBuilder = (ActionBuilder<RequestType, NewRequestItem, NewRequestItemType>) this;

            newRequestTypeBuilder.requestItemFunction = (r) -> Arrays.asList(function.apply(r));
            newRequestTypeBuilder.requestItemTypeFunction = requestItemTypeFunction;

            return newRequestTypeBuilder;
        }

        ActionBuilder<RequestType, RequestItem, RequestItemType> requiresAdditionalPrivileges(Predicate<RequestType> condition, String privilege,
                String... morePrivileges) {
            additionalPrivileges.add(new AdditionalPrivileges<RequestType, RequestItem>(ImmutableSet.of(privilege, morePrivileges), condition));

            return this;
        }

        ActionBuilder<RequestType, RequestItem, RequestItemType> requiresAdditionalPrivilegesForItemType(RequestItemType requestItemType,
                String privilege, String... morePrivileges) {
            ImmutableSet<String> privileges = ImmutableSet.of(privilege, morePrivileges);

            if (additionalPrivilegesByItemType == null) {
                if (Enum.class.isAssignableFrom(requestItemType.getClass())) {
                    @SuppressWarnings("unchecked")
                    Class<RequestItemType> requestItemTypeClass = (Class<RequestItemType>) requestItemType.getClass();
                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType = new EnumMap(requestItemTypeClass);
                    this.additionalPrivilegesByItemType = additionalPrivilegesByItemType;
                    @SuppressWarnings("unchecked")
                    Class<Enum<?>> enumClass = (Class<Enum<?>>) requestItemType.getClass();
                    requestItemTypeEnum = enumClass;
                } else {
                    additionalPrivilegesByItemType = new HashMap<>();
                }
            }

            ImmutableSet<String> existingPrivileges = additionalPrivilegesByItemType.get(requestItemType);

            if (existingPrivileges == null) {
                additionalPrivilegesByItemType.put(requestItemType, privileges);
            } else {
                additionalPrivilegesByItemType.put(requestItemType, privileges.with(existingPrivileges));
            }

            return this;
        }

        ActionBuilder<RequestType, RequestItem, RequestItemType> createsResource(String type, Function<ActionResponse, Object> id,
                Function<ActionResponse, Instant> expiresAfter) {
            createsResource = new NewResource(type, id, expiresAfter);
            return this;
        }

        ActionBuilder<RequestType, RequestItem, RequestItemType> uses(Resource resource) {
            usesResources.add(resource);
            return this;
        }

        ActionBuilder<RequestType, RequestItem, RequestItemType> deletes(Resource resource) {
            usesResources.add(resource.deleteAction(true));
            return this;
        }

        /*<PropertyType> ActionBuilder<RequestType, RequestItem, RequestItemType> setRequestProperty(String name, Class<PropertyType> type,
                Function<PropertyType, PropertyType> function) {
            requestProperyModifiers.add(new RequestPropertyModifier<>(ReflectiveAttributeAccessors.objectAttr(name, type),
                    ReflectiveAttributeAccessors.setObjectAttr(name, type), type, function));
            return this;
        }*/

        Action build() {
            Action.WellKnownAction.Resources resources = null;

            if (createsResource != null || !usesResources.isEmpty()) {
                resources = new Action.WellKnownAction.Resources(createsResource, usesResources);
            }

            Action.WellKnownAction.RequestItems<RequestType, RequestItem, RequestItemType> requestItems = null;

            if (requestItemTypeFunction != null || requestItemTypeEnum != null || requestItemTypeFunction != null) {
                requestItems = new Action.WellKnownAction.RequestItems<RequestType, RequestItem, RequestItemType>(requestItemTypeFunction,
                        requestItemFunction, additionalPrivilegesByItemType, requestItemTypeEnum, Actions.this);
            }

            return new Action.WellKnownAction<RequestType, RequestItem, RequestItemType>(actionName, scope, requestType, requestTypeName,
                    ImmutableList.of(additionalPrivileges),
                    additionalPrivilegesByItemType != null ? ImmutableMap.of(additionalPrivilegesByItemType) : ImmutableMap.empty(), requestItems,
                    resources, Actions.this);
        }

    }

    static <O> Function<O, Object> xContentAttr(String name) {
        return (actionResponse) -> AttributeValueFromXContent.get(XContentObjectConverter.convertOrNull(actionResponse), name);
    }

    static <O> Function<O, Instant> xContentInstantFromMillis(String name) {
        return (actionResponse) -> {
            Object value = AttributeValueFromXContent.get(XContentObjectConverter.convertOrNull(actionResponse), name);

            if (value instanceof Number) {
                return Instant.ofEpochMilli(((Number) value).longValue());
            } else if (value == null) {
                return null;
            } else {
                throw new RuntimeException("Unexpected value " + value + " for attribute " + name);
            }
        };
    }

    static <T> Predicate<T> ifNotEmpty(Function<T, Collection<?>> itemFunction) {
        return (t) -> {
            Collection<?> items = itemFunction.apply(t);

            return items != null && !items.isEmpty();
        };
    }

    static <Request extends ActionRequest> Predicate<Request> always() {
        return (request) -> true;
    }
    
    private static Actions forTestsInstance;
    
    /**
     * For testing only
     */
    public synchronized static Actions forTests() {
        if (forTestsInstance == null) {
            LogConfigurator.configureESLogging();
            forTestsInstance = new Actions(null);
        }
        
        return forTestsInstance;
    }

}
