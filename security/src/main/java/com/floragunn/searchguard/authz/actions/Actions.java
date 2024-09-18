/*
 * Copyright 2022-2024 floragunn GmbH
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

import com.floragunn.searchsupport.xcontent.XContentObjectConverter;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetScriptContextAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetScriptLanguageAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.delete.DeleteDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.import_index.ImportDanglingIndexAction;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.root.MainRestPlugin;

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
import com.floragunn.searchguard.authz.ActionAuthorization.AliasDataStreamHandling;
import com.floragunn.searchguard.authz.actions.Action.Scope;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.AdditionalPrivileges;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.NewResource;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.RequestPropertyModifier;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction.Resource;
import com.floragunn.searchguard.configuration.api.BulkConfigApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarRefreshAction;
import com.floragunn.searchguard.modules.api.GetComponentStateAction;

import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors;
import com.floragunn.searchsupport.xcontent.AttributeValueFromXContent;

import static com.floragunn.searchguard.authz.actions.Action.AdditionalDimension.*;

public class Actions {
    private final ImmutableMap<String, Action> actionMap;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> indexLikeActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> clusterActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> tenantActions;

    private Builder builder = new Builder();

    public Actions(SearchGuardModulesRegistry modulesRegistry) {
        // We define here "well-known" actions. 
        //
        // Having well-known actions allows us to pre-cache a hash table of allowed actions for roles,
        // which can significantly improve performance of privilege checks.
        //
        // Additionally, extended settings are applied for some actions, such as additionally needed privileges.

        indexLike(IndexAction.INSTANCE).aliasesResolveToWriteTarget();
        indexLike(GetAction.INSTANCE);
        indexLike(TermVectorsAction.INSTANCE);
        indexLike(DeleteAction.INSTANCE);
        indexLike(UpdateAction.INSTANCE);
        index(TransportSearchAction.TYPE);
        index(TransportExplainAction.TYPE);
        indexLike(ResolveIndexAction.INSTANCE);

        indexLike(UpdateByQueryAction.INSTANCE);
        indexLike(DeleteByQueryAction.INSTANCE);

        indexLike(TransportShardBulkAction.ACTION_NAME)//
                .requestType(BulkShardRequest.class)//
                .requestItemsA(BulkShardRequest::items, (item) -> item.request().opType())
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.DELETE, "indices:data/write/delete")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.INDEX, "indices:data/write/index")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.CREATE, "indices:data/write/index")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.UPDATE, "indices:data/write/index");

        indexLike(ClusterSearchShardsAction.INSTANCE) //
                .requiresAdditionalPrivileges(always(), "indices:data/read/search");
        index(TransportSearchShardsAction.TYPE) //
                .requiresAdditionalPrivileges(always(), "indices:data/read/search");

        indexLike(MultiGetAction.NAME + "[shard]");

        cluster(MultiGetAction.INSTANCE);
        cluster(BulkAction.INSTANCE);
        cluster(TransportSearchScrollAction.TYPE);
        cluster(TransportMultiSearchAction.TYPE);
        cluster(MultiTermVectorsAction.INSTANCE);

        cluster("indices:data/read/search/template");
        cluster("indices:data/read/msearch/template");

        indexLike(IndicesStatsAction.INSTANCE);
        indexLike(IndicesSegmentsAction.INSTANCE);
        indexLike(IndicesShardStoresAction.INSTANCE);

        index(CreateIndexAction.INSTANCE)
            .requestType(CreateIndexRequest.class)//
            .requiresAdditionalPrivileges(ifNotEmpty(CreateIndexRequest::aliases), "indices:admin/aliases")
            .additionalDimensions(MANAGE_ALIASES);
        index(OpenIndexAction.INSTANCE);
        index(CloseIndexAction.INSTANCE);
        index(ResizeAction.INSTANCE).additionalDimensions(RESIZE_TARGET);
        index(DownsampleAction.INSTANCE).additionalDimensions(DOWNSAMPLE_TARGET);

        indexLike(RolloverAction.INSTANCE);
        indexLike(DeleteIndexAction.INSTANCE);
        indexLike(GetIndexAction.INSTANCE);
        indexLike(AddIndexBlockAction.INSTANCE);
        indexLike(GetMappingsAction.INSTANCE);
        indexLike(GetFieldMappingsAction.INSTANCE);
        indexLike(PutMappingAction.INSTANCE);
        indexLike(AutoPutMappingAction.INSTANCE);

        indexLike(IndicesAliasesAction.INSTANCE) //
            .requestType(IndicesAliasesRequest.class)//
            .requestItems(IndicesAliasesRequest::getAliasActions, IndicesAliasesRequest.AliasActions::actionType)//
            .requiresAdditionalPrivilegesForItemType(AliasActions.Type.REMOVE_INDEX, "indices:admin/delete")
            .additionalDimensions(ALIASES);

        indexLike(UpdateSettingsAction.INSTANCE);
        indexLike(AnalyzeAction.INSTANCE);
        indexLike(AutoCreateAction.INSTANCE);

        cluster(TransportClearScrollAction.TYPE);
        cluster(RecoveryAction.INSTANCE);
        cluster(NodesReloadSecureSettingsAction.INSTANCE);

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

        cluster(MainRestPlugin.MAIN_ACTION);
        cluster(TransportNodesInfoAction.TYPE);
        cluster(TransportRemoteInfoAction.TYPE);
        cluster(TransportNodesStatsAction.TYPE);
        cluster(TransportNodesUsageAction.TYPE);
        cluster(TransportNodesHotThreadsAction.TYPE);
        cluster(TransportListTasksAction.TYPE);
        cluster(GetTaskAction.INSTANCE);
        cluster(CancelTasksAction.INSTANCE);

        cluster(AddVotingConfigExclusionsAction.INSTANCE);
        cluster(ClearVotingConfigExclusionsAction.INSTANCE);
        cluster(ClusterAllocationExplainAction.INSTANCE);
        cluster(ClusterStatsAction.INSTANCE);
        cluster(ClusterStateAction.INSTANCE);
        cluster(ClusterHealthAction.INSTANCE);
        cluster(ClusterUpdateSettingsAction.INSTANCE);
        cluster(ClusterRerouteAction.INSTANCE);
        cluster(PendingClusterTasksAction.INSTANCE);
        cluster(PutRepositoryAction.INSTANCE);
        cluster(GetRepositoriesAction.INSTANCE);
        cluster(DeleteRepositoryAction.INSTANCE);
        cluster(VerifyRepositoryAction.INSTANCE);
        cluster(CleanupRepositoryAction.INSTANCE);
        cluster(GetSnapshotsAction.INSTANCE);
        cluster(DeleteSnapshotAction.INSTANCE);
        cluster(CreateSnapshotAction.INSTANCE);
        cluster(CloneSnapshotAction.INSTANCE);

        cluster(RestoreSnapshotAction.INSTANCE)//
                .requestType(RestoreSnapshotRequest.class)//
                .requiresAdditionalPrivileges(always(), "indices:admin/create", "indices:data/write/index");

        cluster(SnapshotsStatusAction.INSTANCE);

        cluster(ReindexAction.INSTANCE);

        cluster(PutIndexTemplateAction.INSTANCE);
        cluster(GetIndexTemplatesAction.INSTANCE);
        cluster(DeleteIndexTemplateAction.INSTANCE);
        cluster(PutComponentTemplateAction.INSTANCE);
        cluster(GetComponentTemplateAction.INSTANCE);
        cluster(DeleteComponentTemplateAction.INSTANCE);
        cluster(PutComposableIndexTemplateAction.INSTANCE);
        cluster(GetComposableIndexTemplateAction.INSTANCE);
        cluster(DeleteComposableIndexTemplateAction.INSTANCE);
        cluster(SimulateIndexTemplateAction.INSTANCE);
        cluster(SimulateTemplateAction.INSTANCE);

        indexLike(ValidateQueryAction.INSTANCE);
        indexLike(RefreshAction.INSTANCE);
        indexLike(TransportShardRefreshAction.NAME);
        indexLike(FlushAction.INSTANCE);
        indexLike(ForceMergeAction.INSTANCE);
        indexLike(ClearIndicesCacheAction.INSTANCE);

        indexLike(GetAliasesAction.INSTANCE).additionalDimensions(ALIASES);
        indexLike(GetSettingsAction.INSTANCE);

        indexLike(FieldCapabilitiesAction.INSTANCE);

        cluster(PutStoredScriptAction.INSTANCE);
        cluster(GetStoredScriptAction.INSTANCE);
        cluster(DeleteStoredScriptAction.INSTANCE);
        cluster(GetScriptContextAction.INSTANCE);
        cluster(GetScriptLanguageAction.INSTANCE);

        cluster(PutPipelineAction.INSTANCE);
        cluster(GetPipelineAction.INSTANCE);
        cluster(DeletePipelineAction.INSTANCE);
        cluster(SimulatePipelineAction.INSTANCE);

        cluster(StartPersistentTaskAction.INSTANCE);
        cluster(UpdatePersistentTaskStatusAction.INSTANCE);
        cluster(CompletionPersistentTaskAction.INSTANCE);
        cluster(RemovePersistentTaskAction.INSTANCE);

        cluster(ListDanglingIndicesAction.INSTANCE);
        cluster(ImportDanglingIndexAction.INSTANCE);
        cluster(DeleteDanglingIndexAction.INSTANCE);
        cluster(FindDanglingIndexAction.INSTANCE);

        cluster(TransportNodesSnapshotsStatus.ACTION_NAME);

        open(RetentionLeaseActions.Add.INSTANCE);
        open(RetentionLeaseActions.Renew.INSTANCE);
        open(RetentionLeaseActions.Remove.INSTANCE);

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

        dataStream("indices:admin/data_stream/create");
        dataStream("indices:admin/data_stream/get");
        dataStream("indices:admin/data_stream/migrate");
        dataStream("indices:admin/data_stream/modify");
        dataStream("indices:admin/data_stream/promote");
        dataStream("indices:admin/data_stream/delete");
        dataStream("indices:monitor/data_stream/stats");

        if (modulesRegistry != null) {
            for (ActionHandler<?, ?> action : modulesRegistry.getActions()) {
                builder.action(action.getAction());
            }
        }

        this.actionMap = builder.build();

        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> clusterActions = new ImmutableSet.Builder<>(actionMap.size());
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> indexLikeActions = new ImmutableSet.Builder<>(actionMap.size());
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> tenantActions = new ImmutableSet.Builder<>();

        for (Action action : actionMap.values()) {
            if (action.isClusterPrivilege()) {
                clusterActions.add((WellKnownAction<?, ?, ?>) action);
            } else if (action.isTenantPrivilege()) {
                tenantActions.add((WellKnownAction<?, ?, ?>) action);
            } else {
                indexLikeActions.add((WellKnownAction<?, ?, ?>) action);
            }
        }

        this.clusterActions = clusterActions.build();
        this.tenantActions = tenantActions.build();
        this.indexLikeActions = indexLikeActions.build();
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

    public ImmutableSet<WellKnownAction<?, ?, ?>> indexLikeActions() {
        return indexLikeActions;
    }

    public ImmutableSet<WellKnownAction<?, ?, ?>> tenantActions() {
        return tenantActions;
    }

    private static Scope getScope(String action) {
        if (action.startsWith("indices:admin/data_stream/")) {
            return Scope.DATA_STREAM;
        } else if (action.startsWith("cluster:admin:searchguard:tenant:") || action.startsWith("kibana:saved_objects/")) {
            return Scope.TENANT;
        } else if (action.startsWith("searchguard:cluster:") || action.startsWith("cluster:")) {
            return Scope.CLUSTER;
        } else {
            return Scope.INDEX_LIKE;
        }
    }

    private <RequestType extends ActionRequest> ActionBuilder<RequestType, Void, Void> cluster(ActionType<?> actionType) {
        return builder.cluster(actionType);
    }

    private ActionBuilder<?, ?, ?> cluster(String action) {
        return builder.cluster(action);
    }

    private ActionBuilder<?, ?, ?> indexLike(ActionType<?> actionType) {
        return builder.indexLike(actionType);
    }

    private ActionBuilder<?, ?, ?> indexLike(String action) {
        return builder.indexLike(action);
    }

    private ActionBuilder<?, ?, ?> index(ActionType<?> actionType) {
        return builder.index(actionType);
    }

    private ActionBuilder<?, ?, ?> alias(ActionType<?> actionType) {
        return builder.alias(actionType);
    }

    private ActionBuilder<?, ?, ?> dataStream(String action) {
        return builder.dataStream(action);
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

        ActionBuilder<?, ?, ?> indexLike(ActionType<?> actionType) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(actionType.name(), Scope.INDEX_LIKE);
            builders.put(actionType.name(), builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> indexLike(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.INDEX_LIKE);
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

        ActionBuilder<?, ?, ?> alias(ActionType<?> actionType) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(actionType.name(), Scope.ALIAS);
            builders.put(actionType.name(), builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> dataStream(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.DATA_STREAM);
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
        private AliasDataStreamHandling aliasDataStreamHandling = AliasDataStreamHandling.RESOLVE_IF_NECESSARY;
        private Meta.Alias.ResolutionMode aliasResolutionMode = Meta.Alias.ResolutionMode.NORMAL;
        private ImmutableSet<Action.AdditionalDimension> additionalDimensions = ImmutableSet.empty();

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

        ActionBuilder<RequestType, RequestItem, RequestItemType> additionalDimensions(Action.AdditionalDimension... additionalDimensions) {
            this.additionalDimensions = ImmutableSet.ofArray(additionalDimensions);

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

        ActionBuilder<RequestType, RequestItem, RequestItemType> doNotResolveAliasesOrDataStreams() {
            this.aliasDataStreamHandling = AliasDataStreamHandling.DO_NOT_RESOLVE;
            return this;
        }

        ActionBuilder<RequestType, RequestItem, RequestItemType> aliasesResolveToWriteTarget() {
            this.aliasResolutionMode = Meta.Alias.ResolutionMode.TO_WRITE_TARGET;
            return this;
        }

        <PropertyType> ActionBuilder<RequestType, RequestItem, RequestItemType> setRequestProperty(String name, Class<PropertyType> type,
                Function<PropertyType, PropertyType> function) {
            requestProperyModifiers.add(new RequestPropertyModifier<>(ReflectiveAttributeAccessors.objectAttr(name, type),
                    ReflectiveAttributeAccessors.setObjectAttr(name, type), type, function));
            return this;
        }

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
                    resources, this.aliasDataStreamHandling, aliasResolutionMode, additionalDimensions, Actions.this);
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

}
