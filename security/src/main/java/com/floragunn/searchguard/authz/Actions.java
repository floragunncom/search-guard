package com.floragunn.searchguard.authz;

import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.always;
import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.ifNotEmpty;
import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.xContentInstantFromMillis;
import static com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors.objectAttr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.usage.NodesUsageAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
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
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
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
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
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
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import com.floragunn.searchguard.SearchGuardModulesRegistry;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.licenseinfo.LicenseInfoAction;
import com.floragunn.searchguard.action.whoami.WhoAmIAction;
import com.floragunn.searchguard.authc.internal_users_db.InternalUsersConfigApi;
import com.floragunn.searchguard.authz.Action.WellKnownAction;
import com.floragunn.searchguard.authz.ActionsRegistry.Builder.ActionBuilder;
import com.floragunn.searchguard.configuration.api.BulkConfigApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarApi;
import com.floragunn.searchguard.configuration.variables.ConfigVarRefreshAction;
import com.floragunn.searchguard.modules.api.GetComponentStateAction;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Scope;
import com.floragunn.searchsupport.util.ImmutableMap;
import com.floragunn.searchsupport.util.ImmutableSet;

public class Actions {
    private final ImmutableMap<String, Action> actionMap;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> indexActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> clusterActions;
    private final ImmutableSet<WellKnownAction<?, ?, ?>> tenantActions;

    private ActionsRegistry.Builder builder = new ActionsRegistry.Builder();

    Actions(SearchGuardModulesRegistry modulesRegistry) {
        index(IndexAction.INSTANCE);
        index(GetAction.INSTANCE);
        index(TermVectorsAction.INSTANCE);
        index(DeleteAction.INSTANCE);
        index(UpdateAction.INSTANCE);
        index(SearchAction.INSTANCE);
        index(ExplainAction.INSTANCE);
        index(ResolveIndexAction.INSTANCE);

        index(TransportShardBulkAction.ACTION_NAME)//
                .requestType(BulkShardRequest.class)//
                .requestItemsA(BulkShardRequest::items, (item) -> item.request().opType())
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.DELETE, "indices:data/write/delete")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.INDEX, "indices:data/write/index")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.CREATE, "indices:data/write/index")
                .requiresAdditionalPrivilegesForItemType(DocWriteRequest.OpType.UPDATE, "indices:data/write/index");

        index(ClusterSearchShardsAction.INSTANCE) //
                .requiresAdditionalPrivileges(always(), "indices:data/read/search");

        cluster(MultiGetAction.INSTANCE);
        cluster(BulkAction.INSTANCE);
        cluster(SearchScrollAction.INSTANCE);
        cluster(MultiSearchAction.INSTANCE);
        cluster(MultiTermVectorsAction.INSTANCE);

        cluster("indices:data/read/search/template");
        cluster("indices:data/read/msearch/template");

        index(IndicesStatsAction.INSTANCE);
        index(IndicesSegmentsAction.INSTANCE);
        index(IndicesShardStoresAction.INSTANCE);
        index(CreateIndexAction.INSTANCE) //
                .requestType(CreateIndexRequest.class)//
                .requiresAdditionalPrivileges(ifNotEmpty(CreateIndexRequest::aliases), "indices:admin/aliases");

        index(ResizeAction.INSTANCE);
        index(RolloverAction.INSTANCE);
        index(DeleteIndexAction.INSTANCE);
        index(GetIndexAction.INSTANCE);
        index(OpenIndexAction.INSTANCE);
        index(CloseIndexAction.INSTANCE);
        index(IndicesExistsAction.INSTANCE);
        index(TypesExistsAction.INSTANCE);
        index(AddIndexBlockAction.INSTANCE);
        index(GetMappingsAction.INSTANCE);
        index(GetFieldMappingsAction.INSTANCE);
        index(PutMappingAction.INSTANCE);
        index(AutoPutMappingAction.INSTANCE);

        index(IndicesAliasesAction.INSTANCE) //
                .requestType(IndicesAliasesRequest.class)//
                .requestItems(IndicesAliasesRequest::getAliasActions, IndicesAliasesRequest.AliasActions::actionType)//
                .requiresAdditionalPrivilegesForItemType(AliasActions.Type.REMOVE_INDEX, "indices:admin/delete");

        index(UpdateSettingsAction.INSTANCE);
        index(AnalyzeAction.INSTANCE);
        index(AutoCreateAction.INSTANCE);

        cluster(ClearScrollAction.INSTANCE);
        cluster(RecoveryAction.INSTANCE);
        cluster(NodesReloadSecureSettingsAction.INSTANCE);

        cluster("indices:data/read/async_search/submit") //
                .createsResource("async_search", objectAttr("id"), xContentInstantFromMillis("expiration_time_in_millis"));

        cluster("indices:data/read/async_search/get") //
                .usesResource("async_search", objectAttr("id"));

        cluster("indices:data/read/async_search/delete") //
                .deletesResource("async_search", objectAttr("id"));

        cluster("indices:data/read/sql");
        cluster("indices:data/read/sql/translate");
        cluster("indices:data/read/sql/close_cursor");

        cluster(MainAction.INSTANCE);
        cluster(NodesInfoAction.INSTANCE);
        cluster(RemoteInfoAction.INSTANCE);
        cluster(NodesStatsAction.INSTANCE);
        cluster(NodesUsageAction.INSTANCE);
        cluster(NodesHotThreadsAction.INSTANCE);
        cluster(ListTasksAction.INSTANCE);
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

        index(ValidateQueryAction.INSTANCE);
        index(RefreshAction.INSTANCE);
        index(FlushAction.INSTANCE);
        index(SyncedFlushAction.INSTANCE);
        index(ForceMergeAction.INSTANCE);
        index(UpgradeAction.INSTANCE);
        index(UpgradeStatusAction.INSTANCE);
        index(UpgradeSettingsAction.INSTANCE);
        index(ClearIndicesCacheAction.INSTANCE);

        index(GetAliasesAction.INSTANCE);
        index(AliasesExistAction.INSTANCE);
        index(GetSettingsAction.INSTANCE);

        cluster(PutStoredScriptAction.INSTANCE);
        cluster(GetStoredScriptAction.INSTANCE);
        cluster(DeleteStoredScriptAction.INSTANCE);
        cluster(GetScriptContextAction.INSTANCE);
        cluster(GetScriptLanguageAction.INSTANCE);

        cluster(FieldCapabilitiesAction.INSTANCE);

        cluster(PutPipelineAction.INSTANCE);
        cluster(GetPipelineAction.INSTANCE);
        cluster(DeletePipelineAction.INSTANCE);
        cluster(SimulatePipelineAction.INSTANCE);

        cluster(StartPersistentTaskAction.INSTANCE);
        cluster(UpdatePersistentTaskStatusAction.INSTANCE);
        cluster(CompletionPersistentTaskAction.INSTANCE);
        cluster(RemovePersistentTaskAction.INSTANCE);

        cluster(RetentionLeaseActions.Add.INSTANCE);
        cluster(RetentionLeaseActions.Renew.INSTANCE);
        cluster(RetentionLeaseActions.Remove.INSTANCE);

        cluster(ListDanglingIndicesAction.INSTANCE);
        cluster(ImportDanglingIndexAction.INSTANCE);
        cluster(DeleteDanglingIndexAction.INSTANCE);
        cluster(FindDanglingIndexAction.INSTANCE);

        cluster(TransportNodesSnapshotsStatus.ACTION_NAME);

        cluster(ConfigUpdateAction.INSTANCE);
        cluster(LicenseInfoAction.INSTANCE);
        cluster(WhoAmIAction.INSTANCE);
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

        for (ActionHandler<?, ?> action : modulesRegistry.getActions()) {
            cluster(action.getAction().name());
        }

        actionMap = builder.build();

        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> clusterActions = new ImmutableSet.Builder<>(actionMap.size());
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> indexActions = new ImmutableSet.Builder<>(actionMap.size());
        ImmutableSet.Builder<WellKnownAction<?, ?, ?>> tenantActions = new ImmutableSet.Builder<>();

        for (Action action : actionMap.values()) {
            if (action.isClusterPrivilege()) {
                clusterActions.add((WellKnownAction<?, ?, ?>) action);
            } else if (action.isTenantPrivilege()) {
                tenantActions.add((WellKnownAction<?, ?, ?>) action);
            } else {
                indexActions.add((WellKnownAction<?, ?, ?>) action);
            }
        }
        
        this.clusterActions = clusterActions.build();
        this.indexActions = indexActions.build();
        this.tenantActions = tenantActions.build();
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

    private static Scope getScope(String action) {
        if (action.startsWith("cluster:admin:searchguard:tenant:")) {
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

    private ActionBuilder<?, ?, ?> index(ActionType<?> actionType) {
        return builder.index(actionType);
    }

    private ActionBuilder<?, ?, ?> index(String action) {
        return builder.index(action);
    }

}
