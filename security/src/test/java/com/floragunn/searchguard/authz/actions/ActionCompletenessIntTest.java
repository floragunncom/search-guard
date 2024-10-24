/*
 * Copyright 2024 floragunn GmbH
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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;


/**
 * This test can be used to discover when new actions are added to ES or when actions are removed. In such cases, the test will fail and report these actions.
 * If new actions are added, these should be reviewed whether special authorization is necessary.
 * If actions are removed, it might make sense to remove these from the Actions class and potentially other code such as action groups, tests, etc.
 */
@Ignore("ATM ignored to facilitate backporting procedures")
public class ActionCompletenessIntTest {

    /**
     * Index actions which are ignored by the test at the moment. It might make sense to review these actions sooner or later, as index actions might need special handling.
     */
    static final ImmutableSet<String> IGNORED_INDICES_ACTIONS = ImmutableSet.of(//
            "indices:admin/analyze[s]", //
            "indices:admin/cache/clear[n]", //
            "indices:admin/seq_no/retention_lease_background_sync[p]", //
            "indices:monitor/recovery[n]", //
            "indices:data/read/search[free_context]", //
            "indices:data/read/search[phase/dfs]", //
            "indices:data/read/explain[s]", //
            "indices:data/read/field_caps[n]", //
            "indices:admin/seq_no/retention_lease_sync[p]", //
            "indices:admin/block/add[s][r]", //
            "indices:admin/seq_no/retention_lease_sync", //
            "indices:data/read/search[free_context/scroll]", //
            "indices:admin/seq_no/remove_retention_lease[s]", //
            "indices:admin/seq_no/global_checkpoint_sync[r]", //
            "indices:admin/analyze_disk_usage", //
            "indices:data/read/get_from_translog", //
            "indices:admin/close[s]", //
            "indices:data/write/bulk[s][p]", //
            "indices:admin/mappings/fields/get[index]", //
            "indices:admin/refresh[s][r]", //
            "indices:admin/close[s][r]", //
            "indices:admin/refresh/unpromotable[u]", //
            "indices:admin/seq_no/global_checkpoint_sync", //
            "indices:data/read/mget[shard][s]", //
            "indices:admin/flush[s][r]", //
            "indices:data/read/open_reader_context", //
            "indices:monitor/segments[n]", //
            "indices:data/read/search[phase/fetch/id]", //
            "indices:monitor/field_usage_stats[n]", //
            "indices:admin/reload_analyzers", //
            "indices:admin/seq_no/retention_lease_background_sync", //
            "indices:admin/analyze_disk_usage[s]", //
            "indices:data/read/search[clear_scroll_contexts]", //
            "indices:admin/seq_no/retention_lease_background_sync[r]", //
            "indices:data/read/search[phase/rank/feature]", //
            "indices:data/read/search[phase/query/id]", //
            "indices:admin/forcemerge[n]", //
            "indices:data/read/get[s]", //
            "indices:admin/seq_no/renew_retention_lease[s]", //
            "indices:admin/data_stream/lazy_rollover", //
            "indices:data/read/close_point_in_time", //
            "indices:admin/block/add[s][p]", //
            "indices:data/read/search[phase/query]", //
            "indices:admin/seq_no/global_checkpoint_sync[p]", //
            "indices:monitor/stats[n]", //
            "indices:admin/seq_no/add_retention_lease[s]", //
            "indices:data/read/search[phase/fetch/id/scroll]", //
            "indices:admin/seq_no/retention_lease_sync[r]", //
            "indices:admin/validate/query[s]", //
            "indices:data/write/update[s]", //
            "indices:data/write/simulate/bulk", //
            "indices:data/write/bulk[s][r]", //
            "indices:admin/block/add[s]", //
            "indices:admin/refresh[s][p]", //
            "indices:data/read/search[phase/query/scroll]", //
            "indices:data/read/mtv[shard][s]", //
            "indices:admin/flush[s]", //
            "indices:admin/mappings/fields/get[index][s]", //
            "indices:data/read/tv[s]", //
            "indices:admin/refresh/unpromotable", //
            "indices:admin/flush[s][p]", //
            "indices:admin/close[s][p]", //
            "indices:monitor/field_usage_stats", //
            "indices:data/read/search[phase/query+fetch/scroll]", //
            "indices:admin/reload_analyzers[n]", //
            "indices:data/read/search[can_match][n]");

    /**
     * Index actions which are ignored by the test at the moment. Unknown cluster actions are less critical than unknown index actions, as these usually do not need additional resource checks in authorization.
     */
    static final ImmutableSet<String> IGNORED_CLUSTER_ACTIONS = ImmutableSet.of(//
            "cluster:admin:searchguard:config/authc/patch", //
            "cluster:monitor/health_api/stats[n]", //
            "cluster:admin:searchguard:config/authc_frontend/_all/patch", //
            "cluster:admin/desired_balance/get", //
            "cluster:admin/desired_nodes/get", //
            "cluster:internal/admin/tasks/cancel_child", //
            "cluster:admin/indices/dangling/list[n]", //
            "cluster:admin:searchguard:config/authc_frontend/_all/get", //
            "cluster:admin/desired_balance/reset", //
            "cluster:admin/synonym_rules/delete", //
            "cluster:admin:searchguard:config/sessions/patch", //
            "cluster:admin:searchguard:config/authc_frontend/_id/get", //
            "cluster:monitor/nodes/capabilities[n]", //
            "cluster:admin/snapshot/status[nodes][n]", //
            "cluster:internal/admin/tasks/ban", //
            "cluster:admin/migration/post_system_feature", //
            "cluster:admin:searchguard:config/authc/put", //
            "cluster:admin:searchguard:config/authc_frontend/_id/patch", //
            "cluster:admin/features/reset", //
            "cluster:admin:searchguard:config/sessions/put", //
            "cluster:admin/synonyms/delete", //
            "cluster:admin:searchguard:internal/indices/create[n]", //
            "cluster:admin/synonyms/put", //
            "cluster:admin/migration/get_system_feature", //
            "cluster:admin/tasks/cancel[n]", //
            "cluster:admin:searchguard:config_vars/refresh[n]", //
            "cluster:admin:searchguard:config/authz/get", //
            "cluster:admin/searchguard/session_token/update/push[n]", //
            "cluster:monitor/tasks/lists[n]", //
            "cluster:monitor/settings", //
            "cluster:admin/shutdown/prevalidate_removal", //
            "cluster:admin/searchguard/components/state[n]", //
            "cluster:admin/synonym_rules/get", //
            "cluster:monitor/update/health/info", //
            "cluster:admin/indices/dangling/find[n]", //
            "cluster:admin/desired_nodes/update", //
            "cluster:monitor/nodes/hot_threads[n]", //
            "cluster:admin:searchguard:config/authc_frontend/_all/put", //
            "cluster:monitor/nodes/info[n]", //
            "cluster:internal/remote_cluster/nodes", //
            "cluster:monitor/stats[n]", //
            "cluster:monitor/nodes/stats[n]", //
            "cluster:monitor/allocation/stats", //
            "cluster:admin:searchguard:config/migrate_index", //
            "cluster:monitor/nodes/features[n]", //
            "cluster:admin:searchguard:config/license_key/get", //
            "cluster:admin:searchguard:config/authc/delete", //
            "cluster:admin:searchguard:config/authc/get", //
            "cluster:admin:searchguard:config/sessions/get", //
            "cluster:monitor/fetch/health/info", //
            "cluster:admin:searchguard:config/authz/patch", //
            "cluster:admin/desired_nodes/delete", //
            "cluster:admin:searchguard:config/delete_by_type", //
            "cluster:admin/searchguard/config/update[n]", //
            "cluster:admin/reindex/rethrottle[n]", //
            "cluster:admin/synonyms/get", //
            "cluster:admin:searchguard:config/authc_frontend/_id/put", //
            "cluster:admin/synonyms_sets/get", //
            "cluster:admin/nodes/reload_secure_settings[n]", //
            "cluster:admin:searchguard:config/license_key/put", //
            "cluster:admin/features/get", //
            "cluster:monitor/nodes/usage[n]", //
            "cluster:admin:searchguard:get_license_info", //
            "cluster:admin:searchguard:config/license_key/patch", //
            "cluster:admin/synonym_rules/put", //
            "cluster:admin/reindex/rethrottle", //
            "cluster:admin:searchguard:cache/delete[n]", //
            "cluster:admin/searchguard/capabilities/cluster_wide/get[n]", //
            "cluster:admin:searchguard:config/authz/put");

    static final ImmutableSet<String> IGNORED_OTHER_ACTIONS = ImmutableSet.of(//
            "internal:transport/proxy/indices:data/read/search[phase/query+fetch/scroll]", //
            "internal:index/seq_no/resync", //
            "internal:admin/tasks/ban", //
            "internal:cluster/request_pre_vote", //
            "internal:cluster/coordination/join/ping", //
            "internal:transport/proxy/indices:data/read/search[phase/query]", //
            "internal:index/shard/recovery/file_chunk", //
            "internal:cluster/formation/info", //
            "internal:admin/repository/verify", //
            "internal:cluster/coordination_diagnostics/info", //
            "internal:index/shard/recovery/finalize", //
            "internal:transport/proxy/indices:data/read/search[can_match][n]", //
            "internal:transport/proxy/indices:data/read/search[free_context/scroll]", //
            "internal:index/seq_no/resync[p]", //
            "internal:admin/snapshot/get_shard", //
            "internal:cluster/shard/started", //
            "internal:data/read/mget_from_translog[shard]", //
            "internal:cluster/nodes/indices/shard/store[n]", //
            "internal:transport/proxy/indices:data/read/search[phase/fetch/id]", //
            "internal:cluster/shard/failure", //
            "internal:transport/proxy/indices:data/read/search[phase/fetch/id/scroll]", //
            "internal:index/shard/recovery/translog_ops", //
            "internal:index/shard/recovery/restore_file_from_snapshot", //
            "internal:transport/proxy/indices:data/read/search[phase/dfs]", //
            "internal:discovery/request_peers", //
            "internal:cluster/snapshot/update_snapshot_status", //
            "internal:transport/proxy/indices:data/read/search[clear_scroll_contexts]", //
            "internal:cluster/coordination/join/validate", //
            "internal:transport/proxy/indices:data/read/search[free_context]", //
            "internal:gateway/local/allocate_dangled", //
            "internal:cluster/master_history/get", //
            "internal:transport/proxy/indices:data/read/search[phase/rank/feature]", //
            "internal:index/shard/recovery/clean_files", //
            "internal:cluster/coordination/publish_state", //
            "internal:coordination/fault_detection/follower_check", //
            "internal:cluster/coordination/start_join", //
            "internal:transport/handshake", //
            "internal:cluster/coordination/join", //
            "internal:transport/proxy/indices:data/read/search[phase/query/id]", //
            "internal:admin/tasks/cancel_child", //
            "internal:coordination/fault_detection/leader_check", //
            "internal:index/shard/recovery/filesInfo", //
            "internal:indices/flush/synced/pre", //
            "internal:admin/indices/prevalidate_shard_path[n]", //
            "internal:index/seq_no/resync[r]", //
            "internal:index/shard/recovery/start_recovery", //
            "internal:transport/proxy/indices:data/read/search[phase/query/scroll]", //
            "internal:index/shard/recovery/reestablish_recovery", //
            "internal:index/shard/exists", //
            "internal:transport/proxy/indices:data/read/open_reader_context", //
            "internal:gateway/local/started_shards[n]", //
            "internal:cluster/coordination/commit_state", //
            "internal:index/shard/recovery/handoff_primary_context", //
            "internal:index/shard/recovery/prepare_translog");

    /**
     * Contains actions which are ok to be included in the well known actions, but which
     * are not reported by the test cluster - either because they are part of additional plugins
     * or because they are just dummy actions.
     */
    static final ImmutableSet<String> ADDITIONALLY_KNOWN_ACTIONS = ImmutableSet.of(//
            "kibana:saved_objects/_/write", //
            "kibana:saved_objects/_/read", //
            "indices:data/read/async_search/delete", //
            "indices:searchguard:async_search/_all_owners", //
            "indices:data/read/async_search/submit", //
            "indices:data/read/sql/translate", //
            "cluster:monitor/stats", //
            "cluster:monitor/nodes/info", //
            "cluster:admin:searchguard:login/session", //
            "indices:data/read/sql", //
            "cluster:admin/searchguard/config/update", //
            "indices:data/read/sql/close_cursor", //
            "cluster:monitor/nodes/hot_threads", //
            "cluster:admin/indices/dangling/find", //
            "cluster:admin/nodes/reload_secure_settings", //
            "cluster:admin/searchguard/components/state", //
            "cluster:admin/snapshot/status[nodes]", //
            "cluster:admin/indices/dangling/list", //
            "indices:data/read/async_search/get", //
            "cluster:monitor/nodes/stats", //
            "cluster:monitor/main", //
            "cluster:admin:searchguard:config_vars/refresh", //
            "cluster:monitor/nodes/usage", //
            "cluster:admin/searchguard/license/info");

    static final ImmutableSet<String> IGNORED_ACTIONS = IGNORED_CLUSTER_ACTIONS.with(IGNORED_INDICES_ACTIONS).with(IGNORED_OTHER_ACTIONS);

    static final Set<String> interceptedActions = new HashSet<>();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().singleNode().sslEnabled().embedded()
            .plugin(TransportInterceptorPlugin.class).build();

    @Test
    public void actionCompleteness() {
        Actions actions = new Actions(null);
        ImmutableSet<String> knownActions = actions.allActions().map(a -> a.name());
        Set<String> unknownIndicesActions = new HashSet<>();
        Set<String> unknownClusterActions = new HashSet<>();
        Set<String> unknownOtherActions = new HashSet<>();
        Set<String> unseenKnownActions = new HashSet<>(knownActions);

        for (String interceptedAction : interceptedActions) {
            if (!knownActions.contains(interceptedAction) && !IGNORED_ACTIONS.contains(interceptedAction)) {
                if (interceptedAction.startsWith("indices:")) {
                    unknownIndicesActions.add(interceptedAction);
                } else if (interceptedAction.startsWith("cluster:")) {
                    unknownClusterActions.add(interceptedAction);
                } else {
                    unknownOtherActions.add(interceptedAction);
                }
            }

            unseenKnownActions.remove(interceptedAction);
        }

        int countUnknown = unknownIndicesActions.size() + unknownClusterActions.size() + unknownOtherActions.size();

        if (countUnknown != 0) {
            Assert.fail("Found unknown actions:\nindex actions:\n" + DocNode.wrap(unknownIndicesActions).toPrettyJsonString() + "\ncluster actions:\n"
                    + DocNode.wrap(unknownClusterActions).toPrettyJsonString() + "\nother actions:\n"
                    + DocNode.wrap(unknownOtherActions).toPrettyJsonString());
        }

        unseenKnownActions.removeAll(ADDITIONALLY_KNOWN_ACTIONS);

        if (!unseenKnownActions.isEmpty()) {
            Assert.fail("Found well-known action which was not reported by ES:\n" + DocNode.wrap(unseenKnownActions).toPrettyJsonString());
        }

    }

    public static class TransportInterceptorPlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
            return Collections.singletonList(new TransportInterceptor() {

                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, Executor executor,
                        boolean forceExecution, TransportRequestHandler<T> actualHandler) {
                    interceptedActions.add(action);

                    return actualHandler;
                }
            });
        }
    }

}
