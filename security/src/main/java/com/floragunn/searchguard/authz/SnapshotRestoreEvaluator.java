/*
 * Copyright 2015-2018 floragunn GmbH
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
package com.floragunn.searchguard.authz;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.SnapshotRestoreHelper;

public class SnapshotRestoreEvaluator {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final boolean enableSnapshotRestorePrivilege;
    private final AuditLog auditLog;
    private final boolean restoreSgIndexEnabled;
    private final GuiceDependencies guiceDependencies;
    
    public SnapshotRestoreEvaluator(final Settings settings, AuditLog auditLog, GuiceDependencies guiceDependencies) {
        this.enableSnapshotRestorePrivilege = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENABLE_SNAPSHOT_RESTORE_PRIVILEGE,
                ConfigConstants.SG_DEFAULT_ENABLE_SNAPSHOT_RESTORE_PRIVILEGE);
        this.restoreSgIndexEnabled = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_RESTORE_SGINDEX_ENABLED, false);
        this.auditLog = auditLog;
        this.guiceDependencies = guiceDependencies;
    }

    public PrivilegesEvaluationResult evaluate(final ActionRequest request, final Task task, final Action action, final ClusterInfoHolder clusterInfoHolder) {

        if (!(request instanceof RestoreSnapshotRequest)) {
            return PrivilegesEvaluationResult.PENDING;
        }
        
        // snapshot restore for regular users not enabled
        if (!enableSnapshotRestorePrivilege) {
            log.warn(action + " is not allowed for a regular user");
            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Action is not allowed for a non-admin user").missingPrivileges(action);
        }

        // if this feature is enabled, users can also snapshot and restore
        // the SG index and the global state
        if (restoreSgIndexEnabled) {
            return PrivilegesEvaluationResult.PENDING;
        }
        
        if (clusterInfoHolder.isLocalNodeElectedMaster() == Boolean.FALSE) {
            return PrivilegesEvaluationResult.OK;
        }
        
        final RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;

        // Do not allow restore of global state
        if (restoreRequest.includeGlobalState()) {
            auditLog.logSgIndexAttempt(request, action.name(), task);
            log.warn(action + " with 'include_global_state' enabled is not allowed");
            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Action with 'include_global_state' enabled is not allowed").missingPrivileges(action);
        }

        final List<String> rs = SnapshotRestoreHelper.resolveOriginalIndices(restoreRequest, guiceDependencies.getRepositoriesService());

        if (rs != null && (SearchGuardPlugin.getProtectedIndices().containsProtected(rs) || rs.contains("_all") || rs.contains("*"))) {
            auditLog.logSgIndexAttempt(request, action.name(), task);
            log.warn(action + " for '{}' as source index is not allowed", SearchGuardPlugin.getProtectedIndices().printProtectedIndices());
            return PrivilegesEvaluationResult.INSUFFICIENT.reason("Source index is protected").missingPrivileges(action);
        }
        
        return PrivilegesEvaluationResult.PENDING;
    }
}
