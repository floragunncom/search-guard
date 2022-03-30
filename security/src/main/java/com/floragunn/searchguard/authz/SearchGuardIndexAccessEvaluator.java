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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ActionRequestInfo;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;

public class SearchGuardIndexAccessEvaluator {

    protected final Logger log = LogManager.getLogger(this.getClass());

    private final AuditLog auditLog;
    private final String[] sgDeniedActionPatterns;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final boolean filterSgIndex;

    public SearchGuardIndexAccessEvaluator(final Settings settings, AuditLog auditLog, ActionRequestIntrospector actionRequestIntrospector) {
        this.auditLog = auditLog;
        this.actionRequestIntrospector = actionRequestIntrospector;
        this.filterSgIndex = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_FILTER_SGINDEX_FROM_ALL_REQUESTS, false);

        final boolean restoreSgIndexEnabled = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_RESTORE_SGINDEX_ENABLED, false);

        final List<String> sgIndexDeniedActionPatternsList = new ArrayList<String>();
        sgIndexDeniedActionPatternsList.add("indices:data/write*");
        sgIndexDeniedActionPatternsList.add("indices:admin/delete*");
        sgIndexDeniedActionPatternsList.add("indices:admin/mapping/delete*");
        sgIndexDeniedActionPatternsList.add("indices:admin/mapping/put*");
        sgIndexDeniedActionPatternsList.add("indices:admin/freeze*");
        sgIndexDeniedActionPatternsList.add("indices:admin/settings/update*");
        sgIndexDeniedActionPatternsList.add("indices:admin/aliases");

        final List<String> sgIndexDeniedActionPatternsListNoSnapshot = new ArrayList<String>();
        sgIndexDeniedActionPatternsListNoSnapshot.addAll(sgIndexDeniedActionPatternsList);
        sgIndexDeniedActionPatternsListNoSnapshot.add("indices:admin/close*");
        sgIndexDeniedActionPatternsListNoSnapshot.add("cluster:admin/snapshot/restore*");

        sgDeniedActionPatterns = (restoreSgIndexEnabled ? sgIndexDeniedActionPatternsList : sgIndexDeniedActionPatternsListNoSnapshot)
                .toArray(new String[0]);
    }

    public PrivilegesEvaluationResult evaluate(final ActionRequest request, final Task task, final Action action,
            ActionRequestInfo actionRequestInfo) {
        if (!actionRequestInfo.isIndexRequest()) {
            return PrivilegesEvaluationResult.PENDING;
        }
        
        ResolvedIndices requestedResolved = actionRequestInfo.getResolvedIndices();
        
        if (WildcardMatcher.matchAny(sgDeniedActionPatterns, action.name())) {


            if (requestedResolved.isLocalAll()) {

                if (filterSgIndex) {
                    ImmutableSet<String> resolvedProtectedIndices = SearchGuardPlugin.getProtectedIndices().getProtectedIndicesAsMinusPattern();
                    actionRequestIntrospector.replaceIndices(request,
                            (r) -> r.isLocalAll() ? ImmutableList.of("*").with(resolvedProtectedIndices)
                                    : ImmutableList.of(r.getLocalIndices()).with(resolvedProtectedIndices).with(r.getRemoteIndices()),
                            actionRequestInfo);

                    if (log.isDebugEnabled()) {
                        log.debug("Filtered '{}'from {}, resulting list with *,-{} is {}",
                                SearchGuardPlugin.getProtectedIndices().printProtectedIndices(), requestedResolved, resolvedProtectedIndices);
                    }
                    return PrivilegesEvaluationResult.PENDING;
                } else {
                    auditLog.logSgIndexAttempt(request, action.name(), task);
                    log.warn(action + " for '_all' indices is not allowed for a regular user");
                    return PrivilegesEvaluationResult.INSUFFICIENT.reason("Action for '_all' indices is not allowed for a regular user").missingPrivileges(action);
                }
            } else if (SearchGuardPlugin.getProtectedIndices().containsProtected(requestedResolved.getLocalIndices())) {

                if (filterSgIndex) {
                    if (!actionRequestIntrospector.replaceIndices(
                            request, (r) -> ImmutableList.of(r.getLocalIndices())
                                    .without(SearchGuardPlugin.getProtectedIndices().getProtectedPatterns()).with(r.getRemoteIndices()),
                            actionRequestInfo)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Filtered '{}' but resulting list is empty", SearchGuardPlugin.getProtectedIndices().printProtectedIndices());
                        }
                        return PrivilegesEvaluationResult.INSUFFICIENT.reason("No unprotected indices referenced").missingPrivileges(action);
                    }
                    
                    return PrivilegesEvaluationResult.PENDING;
                } else {
                    auditLog.logSgIndexAttempt(request, action.name(), task);
                    log.warn(action + " for '{}' index is not allowed for a regular user",
                            SearchGuardPlugin.getProtectedIndices().printProtectedIndices());
                    return PrivilegesEvaluationResult.INSUFFICIENT.reason("Action requested index is not allowed for a regular user").missingPrivileges(action);
                }
            }
        }

        if (requestedResolved.isLocalAll() || SearchGuardPlugin.getProtectedIndices().containsProtected(requestedResolved.getLocalIndices())) {

            if (request instanceof SearchRequest) {
                ((SearchRequest) request).requestCache(Boolean.FALSE);
                if (log.isDebugEnabled()) {
                    log.debug("Disable search request cache for this request");
                }
            }

            if (request instanceof RealtimeRequest) {
                ((RealtimeRequest) request).realtime(Boolean.FALSE);
                if (log.isDebugEnabled()) {
                    log.debug("Disable realtime for this request");
                }
            }
        }
        return PrivilegesEvaluationResult.PENDING;
    }
}
