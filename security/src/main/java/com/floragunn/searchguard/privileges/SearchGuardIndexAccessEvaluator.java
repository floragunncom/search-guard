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

package com.floragunn.searchguard.privileges;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.resolver.IndexResolverReplacer;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.google.common.base.Objects;
import com.google.common.collect.ObjectArrays;

public class SearchGuardIndexAccessEvaluator {
    
    protected final Logger log = LogManager.getLogger(this.getClass());
    
    private final AuditLog auditLog;
    private final String[] sgDeniedActionPatterns;
    private final IndexResolverReplacer irr;
    private final boolean filterSgIndex;
    
    public SearchGuardIndexAccessEvaluator(final Settings settings, AuditLog auditLog, IndexResolverReplacer irr) {
        this.auditLog = auditLog;
        this.irr = irr;
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

        sgDeniedActionPatterns = (restoreSgIndexEnabled?sgIndexDeniedActionPatternsList:sgIndexDeniedActionPatternsListNoSnapshot).toArray(new String[0]);
    }
    
    public PrivilegesEvaluatorResponse evaluate(final ActionRequest request, final Task task, final String action, final Resolved requestedResolved,
            final PrivilegesEvaluatorResponse presponse)  {
                
        if (SearchGuardPlugin.getProtectedIndices().containsProtected(requestedResolved.getAllIndices())
                && WildcardMatcher.matchAny(sgDeniedActionPatterns, action)) {
            
            
            if(filterSgIndex) {
                Set<String> allWithoutProtected = new HashSet<>(requestedResolved.getAllIndices());
                SearchGuardPlugin.getProtectedIndices().filterIndices(allWithoutProtected);
                if(allWithoutProtected.isEmpty()) {
                    if(log.isDebugEnabled()) {
                        log.debug("Filtered '{}' but resulting list is empty", SearchGuardPlugin.getProtectedIndices().printProtectedIndices());
                    }
                    presponse.allowed = false;
                    return presponse.markComplete();
                }
                irr.replace(request, false, allWithoutProtected.toArray(new String[0]));
                if(log.isDebugEnabled()) {
                    log.debug("Filtered '{}', resulting list is {}", SearchGuardPlugin.getProtectedIndices().printProtectedIndices(), allWithoutProtected);
                }
                return presponse;
            } else {
                auditLog.logSgIndexAttempt(request, action, task);
                log.warn(action + " for '{}' index is not allowed for a regular user", SearchGuardPlugin.getProtectedIndices().printProtectedIndices());
                presponse.allowed = false;
                return presponse.markComplete();
            }

        }

        if (requestedResolved.isLocalAll()
                && WildcardMatcher.matchAny(sgDeniedActionPatterns, action)) {
            
            if(filterSgIndex) {
                final String[] resolvedProtectedIndices = SearchGuardPlugin.getProtectedIndices().getProtectedIndicesAsMinusPattern(irr, request);
                irr.replace(request, false, ObjectArrays.concat("*", resolvedProtectedIndices));
                if(log.isDebugEnabled()) {
                    log.debug("Filtered '{}'from {}, resulting list with *,-{} is {}", SearchGuardPlugin.getProtectedIndices().printProtectedIndices(), requestedResolved, Arrays.toString(resolvedProtectedIndices), irr.resolveRequest(request));
                }
                return presponse;
            } else {
              auditLog.logSgIndexAttempt(request, action, task);
              log.warn(action + " for '_all' indices is not allowed for a regular user");
              presponse.allowed = false;
              return presponse.markComplete();
            }
        }

        if(SearchGuardPlugin.getProtectedIndices().containsProtected(requestedResolved.getAllIndices())
                || requestedResolved.isLocalAll()) {

            if(request instanceof SearchRequest) {
                ((SearchRequest)request).requestCache(Boolean.FALSE);
                if(log.isDebugEnabled()) {
                    log.debug("Disable search request cache for this request");
                }
            }

            if(request instanceof RealtimeRequest) {
                ((RealtimeRequest) request).realtime(Boolean.FALSE);
                if(log.isDebugEnabled()) {
                    log.debug("Disable realtime for this request");
                }
            }
        }
        return presponse;
    }
}
