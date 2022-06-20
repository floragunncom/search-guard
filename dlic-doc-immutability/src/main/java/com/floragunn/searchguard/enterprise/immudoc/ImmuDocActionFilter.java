/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.enterprise.immudoc;

import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.metrics.CountAggregation;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.TimeAggregation;

public class ImmuDocActionFilter implements ActionFilter, ComponentStateProvider {
    private static final Logger log = LogManager.getLogger(ImmuDocActionFilter.class);

    private final ComponentState componentState = new ComponentState(1, null, "action_filter", ImmuDocActionFilter.class).requiresEnterpriseLicense()
            .initialized();

    private final AdminDNs adminDns;
    private final AuthInfoService authInfoService;
    private final AuditLog auditLog;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final Supplier<ImmuDocConfig> config;
    private final Supplier<Boolean> enabled;

    private final CountAggregation requestCount;
    private final CountAggregation blockedRequestCount;
    private final TimeAggregation.Nanoseconds applyTimeAggregation = new TimeAggregation.Nanoseconds();

    ImmuDocActionFilter(AdminDNs adminDns, AuthInfoService authInfoService, AuditLog auditLog, ActionRequestIntrospector actionRequestIntrospector,
            Supplier<ImmuDocConfig> config, Supplier<Boolean> enabled) {
        this.adminDns = adminDns;
        this.authInfoService = authInfoService;
        this.auditLog = auditLog;
        this.actionRequestIntrospector = actionRequestIntrospector;
        this.config = config;
        this.enabled = enabled;

        this.requestCount = new CountAggregation();
        this.blockedRequestCount = this.requestCount.getSubCount("blocked");

        this.componentState.addMetrics("request_count", requestCount);
        this.componentState.addMetrics("apply", applyTimeAggregation);

    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        try {
            User user = authInfoService.peekCurrentUser();

            if (user != null && adminDns.isAdmin(user)) {
                chain.proceed(task, action, request, listener);
                return;
            }

            if (!this.enabled.get()) {
                chain.proceed(task, action, request, listener);
                return;
            }

            ImmuDocConfig config = this.config.get();

            if (config == null || config.getImmutableIndicesPattern().isBlank()) {
                chain.proceed(task, action, request, listener);
                return;
            }

            if (config.getMetricsLevel().basicEnabled()) {
                this.requestCount.increment();
            }

            try (Meter meter = Meter.detail(config.getMetricsLevel(), applyTimeAggregation)) {
                if (request instanceof BulkShardRequest) {
                    for (BulkItemRequest item : ((BulkShardRequest) request).items()) {
                        if (!handleItem(action, item.request(), config)) {
                            auditLog.logImmutableIndexAttempt(request, action, task);
                            listener.onFailure(new ElasticsearchSecurityException("Index is immutable", RestStatus.FORBIDDEN));

                            if (config.getMetricsLevel().basicEnabled()) {
                                this.blockedRequestCount.increment();
                            }

                            return;
                        }
                    }

                    chain.proceed(task, action, request, listener);
                } else if (request instanceof DeleteRequest || request instanceof UpdateRequest || request instanceof UpdateByQueryRequest
                        || request instanceof DeleteByQueryRequest || request instanceof DeleteIndexRequest
                        || request instanceof RestoreSnapshotRequest || request instanceof CloseIndexRequest
                        || request instanceof IndicesAliasesRequest) {

                    if (isImmutable(action, request, config)) {
                        auditLog.logImmutableIndexAttempt(request, action, task);

                        listener.onFailure(new ElasticsearchSecurityException("Index is immutable", RestStatus.FORBIDDEN));

                        if (config.getMetricsLevel().basicEnabled()) {
                            this.blockedRequestCount.increment();
                        }
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                } else if (request instanceof IndexRequest) {
                    ((IndexRequest) request).opType(OpType.CREATE);

                    chain.proceed(task, action, request, new ActionListener<Response>() {

                        @Override
                        public void onResponse(Response response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException) {
                                auditLog.logImmutableIndexAttempt(request, action, task);
                                listener.onFailure(new ElasticsearchSecurityException("Index is immutable", RestStatus.FORBIDDEN));
                                if (config.getMetricsLevel().basicEnabled()) {
                                    blockedRequestCount.increment();
                                }
                            } else {
                                listener.onFailure(e);
                            }
                        }

                    });
                }
            }
        } catch (RuntimeException e) {
            log.error("Error in ImmuDocActionFilter", e);
            throw e;
        }
    }

    private boolean handleItem(String action, DocWriteRequest<?> request, ImmuDocConfig config) {
        if (!isImmutable(action, request, config)) {
            return true;
        }

        if (request instanceof IndexRequest) {
            ((IndexRequest) request).opType(OpType.CREATE);
            return true;
        } else { // request instanceof DeleteRequest || request instanceof UpdateRequest            
            return false;
        }
    }

    private boolean isImmutable(String action, Object request, ImmuDocConfig config) {
        ResolvedIndices resolved = actionRequestIntrospector.getActionRequestInfo(action, request).getResolvedIndices();

        if (resolved.isLocalAll()) {
            return true;
        } else {
            return config.getImmutableIndicesPattern().matches(resolved.getLocalIndices());
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }
}
