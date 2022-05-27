/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.filter;

import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.action.whoami.WhoAmIAction;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.Action.WellKnownAction;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.compliance.ComplianceConfig;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.DlsFlsRequestValve;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.privileges.extended_action_handling.ExtendedActionHandlingService;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SourceFieldsContext;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;

public class SearchGuardFilter implements ActionFilter {
    

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final Logger actionTrace = LogManager.getLogger("sg_action_trace");
    private final PrivilegesEvaluator evalp;
    private final AdminDNs adminDns;
    private DlsFlsRequestValve dlsFlsValve;
    private final AuditLog auditLog;
    private final ThreadContext threadContext;
    private final ClusterService cs;
    private final ComplianceConfig complianceConfig;
    private final SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry;
    private final ExtendedActionHandlingService extendedActionHandlingService;
    private final DiagnosticContext diagnosticContext;
    private final Actions actions;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final IndexNameExpressionResolver resolver;

    public SearchGuardFilter(final PrivilegesEvaluator evalp, final AdminDNs adminDns, DlsFlsRequestValve dlsFlsValve, AuditLog auditLog,
            ThreadPool threadPool, ClusterService cs, DiagnosticContext diagnosticContext, ComplianceConfig complianceConfig, Actions actions,
            ActionRequestIntrospector actionRequestIntrospector,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry,
            ExtendedActionHandlingService extendedActionHandlingService, NamedXContentRegistry namedXContentRegistry, IndexNameExpressionResolver resolver) {
        this.evalp = evalp;
        this.adminDns = adminDns;
        this.dlsFlsValve = dlsFlsValve;
        this.auditLog = auditLog;
        this.threadContext = threadPool.getThreadContext();
        this.cs = cs;
        this.complianceConfig = complianceConfig;
        this.specialPrivilegesEvaluationContextProviderRegistry = specialPrivilegesEvaluationContextProviderRegistry;
        this.extendedActionHandlingService = extendedActionHandlingService;
        this.diagnosticContext = diagnosticContext;
        this.actions = actions;
        this.actionRequestIntrospector = actionRequestIntrospector;
        this.resolver = resolver;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, final String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        
        specialPrivilegesEvaluationContextProviderRegistry.provide(threadContext.getTransient(ConfigConstants.SG_USER), threadContext,
                (specialPrivilegesEvaluationContext) -> {                    
                    try (StoredContext ctx = threadContext.newStoredContext(true)) {
                        apply0(task, action, request, listener, chain, specialPrivilegesEvaluationContext);
                    } catch (Exception e) {
                        log.error(e);
                        listener.onFailure(new OpenSearchSecurityException("Unexpected exception " + action, RestStatus.INTERNAL_SERVER_ERROR, e));
                    }
                }, (e) -> {
                    log.error(e);
                    listener.onFailure(new OpenSearchSecurityException("Unexpected exception " + action, RestStatus.INTERNAL_SERVER_ERROR, e));
                });
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void apply0(Task task, String actionName, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain, SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext ) {

        try {

            if (threadContext.getTransient(ConfigConstants.SG_ORIGIN) == null) {
                threadContext.putTransient(ConfigConstants.SG_ORIGIN, Origin.LOCAL.toString());
            }
            
            if (complianceConfig != null && complianceConfig.isEnabled()) {
                attachSourceFieldContext(request);
            }

            
            User user = threadContext.getTransient(ConfigConstants.SG_USER);
            final boolean userIsAdmin = isUserAdmin(user, adminDns);
            final boolean interClusterRequest = HeaderHelper.isInterClusterRequest(threadContext);
            final boolean trustedClusterRequest = HeaderHelper.isTrustedClusterRequest(threadContext);
            final boolean confRequest = "true".equals(HeaderHelper.getSafeFromHeader(threadContext, ConfigConstants.SG_CONF_REQUEST_HEADER));
            final boolean passThroughRequest = actionName.equals("cluster:admin/searchguard/license/info") || actionName.startsWith("indices:admin/seq_no")
                    || actionName.equals(WhoAmIAction.NAME);

            final boolean internalRequest = (interClusterRequest || HeaderHelper.isDirectRequest(threadContext)) && actionName.startsWith("internal:")
                    && !actionName.startsWith("internal:transport/proxy");
            
            diagnosticContext.addHeadersToLogContext(cs, threadContext);

            if (specialPrivilegesEvaluationContext != null) {
                if (log.isDebugEnabled()) {
                    log.debug("userIsAdmin: " + userIsAdmin + "\n" + "interClusterRequest: " + interClusterRequest + "\ntrustedClusterRequest: "
                            + trustedClusterRequest + "\nconfRequest: " + confRequest + "\npassThroughRequest: " + passThroughRequest);
                    log.debug("Getting auth from specialPrivilegesEvaluationContext.\nOld user: " + user + "\nNew auth: " + specialPrivilegesEvaluationContext);
                    log.debug(threadContext.getHeaders());
                }

                user = specialPrivilegesEvaluationContext.getUser();
                
                if (user != null && threadContext.getTransient(ConfigConstants.SG_USER) == null) {
                    threadContext.putTransient(ConfigConstants.SG_USER, user);
                }
            }

             org.apache.logging.log4j.ThreadContext.put("user", user != null ? user.getName() : "n/a");

            if (actionTrace.isTraceEnabled()) {

                String count = "";
                if (request instanceof BulkRequest) {
                    count = "" + ((BulkRequest) request).requests().size();
                }

                if (request instanceof MultiGetRequest) {
                    count = "" + ((MultiGetRequest) request).getItems().size();
                }

                if (request instanceof MultiSearchRequest) {
                    count = "" + ((MultiSearchRequest) request).requests().size();
                }

                actionTrace.trace("Node " + cs.localNode().getName() + " -> " + actionName + " (" + count + "): userIsAdmin=" + userIsAdmin
                        + "/conRequest=" + confRequest + "/internalRequest=" + internalRequest + "origin="
                        + threadContext.getTransient(ConfigConstants.SG_ORIGIN) + "/directRequest=" + HeaderHelper.isDirectRequest(threadContext)
                        + "/remoteAddress=" + request.remoteAddress());

                threadContext.putHeader("_sg_trace" + System.currentTimeMillis() + "#" + UUID.randomUUID().toString(),
                        Thread.currentThread().getName() + " FILTER -> " + "Node " + cs.localNode().getName() + " -> " + actionName + " userIsAdmin="
                                + userIsAdmin + "/conRequest=" + confRequest + "/internalRequest=" + internalRequest + "origin="
                                + threadContext.getTransient(ConfigConstants.SG_ORIGIN) + "/directRequest="
                                + HeaderHelper.isDirectRequest(threadContext) + "/remoteAddress=" + request.remoteAddress() + " "
                                + threadContext.getHeaders().entrySet().stream().filter(p -> !p.getKey().startsWith("_sg_trace"))
                                        .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));

            }

            if (userIsAdmin || confRequest || internalRequest || passThroughRequest) {

                if (userIsAdmin && !confRequest && !internalRequest && !passThroughRequest) {
                    auditLog.logGrantedPrivileges(actionName, request, task);
                }

                chain.proceed(task, actionName, request, listener);
                return;
            }
            
            Action action = actions.get(actionName);

            if (complianceConfig != null && complianceConfig.isEnabled()) {

                Tuple<ImmutableState, ActionListener> immutableResult;

                if (request instanceof BulkShardRequest) {
                    for (BulkItemRequest bsr : ((BulkShardRequest) request).items()) {
                        immutableResult = checkImmutableIndices(bsr.request(), request, listener, actionName, task, auditLog);
                        if (immutableResult != null && immutableResult.v1() == ImmutableState.FAILURE) {
                            return;
                        }

                        if (immutableResult != null && immutableResult.v1() == ImmutableState.LISTENER) {
                            listener = immutableResult.v2();
                        }
                    }
                } else {
                    immutableResult = checkImmutableIndices(request, request, listener, actionName, task, auditLog);
                    if (immutableResult != null && immutableResult.v1() == ImmutableState.FAILURE) {
                        return;
                    }

                    if (immutableResult != null && immutableResult.v1() == ImmutableState.LISTENER) {
                        listener = immutableResult.v2();
                    }
                }
            }

            if (Origin.LOCAL.toString().equals(threadContext.getTransient(ConfigConstants.SG_ORIGIN))
                    && (interClusterRequest || HeaderHelper.isDirectRequest(threadContext) && (specialPrivilegesEvaluationContext == null
                            || !specialPrivilegesEvaluationContext.requiresPrivilegeEvaluationForLocalRequests()))) {

                chain.proceed(task, actionName, request, listener);
                return;
            }

            if (user == null) {

                if (actionName.startsWith("cluster:monitor/state")) {
                    chain.proceed(task, actionName, request, listener);
                    return;
                }

                log.error("No user found for " + actionName + " from " + request.remoteAddress() + " "
                        + threadContext.getTransient(ConfigConstants.SG_ORIGIN) + " via "
                        + threadContext.getTransient(ConfigConstants.SG_CHANNEL_TYPE) + " " + threadContext.getHeaders());
                listener.onFailure(new OpenSearchSecurityException("No user found for " + actionName, RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            final PrivilegesEvaluator eval = evalp;

            if (!eval.isInitialized()) {
                log.error("Search Guard not initialized (SG11) for {}", actionName);
                listener.onFailure(new OpenSearchSecurityException(
                        "Search Guard not initialized (SG11) for " + actionName + ". See https://docs.search-guard.com/latest/sgadmin",
                        RestStatus.SERVICE_UNAVAILABLE));
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("Evaluate permissions for user: {}", user.getName());
            }

            ImmutableSet<String> mappedRoles = eval.getMappedRoles(user, specialPrivilegesEvaluationContext);
            PrivilegesEvaluationContext privilegesEvaluationContext = new PrivilegesEvaluationContext(user, mappedRoles, action, request,
                    eval.isDebugEnabled(), this.actionRequestIntrospector, this.resolver, specialPrivilegesEvaluationContext);
            PrivilegesEvaluationResult privilegesEvaluationResult = eval.evaluate(user, mappedRoles, actionName, request, task,
                    privilegesEvaluationContext, specialPrivilegesEvaluationContext);

            if (privilegesEvaluationResult.isOk()) {
                auditLog.logGrantedPrivileges(actionName, request, task);
                // save username fo later use on current node
                if (threadContext.getHeader(ConfigConstants.SG_USER_NAME) == null) {
                    threadContext.putHeader(ConfigConstants.SG_USER_NAME, user.getName());
                }

                if (!dlsFlsValve.invoke(user, mappedRoles, actionName, request, listener,
                        complianceConfig != null && complianceConfig.isLocalHashingEnabled(),
                        privilegesEvaluationContext.getRequestInfo().getResolvedIndices(), specialPrivilegesEvaluationContext)) {
                    return;
                }
                
                if (privilegesEvaluationResult.hasAdditionalActionFilters()) {
                    chain = new ExtendedActionFilterChain<Request, Response>(privilegesEvaluationResult.getAdditionalActionFilters(), chain);
                }
                                
                WellKnownAction<Request, ?, ?> wellKnownAction = action.wellKnown(request);
                
                if (wellKnownAction != null && wellKnownAction.requiresSpecialProcessing()) {
                    extendedActionHandlingService.apply(wellKnownAction, task, actionName, privilegesEvaluationContext, request, listener, chain);
                } else {
                    chain.proceed(task, actionName, request, listener);
                }
                return;
            } else {
                auditLog.logMissingPrivileges(actionName, request, task);
                listener.onFailure(privilegesEvaluationResult.toSecurityException(privilegesEvaluationContext));
                return;
            }
        } catch (Exception e) {
            log.error("Exception while handling " + actionName + "; " + request, e);
            listener.onFailure(e);
        } catch (Throwable e) {
            log.error("Throwable while handling " + actionName + "; " + request, e);
            listener.onFailure(new RuntimeException(e));
        }
    }

    private static boolean isUserAdmin(User user, final AdminDNs adminDns) {
        if (user != null && adminDns.isAdmin(user)) {
            return true;
        }

        return false;
    }

    private void attachSourceFieldContext(ActionRequest request) {

        if (request instanceof SearchRequest && SourceFieldsContext.isNeeded((SearchRequest) request)) {
            if (threadContext.getHeader("_sg_source_field_context") == null) {
                final String serializedSourceFieldContext = Base64Helper.serializeObject(new SourceFieldsContext((SearchRequest) request));
                threadContext.putHeader("_sg_source_field_context", serializedSourceFieldContext);
            }
        } else if (request instanceof GetRequest && SourceFieldsContext.isNeeded((GetRequest) request)) {
            if (threadContext.getHeader("_sg_source_field_context") == null) {
                final String serializedSourceFieldContext = Base64Helper.serializeObject(new SourceFieldsContext((GetRequest) request));
                threadContext.putHeader("_sg_source_field_context", serializedSourceFieldContext);
            }
        }
    }

    private static class ImmutableIndexActionListener<Response> implements ActionListener<Response> {

        private final ActionListener<Response> originalListener;
        private final AuditLog auditLog;
        private TransportRequest originalRequest;
        private String action;
        private Task task;

        public ImmutableIndexActionListener(ActionListener<Response> originalListener, AuditLog auditLog, TransportRequest originalRequest, String action,
                Task task) {
            super();
            this.originalListener = originalListener;
            this.auditLog = auditLog;
            this.originalRequest = originalRequest;
            this.action = action;
            this.task = task;
        }

        @Override
        public void onResponse(Response response) {
            originalListener.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {

            if (e instanceof VersionConflictEngineException) {
                auditLog.logImmutableIndexAttempt(originalRequest, action, task);
                originalListener.onFailure(new OpenSearchSecurityException("Index is immutable", RestStatus.FORBIDDEN));
            } else {
                originalListener.onFailure(e);
            }
        }
    }

    private enum ImmutableState {
        FAILURE, LISTENER
    }

    @SuppressWarnings("rawtypes")
    private Tuple<ImmutableState, ActionListener> checkImmutableIndices(Object request, TransportRequest originalRequest,
            ActionListener originalListener, String action, Task task, AuditLog auditLog) {

        if (request instanceof DeleteRequest || request instanceof UpdateRequest || request instanceof UpdateByQueryRequest
                || request instanceof DeleteByQueryRequest || request instanceof DeleteIndexRequest || request instanceof RestoreSnapshotRequest
                || request instanceof CloseIndexRequest || request instanceof IndicesAliasesRequest //TODO only remove index
        ) {

            if (complianceConfig != null && complianceConfig.isIndexImmutable(action, request)) {

                //check index for type = remove index
                //IndicesAliasesRequest iar = (IndicesAliasesRequest) request;
                //for(AliasActions aa: iar.getAliasActions()) {
                //    if(aa.actionType() == Type.REMOVE_INDEX) {

                //    }
                //}

                auditLog.logImmutableIndexAttempt(originalRequest, action, task);

                originalListener.onFailure(new OpenSearchSecurityException("Index is immutable", RestStatus.FORBIDDEN));
                return new Tuple<ImmutableState, ActionListener>(ImmutableState.FAILURE, originalListener);
            }
        }

        if (request instanceof IndexRequest) {
            if (complianceConfig != null && complianceConfig.isIndexImmutable(action, request)) {
                ((IndexRequest) request).opType(OpType.CREATE);
                return new Tuple<ImmutableState, ActionListener>(ImmutableState.LISTENER,
                        new ImmutableIndexActionListener(originalListener, auditLog, originalRequest, action, task));
            }
        }

        return null;
    }
    

}