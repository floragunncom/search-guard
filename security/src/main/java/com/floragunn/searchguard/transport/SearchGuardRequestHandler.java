/*
 * Copyright 2015-2021 floragunn GmbH
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

package com.floragunn.searchguard.transport;

import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.stream.Collectors;

import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationResult;
import com.floragunn.searchguard.authz.PrivilegesEvaluator;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.transport.SearchGuardSSLRequestHandler;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.SearchGuardContext;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;

public class SearchGuardRequestHandler<T extends TransportRequest> extends SearchGuardSSLRequestHandler<T> {

    /**
     * Actions which are handled by plain transport request handlers instead of TransportActions. Such requests never pass
     * SearchGuardFilter (which is an ActionFilter), so privileges are evaluated here, on the receiving node.
     *
     * ES|QL sends the shard-level data requests this way. The top-level indices:data/read/esql request does not know
     * which indices it is going to read (the query might reference views), thus the requests which actually read the
     * shards are the ones which have to be authorized:
     *
     * - indices:data/read/esql/data: Reads the shards of the referenced indices on the data nodes.
     * - indices:data/read/esql/cluster: Received by a remote cluster for the remote part of a cross cluster query. The remote
     *   cluster then sends indices:data/read/esql/data requests to its own data nodes, which are checked as well.
     * - indices:data/read/esql/lookup_from_index: Reads the lookup index of a LOOKUP JOIN on the node holding its shard.
     * - cluster:monitor/xpack/enrich/esql/resolve_policy: Resolves the enrich policies referenced by ENRICH. This is the only
     *   request of the ENRICH flow which is executed in the context of the user; the actual lookup in the .enrich-* indices
     *   is executed by Elasticsearch as internal user (like with X-Pack security, which requires the monitor_enrich cluster
     *   privilege for the policy resolution).
     */
    private static final String ESQL_CLUSTER_ACTION = "indices:data/read/esql/cluster";

    static final ImmutableSet<String> TRANSPORT_LEVEL_AUTHORIZED_ACTIONS = ImmutableSet.of("indices:data/read/esql/data",
            ESQL_CLUSTER_ACTION, "indices:data/read/esql/lookup_from_index", "cluster:monitor/xpack/enrich/esql/resolve_policy");

    protected final Logger actionTrace = LogManager.getLogger("sg_action_trace");
    private final AuditLog auditLog;
    private final InterClusterRequestEvaluator requestEvalProvider;
    private final ClusterService cs;
    private final AdminDNs adminDns;
    private final PrivilegesEvaluator privilegesEvaluator;
    private final AuthorizationService authorizationService;
    private final Actions actions;
    private final ActionRequestIntrospector actionRequestIntrospector;
    private final AuthInfoService authInfoService;

    SearchGuardRequestHandler(String action,
            final TransportRequestHandler<T> actualHandler,
            final ThreadPool threadPool,
            final AuditLog auditLog,
            final PrincipalExtractor principalExtractor,
            final InterClusterRequestEvaluator requestEvalProvider,
            final ClusterService cs,
            final SslExceptionHandler sslExceptionHandler,  AdminDNs adminDns,
            final PrivilegesEvaluator privilegesEvaluator,
            final AuthorizationService authorizationService,
            final Actions actions,
            final ActionRequestIntrospector actionRequestIntrospector,
            final AuthInfoService authInfoService) {
        super(action, actualHandler, threadPool, principalExtractor, sslExceptionHandler);
        this.auditLog = auditLog;
        this.requestEvalProvider = requestEvalProvider;
        this.cs = cs;
        this.adminDns = adminDns;
        this.privilegesEvaluator = privilegesEvaluator;
        this.authorizationService = authorizationService;
        this.actions = actions;
        this.actionRequestIntrospector = actionRequestIntrospector;
        this.authInfoService = authInfoService;
    }

    @Override
    protected void messageReceivedDecorate(T request, final TransportRequestHandler<T> handler,
            final TransportChannel transportChannel, Task task) throws Exception {
        
        String resolvedActionClass = request.getClass().getSimpleName();
        
        if(request instanceof BulkShardRequest) {
            if(((BulkShardRequest) request).items().length == 1) {
                resolvedActionClass = ((BulkShardRequest) request).items()[0].request().getClass().getSimpleName();
            }
        }
        
        if(request instanceof ConcreteShardRequest) {
            resolvedActionClass = ((ConcreteShardRequest<?>) request).getRequest().getClass().getSimpleName();
        }
                
        String initialActionClassValue = getThreadContext().getHeader(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER);
        
        final ThreadContext.StoredContext sgContext = getThreadContext().newStoredContext();

        SearchGuardContext.getOrigin(getThreadContext());

        DiagnosticContext.fixupLoggingContext(getThreadContext());        
        
        try {

           boolean isDirectChannel = isDirectChannelDeep(transportChannel);

           getThreadContext().putTransient(ConfigConstants.SG_CHANNEL_TYPE, isDirectChannel? "direct": "transport");
           getThreadContext().putTransient(ConfigConstants.SG_ACTION_NAME, task.getAction());
           
           if(request instanceof ShardSearchRequest) {
               ShardSearchRequest sr = ((ShardSearchRequest) request);
               if(sr.source() != null && sr.source().suggest() != null
                       && getThreadContext().getHeader(ConfigConstants.SG_IS_SUGGEST_HEADER) == null) {
                   getThreadContext().putHeader(ConfigConstants.SG_IS_SUGGEST_HEADER, "true");
               }
           }

            //bypass non-netty requests
            if(isDirectChannel) {
                SearchGuardContext.initializeTransientCaches(getThreadContext());

                if(actionTrace.isTraceEnabled()) {
                    getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID(), Thread.currentThread().getName()+" DIR -> "+transportChannel+" "+getThreadContext().getHeaders());
                }
                
                putInitialActionClassHeader(initialActionClassValue, resolvedActionClass);

                if (!authorizeTransportLevel(request, transportChannel, task)) {
                    return;
                }

                super.messageReceivedDecorate(request, handler, transportChannel, task);
                return;
            }

            //if the incoming request is an internal:* or a shard request allow only if request was sent by a server node
            //if transport channel is not a netty channel but a direct or local channel (e.g. send via network) then allow it (regardless of beeing a internal: or shard request)
            //also allow when issued from a remote cluster for cross cluster search
            if ( !HeaderHelper.isInterClusterRequest(getThreadContext())
                    && !HeaderHelper.isTrustedClusterRequest(getThreadContext())
                    && !task.getAction().equals("internal:transport/handshake")
                    && (task.getAction().startsWith("internal:") || task.getAction().contains("["))) {

                auditLog.logMissingPrivileges(task.getAction(), request, task);
                log.error("Internal or shard requests ("+task.getAction()+") not allowed from a non-server node for transport type "+transportChannel);
                transportChannel.sendResponse(new ElasticsearchSecurityException(
                        "Internal or shard requests not allowed from a non-server node for transport type "+transportChannel));
                return;
            }


            String principal = null;

            if ((principal = getThreadContext().getTransient(SSLConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL)) == null) {
                Exception ex = new ElasticsearchSecurityException(
                        "No SSL client certificates found for transport type "+transportChannel+". Search Guard needs the Search Guard SSL plugin to be installed");
                auditLog.logSSLException(request, ex, task.getAction(), task);
                log.error("No SSL client certificates found for transport type "+transportChannel+". Search Guard needs the Search Guard SSL plugin to be installed");
                transportChannel.sendResponse(ex);
                return;
            } else {

                if(SearchGuardContext.getOrigin(getThreadContext()) == null) {
                    SearchGuardContext.setOrigin(getThreadContext(), Origin.TRANSPORT.toString());
                }

                //network intercluster request or cross search cluster request
                if(HeaderHelper.isInterClusterRequest(getThreadContext())
                        || HeaderHelper.isTrustedClusterRequest(getThreadContext())) {

                    SearchGuardContext.initializeTransientCaches(getThreadContext());

                    if(SearchGuardContext.getRemoteAddress(getThreadContext()) == null) {
                        SearchGuardContext.setRemoteAddress(getThreadContext(), new TransportAddress(request.remoteAddress()));
                    }

                } else {

                    //this is a netty request from a non-server node (maybe also be internal: or a shard request)
                    //and therefore issued by a transport client

                    User origPKIUser = new User(principal, AuthDomainInfo.TLS_CERT);

                    if (adminDns.isAdmin(origPKIUser)) {
                        auditLog.logSucceededLogin(origPKIUser, true, null, request, task.getAction(), task);
                        org.apache.logging.log4j.ThreadContext.put("user", origPKIUser.getName());
                        SearchGuardContext.setUser(getThreadContext(), origPKIUser);
                        SearchGuardContext.setRemoteAddress(getThreadContext(), new TransportAddress(request.remoteAddress()));
                    } else {
                        Exception e = new ElasticsearchSecurityException("Transport request from untrusted node denied", RestStatus.FORBIDDEN);
                        log.warn("Transport request from untrusted node denied. Check your trusted node configuration.", e);
                        auditLog.logBadHeaders(request, task.getAction(), task);
                        transportChannel.sendResponse(e);
                        return;
                    }           
                }

                if(actionTrace.isTraceEnabled()) {
                    getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" NETTI -> "+transportChannel+" "+getThreadContext().getHeaders().entrySet().stream().filter(p->!p.getKey().startsWith("_sg_trace")).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));
                }

                
                putInitialActionClassHeader(initialActionClassValue, resolvedActionClass);
                             
                if (!authorizeTransportLevel(request, transportChannel, task)) {
                    return;
                }

                super.messageReceivedDecorate(request, handler, transportChannel, task);
            }
        } finally {

            if(actionTrace.isTraceEnabled()) {
                getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" FIN -> "+transportChannel+" "+getThreadContext().getHeaders());
            }

            if(sgContext != null) {
                sgContext.close();
            }
        }
    }
    
    /**
     * Evaluates privileges for the actions listed in TRANSPORT_LEVEL_AUTHORIZED_ACTIONS. Returns true if the request may
     * proceed. If false is returned, a response has already been sent to the transport channel.
     *
     * Note: The SyncAuthorizationFilters provided by modules are intentionally not applied here. DLS/FLS for the ES|QL
     * data requests is enforced on shard level by the DLS/FLS DirectoryReader wrapper.
     */
    private boolean authorizeTransportLevel(T request, TransportChannel transportChannel, Task task) {
        String actionName = task.getAction();

        if (!TRANSPORT_LEVEL_AUTHORIZED_ACTIONS.contains(actionName)) {
            return true;
        }

        try {
            User user = SearchGuardContext.getUser(getThreadContext());

            if (user == null) {
                log.error("No user found for {} from {} via {}", actionName, request.remoteAddress(), transportChannel);
                auditLog.logMissingPrivileges(actionName, request, task);
                transportChannel.sendResponse(new ElasticsearchSecurityException("No user found for " + actionName, RestStatus.FORBIDDEN));
                return false;
            }

            if (adminDns.isAdmin(user)) {
                auditLog.logGrantedPrivileges(actionName, request, task);
                return true;
            }

            if (!privilegesEvaluator.isInitialized()) {
                log.error("Search Guard not initialized (SG11) for {}", actionName);
                transportChannel.sendResponse(new ElasticsearchSecurityException(
                        "Search Guard not initialized (SG11) for " + actionName + ". See https://docs.search-guard.com/latest/sgctl",
                        RestStatus.SERVICE_UNAVAILABLE));
                return false;
            }

            if (request instanceof IndicesRequest indicesRequest && indicesRequest.indices() != null && indicesRequest.indices().length == 0) {
                // ES|QL sends data node requests without indices (and without shards) when reading from external data sources.
                // There are no indices to protect in this case. Without this check, an empty indices array would be
                // interpreted as a request for all indices.
                if (log.isDebugEnabled()) {
                    log.debug("{} does not target any indices; skipping index privilege evaluation", actionName);
                }
                return true;
            }

            TransportRequest requestToEvaluate = request;

            if (ESQL_CLUSTER_ACTION.equals(actionName) && request instanceof IndicesRequest.Replaceable replaceableRequest) {
                // ClusterComputeRequest does not declare includeDataStreams(), but cross cluster ES|QL queries can address
                // data streams. Thus, we evaluate the request as if it would include data streams.
                requestToEvaluate = new IndicesRequestIncludingDataStreams(replaceableRequest);
            }

            SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = authInfoService.getSpecialPrivilegesEvaluationContext();
            ImmutableSet<String> mappedRoles = authorizationService.getMappedRoles(user, specialPrivilegesEvaluationContext);
            Action action = actions.get(actionName);
            PrivilegesEvaluationContext privilegesEvaluationContext = new PrivilegesEvaluationContext(user, false, mappedRoles, action,
                    requestToEvaluate, privilegesEvaluator.isDebugEnabled(), actionRequestIntrospector, specialPrivilegesEvaluationContext);

            PrivilegesEvaluationResult result = privilegesEvaluator.evaluate(user, mappedRoles, actionName, requestToEvaluate, task,
                    privilegesEvaluationContext, specialPrivilegesEvaluationContext);

            if (result.isOk()) {
                auditLog.logGrantedPrivileges(actionName, request, task);
                return true;
            } else {
                auditLog.logMissingPrivileges(actionName, request, task);
                transportChannel.sendResponse(result.toSecurityException(privilegesEvaluationContext));
                return false;
            }
        } catch (Exception e) {
            log.error("Unexpected exception while evaluating privileges for " + actionName, e);
            transportChannel.sendResponse(new ElasticsearchSecurityException(
                    "Unexpected exception while evaluating privileges for " + actionName, RestStatus.INTERNAL_SERVER_ERROR));
            return false;
        }
    }

    /**
     * Wraps an IndicesRequest.Replaceable for privilege evaluation in order to let it include data streams. Index reductions
     * (ignore_unauthorized_indices) are passed on to the wrapped request.
     */
    static class IndicesRequestIncludingDataStreams extends AbstractTransportRequest implements IndicesRequest.Replaceable {
        private final IndicesRequest.Replaceable delegate;

        IndicesRequestIncludingDataStreams(IndicesRequest.Replaceable delegate) {
            this.delegate = delegate;
        }

        @Override
        public String[] indices() {
            return delegate.indices();
        }

        @Override
        public IndicesRequest indices(String... indices) {
            delegate.indices(indices);
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return delegate.indicesOptions();
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return delegate.allowsRemoteIndices();
        }

        @Override
        public String toString() {
            return "IndicesRequestIncludingDataStreams[" + delegate + "]";
        }
    }

    private void putInitialActionClassHeader(String initialActionClassValue, String resolvedActionClass) {
        if(initialActionClassValue == null) {
            if(getThreadContext().getHeader(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER) == null) {
                getThreadContext().putHeader(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER, resolvedActionClass);
            }
        } else {
            if(getThreadContext().getHeader(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER) == null) {
                getThreadContext().putHeader(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER, initialActionClassValue);
            }
        }

    }

    @Override
    protected void addAdditionalContextValues(final String action, final TransportRequest request, final X509Certificate[] localCerts, final X509Certificate[] peerCerts, final String principal)
            throws Exception {

        boolean isInterClusterRequest = requestEvalProvider.isInterClusterRequest(request, localCerts, peerCerts, principal);

        if (isInterClusterRequest) {
            if(cs.getClusterName().value().equals(getThreadContext().getHeader("_sg_remotecn"))) {

                if (log.isTraceEnabled() && !action.startsWith("internal:")) {
                    log.trace("Is inter cluster request ({}/{}/{})", action, request.getClass(), request.remoteAddress());
                }

                getThreadContext().putTransient(SSLConfigConstants.SG_SSL_TRANSPORT_INTERCLUSTER_REQUEST, Boolean.TRUE);
            } else {
                getThreadContext().putTransient(SSLConfigConstants.SG_SSL_TRANSPORT_TRUSTED_CLUSTER_REQUEST, Boolean.TRUE);
            }

        } else {
            if (log.isTraceEnabled()) {
                log.trace("Is not an inter cluster request");
            }
        }

        super.addAdditionalContextValues(action, request, localCerts, peerCerts, principal);
    }
}
