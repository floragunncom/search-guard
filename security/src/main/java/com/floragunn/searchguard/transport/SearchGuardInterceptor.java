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

package com.floragunn.searchguard.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor.AsyncSender;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.google.common.collect.Maps;

public class SearchGuardInterceptor {

    protected final Logger actionTrace = LogManager.getLogger("sg_action_trace");
    protected final static Logger log = LogManager.getLogger(SearchGuardInterceptor.class);
    private AuditLog auditLog;
    private final ThreadPool threadPool;
    private final PrincipalExtractor principalExtractor;
    private final InterClusterRequestEvaluator requestEvalProvider;
    private final ClusterService cs;
    private final SslExceptionHandler sslExceptionHandler;
    private final ClusterInfoHolder clusterInfoHolder;
    private final DiagnosticContext diagnosticContext;
    private final GuiceDependencies guiceDependencies;
    private final AdminDNs adminDns;

    public SearchGuardInterceptor(Settings settings, ThreadPool threadPool,AuditLog auditLog,
            PrincipalExtractor principalExtractor, InterClusterRequestEvaluator requestEvalProvider, ClusterService cs,
            SslExceptionHandler sslExceptionHandler, ClusterInfoHolder clusterInfoHolder, GuiceDependencies guiceDependencies,
            DiagnosticContext diagnosticContext, AdminDNs adminDns) {
        this.auditLog = auditLog;
        this.threadPool = threadPool;
        this.principalExtractor = principalExtractor;
        this.requestEvalProvider = requestEvalProvider;
        this.cs = cs;
        this.sslExceptionHandler = sslExceptionHandler;
        this.clusterInfoHolder = clusterInfoHolder;
        this.diagnosticContext = diagnosticContext;
        this.guiceDependencies = guiceDependencies;
        this.adminDns = adminDns;
    }

    public <T extends TransportRequest> SearchGuardRequestHandler<T> getHandler(String action,
            TransportRequestHandler<T> actualHandler) {
        return new SearchGuardRequestHandler<T>(action, actualHandler, threadPool, auditLog,
                principalExtractor, requestEvalProvider, cs, sslExceptionHandler, adminDns);
    }

    public <T extends TransportResponse> void sendRequestDecorate(AsyncSender sender, Connection connection, String action,
            TransportRequest request, TransportRequestOptions options, TransportResponseHandler<T> handler) {

            getThreadContext().putHeader("_sg_remotecn", cs.getClusterName().value());

            
            RemoteClusterService remoteClusterService = guiceDependencies.getTransportService().getRemoteClusterService();
                        
            if (remoteClusterService.isCrossClusterSearchEnabled() 
                    && clusterInfoHolder.isInitialized()
                    && (action.equals(TransportClusterSearchShardsAction.TYPE.name())
                            || action.equals(TransportSearchAction.NAME)
)
                    && !clusterInfoHolder.hasNode(connection.getNode())) {
                if (log.isDebugEnabled()) {
                    log.debug("remove dls/fls/mf because we sent a ccs request to a remote cluster");
                }
            }
            
            if (remoteClusterService.isCrossClusterSearchEnabled() 
                  && clusterInfoHolder.isInitialized()
                  && !action.startsWith("internal:") 
                  && !action.equals(TransportClusterSearchShardsAction.TYPE.name())
                  && !clusterInfoHolder.hasNode(connection.getNode())) {
                
                if (log.isDebugEnabled()) {
                    log.debug("add dls/fls/mf from transient");
                }

                getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" IC -> "+action+" "+getThreadContext().getHeaders().entrySet().stream().filter(p->!p.getKey().startsWith("_sg_trace")).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));
            }

            sender.sendRequest(connection, action, request, options, handler);

    }

    private ThreadContext getThreadContext() {
        return threadPool.getThreadContext();
    }

}
