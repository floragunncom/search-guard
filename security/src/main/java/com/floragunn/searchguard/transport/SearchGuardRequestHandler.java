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

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.ssl.SslExceptionHandler;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.ssl.transport.SearchGuardSSLRequestHandler;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.google.common.base.Strings;

public class SearchGuardRequestHandler<T extends TransportRequest> extends SearchGuardSSLRequestHandler<T> {

    protected final Logger actionTrace = LogManager.getLogger("sg_action_trace");
    private final AuditLog auditLog;
    private final InterClusterRequestEvaluator requestEvalProvider;
    private final ClusterService cs;
    private final AdminDNs adminDns;

    SearchGuardRequestHandler(String action,
            final TransportRequestHandler<T> actualHandler,
            final ThreadPool threadPool,
            final AuditLog auditLog,
            final PrincipalExtractor principalExtractor,
            final InterClusterRequestEvaluator requestEvalProvider,
            final ClusterService cs,
            final SslExceptionHandler sslExceptionHandler,  AdminDNs adminDns) {
        super(action, actualHandler, threadPool, principalExtractor, sslExceptionHandler);
        this.auditLog = auditLog;
        this.requestEvalProvider = requestEvalProvider;
        this.cs = cs;
        this.adminDns = adminDns;
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

        final String originHeader = getThreadContext().getHeader(ConfigConstants.SG_ORIGIN_HEADER);

        if(!Strings.isNullOrEmpty(originHeader)) {
            getThreadContext().putTransient(ConfigConstants.SG_ORIGIN, originHeader);
        }

        DiagnosticContext.fixupLoggingContext(getThreadContext());        
        
        try {

           if(transportChannel.getChannelType() == null) {
               throw new RuntimeException("Can not determine channel type (null)");
           }

           if(!transportChannel.getChannelType().equals("direct") && !transportChannel.getChannelType().equals("transport")) {
               throw new RuntimeException("Unknown channel type "+transportChannel.getChannelType());
           }

           getThreadContext().putTransient(ConfigConstants.SG_CHANNEL_TYPE, transportChannel.getChannelType());
           getThreadContext().putTransient(ConfigConstants.SG_ACTION_NAME, task.getAction());
           
           if(request instanceof ShardSearchRequest) {
               ShardSearchRequest sr = ((ShardSearchRequest) request);
               if(sr.source() != null && sr.source().suggest() != null) {
                   getThreadContext().putTransient("_sg_issuggest", Boolean.TRUE);
               }
           }

            //bypass non-netty requests
            if(transportChannel.getChannelType().equals("direct")) {
                final String userHeader = getThreadContext().getHeader(ConfigConstants.SG_USER_HEADER);

                if(!Strings.isNullOrEmpty(userHeader)) {
                    getThreadContext().putTransient(ConfigConstants.SG_USER, Objects.requireNonNull((User) Base64Helper.deserializeObject(userHeader)));
                }

                final String originalRemoteAddress = getThreadContext().getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER);

                if(!Strings.isNullOrEmpty(originalRemoteAddress)) {
                    getThreadContext().putTransient(ConfigConstants.SG_REMOTE_ADDRESS, new TransportAddress((InetSocketAddress) Base64Helper.deserializeObject(originalRemoteAddress)));
                }

                if(actionTrace.isTraceEnabled()) {
                    getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" DIR -> "+transportChannel.getChannelType()+" "+getThreadContext().getHeaders());
                }
                
                putInitialActionClassHeader(initialActionClassValue, resolvedActionClass);

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
                log.error("Internal or shard requests ("+task.getAction()+") not allowed from a non-server node for transport type "+transportChannel.getChannelType());
                transportChannel.sendResponse(new ElasticsearchSecurityException(
                        "Internal or shard requests not allowed from a non-server node for transport type "+transportChannel.getChannelType()));
                return;
            }


            String principal = null;

            if ((principal = getThreadContext().getTransient(ConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL)) == null) {
                Exception ex = new ElasticsearchSecurityException(
                        "No SSL client certificates found for transport type "+transportChannel.getChannelType()+". Search Guard needs the Search Guard SSL plugin to be installed");
                auditLog.logSSLException(request, ex, task.getAction(), task);
                log.error("No SSL client certificates found for transport type "+transportChannel.getChannelType()+". Search Guard needs the Search Guard SSL plugin to be installed");
                transportChannel.sendResponse(ex);
                return;
            } else {

                if(getThreadContext().getTransient(ConfigConstants.SG_ORIGIN) == null) {
                    getThreadContext().putTransient(ConfigConstants.SG_ORIGIN, Origin.TRANSPORT.toString());
                }

                //network intercluster request or cross search cluster request
                if(HeaderHelper.isInterClusterRequest(getThreadContext())
                        || HeaderHelper.isTrustedClusterRequest(getThreadContext())) {

                    final String userHeader = getThreadContext().getHeader(ConfigConstants.SG_USER_HEADER);

                    if(Strings.isNullOrEmpty(userHeader)) {
                        //user can be null when a node client wants connect
                        //getThreadContext().putTransient(ConfigConstants.SG_USER, User.SG_INTERNAL);
                    } else {
                        getThreadContext().putTransient(ConfigConstants.SG_USER, Objects.requireNonNull((User) Base64Helper.deserializeObject(userHeader)));
                    }

                    String originalRemoteAddress = getThreadContext().getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER);

                    if(!Strings.isNullOrEmpty(originalRemoteAddress)) {
                        getThreadContext().putTransient(ConfigConstants.SG_REMOTE_ADDRESS, new TransportAddress((InetSocketAddress) Base64Helper.deserializeObject(originalRemoteAddress)));
                    } else {
                        getThreadContext().putTransient(ConfigConstants.SG_REMOTE_ADDRESS, new TransportAddress(request.remoteAddress()));
                    }

                } else {

                    //this is a netty request from a non-server node (maybe also be internal: or a shard request)
                    //and therefore issued by a transport client

                    User origPKIUser = new User(principal, AuthDomainInfo.TLS_CERT);

                    if (adminDns.isAdmin(origPKIUser)) {
                        auditLog.logSucceededLogin(origPKIUser, true, null, request, task.getAction(), task);
                        org.apache.logging.log4j.ThreadContext.put("user", origPKIUser.getName());
                        getThreadContext().putTransient(ConfigConstants.SG_USER, origPKIUser);
                        getThreadContext().putTransient(ConfigConstants.SG_REMOTE_ADDRESS, new TransportAddress(request.remoteAddress()));
                    } else {
                        Exception e = new ElasticsearchSecurityException("Transport request from untrusted node denied", RestStatus.FORBIDDEN);
                        log.warn("Transport request from untrusted node denied. Check your trusted node configuration.", e);
                        auditLog.logBadHeaders(request, task.getAction(), task);
                        transportChannel.sendResponse(e);
                        return;
                    }           
                }

                if(actionTrace.isTraceEnabled()) {
                    getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" NETTI -> "+transportChannel.getChannelType()+" "+getThreadContext().getHeaders().entrySet().stream().filter(p->!p.getKey().startsWith("_sg_trace")).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));
                }

                
                putInitialActionClassHeader(initialActionClassValue, resolvedActionClass);
                             
                super.messageReceivedDecorate(request, handler, transportChannel, task);
            }
        } finally {

            if(actionTrace.isTraceEnabled()) {
                getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" FIN -> "+transportChannel.getChannelType()+" "+getThreadContext().getHeaders());
            }

            if(sgContext != null) {
                sgContext.close();
            }
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

                getThreadContext().putTransient(ConfigConstants.SG_SSL_TRANSPORT_INTERCLUSTER_REQUEST, Boolean.TRUE);
            } else {
                getThreadContext().putTransient(ConfigConstants.SG_SSL_TRANSPORT_TRUSTED_CLUSTER_REQUEST, Boolean.TRUE);
            }

        } else {
            if (log.isTraceEnabled()) {
                log.trace("Is not an inter cluster request");
            }
        }

        super.addAdditionalContextValues(action, request, localCerts, peerCerts, principal);
    }
}
