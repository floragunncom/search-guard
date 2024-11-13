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
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
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
    private final List<Pattern> customAllowedHeaderPatterns;
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
        this.customAllowedHeaderPatterns = getCustomAllowedHeaderPatterns(settings);
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
        
        final Map<String, String> origHeaders0 = getThreadContext().getHeaders();
        final User user0 = getThreadContext().getTransient(ConfigConstants.SG_USER);
        final String origin0 = getThreadContext().getTransient(ConfigConstants.SG_ORIGIN);
        final TransportAddress remoteAdress0 = getThreadContext().getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        final String origCCSTransientDls = getThreadContext().getTransient(ConfigConstants.SG_DLS_QUERY_CCS);
        final String origCCSTransientFls = getThreadContext().getTransient(ConfigConstants.SG_FLS_FIELDS_CCS);
        final String origCCSTransientMf = getThreadContext().getTransient(ConfigConstants.SG_MASKED_FIELD_CCS);
        String actionStack = diagnosticContext.getActionStack();
                  
        //stash headers and transient objects
        try (ThreadContext.StoredContext stashedContext = getThreadContext().stashContext()) {
            
            final TransportResponseHandler<T> restoringHandler = new RestoringTransportResponseHandler<T>(handler, stashedContext);
            getThreadContext().putHeader("_sg_remotecn", cs.getClusterName().value());
                        
            final Map<String, String> headerMap = new HashMap<>(Maps.filterKeys(origHeaders0, k->k!=null && (
                    k.equals(ConfigConstants.SG_CONF_REQUEST_HEADER)
                    || k.equals(ConfigConstants.SG_ORIGIN_HEADER)
                    || k.equals(ConfigConstants.SG_REMOTE_ADDRESS_HEADER)
                    || k.equals(ConfigConstants.SG_USER_HEADER)
                    || k.equals(ConfigConstants.SG_DLS_QUERY_HEADER)
                    || k.equals(ConfigConstants.SG_FLS_FIELDS_HEADER)
                    || k.equals(ConfigConstants.SG_MASKED_FIELD_HEADER)
                    || k.equals(ConfigConstants.SG_DOC_WHITELST_HEADER)
                    || k.equals(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE)
                    || k.equals(ConfigConstants.SG_DLS_MODE_HEADER)
                    || k.equals(ConfigConstants.SG_DLS_FILTER_LEVEL_QUERY_HEADER)
                    || (k.equals("_sg_source_field_context") && ! (request instanceof SearchRequest) && !(request instanceof GetRequest))
                    || k.startsWith("_sg_trace")
                    || k.startsWith(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER)
                    || checkCustomAllowedHeader(k)
                    )));
            
            RemoteClusterService remoteClusterService = guiceDependencies.getTransportService().getRemoteClusterService();
                        
            if (remoteClusterService.isCrossClusterSearchEnabled() 
                    && clusterInfoHolder.isInitialized()
                    && (action.equals(ClusterSearchShardsAction.NAME)
                            || action.equals(TransportSearchAction.NAME)
)
                    && !clusterInfoHolder.hasNode(connection.getNode())) {
                if (log.isDebugEnabled()) {
                    log.debug("remove dls/fls/mf because we sent a ccs request to a remote cluster");
                }
                headerMap.remove(ConfigConstants.SG_DLS_QUERY_HEADER);
                headerMap.remove(ConfigConstants.SG_DLS_MODE_HEADER);
                headerMap.remove(ConfigConstants.SG_MASKED_FIELD_HEADER);
                headerMap.remove(ConfigConstants.SG_FLS_FIELDS_HEADER);
                headerMap.remove(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE);
                headerMap.remove(ConfigConstants.SG_DLS_FILTER_LEVEL_QUERY_HEADER);
                headerMap.remove(ConfigConstants.SG_DOC_WHITELST_HEADER);
            }
            
            if (remoteClusterService.isCrossClusterSearchEnabled() 
                  && clusterInfoHolder.isInitialized()
                  && !action.startsWith("internal:") 
                  && !action.equals(ClusterSearchShardsAction.NAME) 
                  && !clusterInfoHolder.hasNode(connection.getNode())) {
                
                if (log.isDebugEnabled()) {
                    log.debug("add dls/fls/mf from transient");
                }
                
                if (origCCSTransientDls != null && !origCCSTransientDls.isEmpty()) {
                    headerMap.put(ConfigConstants.SG_DLS_QUERY_HEADER, origCCSTransientDls);
                }
                if (origCCSTransientMf != null && !origCCSTransientMf.isEmpty()) {
                    headerMap.put(ConfigConstants.SG_MASKED_FIELD_HEADER, origCCSTransientMf);
                }
                if (origCCSTransientFls != null && !origCCSTransientFls.isEmpty()) {
                    headerMap.put(ConfigConstants.SG_FLS_FIELDS_HEADER, origCCSTransientFls);
                }               
            }

            if (actionStack != null) {
                getThreadContext().putHeader(DiagnosticContext.ACTION_STACK_HEADER, actionStack);
            }
            
            getThreadContext().putHeader(headerMap);

            ensureCorrectHeaders(remoteAdress0, user0, origin0);

            if(actionTrace.isTraceEnabled()) {
                getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" IC -> "+action+" "+getThreadContext().getHeaders().entrySet().stream().filter(p->!p.getKey().startsWith("_sg_trace")).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));
            }

            sender.sendRequest(connection, action, request, options, restoringHandler);
        }
    }

    private void ensureCorrectHeaders(final TransportAddress remoteAdr, final User origUser, final String origin) {
        // keep original address

        if(origin != null && !origin.isEmpty() /*&& !Origin.LOCAL.toString().equalsIgnoreCase(origin)*/ && getThreadContext().getHeader(ConfigConstants.SG_ORIGIN_HEADER) == null) {
            getThreadContext().putHeader(ConfigConstants.SG_ORIGIN_HEADER, origin);
        }

        if(origin == null && getThreadContext().getHeader(ConfigConstants.SG_ORIGIN_HEADER) == null) {
            getThreadContext().putHeader(ConfigConstants.SG_ORIGIN_HEADER, Origin.LOCAL.toString());
        }

        if (remoteAdr != null) {

            String remoteAddressHeader = getThreadContext().getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER);

            if(remoteAddressHeader == null) {
                getThreadContext().putHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER, Base64Helper.serializeObject(remoteAdr.address()));
            }
        }

        if(origUser != null) {
            String userHeader = getThreadContext().getHeader(ConfigConstants.SG_USER_HEADER);

            if(userHeader == null) {
                getThreadContext().putHeader(ConfigConstants.SG_USER_HEADER, Base64Helper.serializeObject(origUser));
            }
        }
    }

    private ThreadContext getThreadContext() {
        return threadPool.getThreadContext();
    }

    private boolean checkCustomAllowedHeader(String headerKey) {        
        if (headerKey.startsWith(ConfigConstants.SG_CONFIG_PREFIX)) {
            // SG specific headers are sensitive and thus should not be externally manipulated
            return false;
        }
        
        if (headerKey.equals(Task.X_OPAQUE_ID_HTTP_HEADER)) {
            // This is included anyway. Including it again would cause an error.
            return false;
        }
        
        if (customAllowedHeaderPatterns.size() == 0) {
            return false;
        }
        
        for (Pattern pattern : customAllowedHeaderPatterns) {
            Matcher matcher = pattern.matcher(headerKey);
            
            if (matcher.matches()) {
                return true;
            }
        }
        
        return false;
    }
    
    private static List<Pattern> getCustomAllowedHeaderPatterns(Settings settings) {
        List<String> patternStrings = settings.getAsList(ConfigConstants.SEARCHGUARD_ALLOW_CUSTOM_HEADERS, Collections.emptyList());
        List<Pattern> result = new ArrayList<>(patternStrings.size());

        for (String patternString : patternStrings) {
            try {
                result.add(Pattern.compile(patternString));
            } catch (PatternSyntaxException e) {
                log.error("Invalid pattern configured in " + ConfigConstants.SEARCHGUARD_ALLOW_CUSTOM_HEADERS + ": " + patternString, e);
            }
        }

        return Collections.unmodifiableList(result);
    }

     //based on
    //org.elasticsearch.transport.TransportService.ContextRestoreResponseHandler<T>
    //which is private scoped
    private class RestoringTransportResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final ThreadContext.StoredContext contextToRestore;
        private final TransportResponseHandler<T> innerHandler;

        private RestoringTransportResponseHandler(TransportResponseHandler<T> innerHandler, ThreadContext.StoredContext contextToRestore) {
            this.contextToRestore = contextToRestore;
            this.innerHandler = innerHandler;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return innerHandler.read(in);
        }

         @Override
         public Executor executor(ThreadPool threadPool) {
             return innerHandler.executor(threadPool);
         }

         @Override
        public void handleResponse(T response) {
            ThreadContext threadContext = getThreadContext();
            Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

            List<String> flsResponseHeader = responseHeaders.get(ConfigConstants.SG_FLS_FIELDS_HEADER);
            List<String> dlsResponseHeader = responseHeaders.get(ConfigConstants.SG_DLS_QUERY_HEADER);
            List<String> maskedFieldsResponseHeader = responseHeaders.get(ConfigConstants.SG_MASKED_FIELD_HEADER);

            contextToRestore.restore();

            if (response instanceof ClusterSearchShardsResponse || response instanceof SearchShardsResponse) {
                if (flsResponseHeader != null && !flsResponseHeader.isEmpty()) {
                    threadContext.putTransient(ConfigConstants.SG_FLS_FIELDS_CCS, flsResponseHeader.get(0));
                }

                if (dlsResponseHeader != null && !dlsResponseHeader.isEmpty()) {
                    threadContext.putTransient(ConfigConstants.SG_DLS_QUERY_CCS, dlsResponseHeader.get(0));
                }

                if (maskedFieldsResponseHeader != null && !maskedFieldsResponseHeader.isEmpty()) {
                    threadContext.putTransient(ConfigConstants.SG_MASKED_FIELD_CCS, maskedFieldsResponseHeader.get(0));
                }
            }

            innerHandler.handleResponse(response);
        }

        @Override
        public void handleException(TransportException e) {
            contextToRestore.restore();
            innerHandler.handleException(e);
        }

    }

}
