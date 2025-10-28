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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.floragunn.fluent.collections.ImmutableList;
import com.google.common.base.Predicate;
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

    // TODO check if this is needed
    private final Set<String> CSS_RELATED_REQUEST_HEADERS_NAME = Set.of(ConfigConstants.SG_DLS_QUERY_HEADER,
            ConfigConstants.SG_DLS_MODE_HEADER,
            ConfigConstants.SG_MASKED_FIELD_HEADER,
            ConfigConstants.SG_FLS_FIELDS_HEADER,
            ConfigConstants.SG_FILTER_LEVEL_DLS_DONE,
            ConfigConstants.SG_DLS_FILTER_LEVEL_QUERY_HEADER,
            ConfigConstants.SG_DOC_WHITELST_HEADER);

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

    private String printThreadContext(ThreadContext threadContext) {
        StringBuilder stringBuilder = new StringBuilder("Request headers: ");
        for (Map.Entry<String, String> entry : threadContext.getHeaders().entrySet()) {
            appendMapEntry(entry, stringBuilder);
        }
        stringBuilder.append("\nTransients: ");
        for (Map.Entry<String, Object> entry: threadContext.getTransientHeaders().entrySet()){
            appendMapEntry(entry, stringBuilder);
        }
        stringBuilder.append("\nResponse headers: ");
        for (Map.Entry<String, List<String>> entry : threadContext.getResponseHeaders().entrySet()) {
            appendMapEntry(entry, stringBuilder);
        }
        return stringBuilder.toString();
    }

    private static void appendMapEntry(Map.Entry<String, ?> entry, StringBuilder stringBuilder) {
        stringBuilder.append(entry.getKey() + " = " + entry.getValue() + ";");
    }

    public <T extends TransportRequest> SearchGuardRequestHandler<T> getHandler(String action,
            TransportRequestHandler<T> actualHandler) {
        return new SearchGuardRequestHandler<T>(action, actualHandler, threadPool, auditLog,
                principalExtractor, requestEvalProvider, cs, sslExceptionHandler, adminDns);
    }


    public <T extends TransportResponse> void sendRequestDecorate(AsyncSender sender, Connection connection, String action,
            TransportRequest request, TransportRequestOptions options, TransportResponseHandler<T> handler) {
//        log.info("Genuine context before sending request '{}': '{}'", action, printThreadContext(getThreadContext()));
        final Map<String, String> origHeaders0 = getThreadContext().getHeaders();
        final User user0 = getThreadContext().getTransient(ConfigConstants.SG_USER);
        final String origin0 = getThreadContext().getTransient(ConfigConstants.SG_ORIGIN);
        final TransportAddress remoteAdress0 = getThreadContext().getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        final String origCCSTransientDls = getThreadContext().getTransient(ConfigConstants.SG_DLS_QUERY_CCS);
        final String origCCSTransientFls = getThreadContext().getTransient(ConfigConstants.SG_FLS_FIELDS_CCS);
        final String origCCSTransientMf = getThreadContext().getTransient(ConfigConstants.SG_MASKED_FIELD_CCS);
        String actionStack = diagnosticContext.getActionStack();
//        Map<String, List<String>> responseHeaders = getThreadContext().getResponseHeaders();

        //stash headers and transient objects
        // 1. SG adds the headers ConfigConstants.SG_ACTION_NAME, ConfigConstants.SG_CHANNEL_TYPE, ConfigConstants.SG_ORIGIN in
        // SearchGuardRequestHandler.messageReceivedDecorate. Therefore, we need to remove these headers to avoid exceptions during adding them again

        // 2. ConfigConstants.SG_REMOTE_ADDRESS, ConfigConstants.SG_USER added in com.floragunn.searchguard.transport.SearchGuardRequestHandler.messageReceivedDecorate
        // for direct channel. Probably the code is not needed if we decide not to remove these headers above
        List<String> transientHeadersToClear = List.of(ConfigConstants.SG_ACTION_NAME, ConfigConstants.SG_CHANNEL_TYPE, ConfigConstants.SG_ORIGIN, ConfigConstants.SG_REMOTE_ADDRESS, ConfigConstants.SG_USER);
        // ConfigConstants.SG_ORIGIN - SG adds the header in SearchGuardRequestHandler.messageReceivedDecorate
        List<String> requestHeadersToClear = ImmutableList.of("_sg_remotecn", DiagnosticContext.ACTION_STACK_HEADER).with(CSS_RELATED_REQUEST_HEADERS_NAME);
        try (ThreadContext.StoredContext stashedContext = getThreadContext().newStoredContextPreservingResponseHeaders(transientHeadersToClear,
                requestHeadersToClear)) {
//            addResponseHeadersToContext(responseHeaders);
            final TransportResponseHandler<T> restoringHandler = new RestoringTransportResponseHandler<T>(handler, stashedContext);
            getThreadContext().putHeader("_sg_remotecn", cs.getClusterName().value());
            // used in com.floragunn.searchguard.transport.SearchGuardRequestHandler.addAdditionalContextValues
            // and then to recognize if the request is inter-cluster that is coming from the same or another cluster


            // The code below removes the following headers
//          SG_CHANNEL_TYPE
//          SG_ORIGIN   - we need to change this?
//          SG_DLS_FILTER_LEVEL_QUERY_TRANSIENT
//          SG_DLS_MODE_TRANSIENT
//          SG_DOC_WHITELST_TRANSIENT
//          SG_DLS_QUERY_CCS
//          SG_FLS_FIELDS_CCS
//          SG_MASKED_FIELD_CCS
//          SG_REMOTE_ADDRESS
//          SG_SSL_PEER_CERTIFICATES
//          SG_SSL_PRINCIPAL
//          SG_SSL_TRANSPORT_INTERCLUSTER_REQUEST - we need to remove this
//          SG_SSL_TRANSPORT_TRUSTED_CLUSTER_REQUEST - we need to remove this
//          SG_SSL_TRANSPORT_PRINCIPAL
//          SG_USER
//          SG_USER_NAME
//          SG_XFF_DONE
//          SSO_LOGOUT_URL
//          SG_ACTION_NAME
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
                    || k.equals(ConfigConstants.SG_AUTHZ_HASH_THREAD_CONTEXT_HEADER)
                    || (k.equals("_sg_source_field_context") && ! (request instanceof SearchRequest) && !(request instanceof GetRequest)) // interesting case, used by Compliance features
                    || k.startsWith("_sg_trace")
                    || k.startsWith(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER) // TODO: why we need this one?
                    || checkCustomAllowedHeader(k)
                    // --- new ---
//                    || k.equals(ConfigConstants.SG_CHANNEL_TYPE)
//                    || k.equals(ConfigConstants.SG_ORIGIN)//   - we need to change this?
//                    || k.equals(ConfigConstants.SG_DLS_FILTER_LEVEL_QUERY_TRANSIENT)
//                    || k.equals(ConfigConstants.SG_DLS_MODE_TRANSIENT)
//                    || k.equals(ConfigConstants.SG_DOC_WHITELST_TRANSIENT)
//                    || k.equals(ConfigConstants.SG_DLS_QUERY_CCS)
//                    || k.equals(ConfigConstants.SG_FLS_FIELDS_CCS)
//                    || k.equals(ConfigConstants.SG_MASKED_FIELD_CCS)
//                    || k.equals(ConfigConstants.SG_REMOTE_ADDRESS)
//                    || k.equals(ConfigConstants.SG_SSL_PEER_CERTIFICATES)
//                    || k.equals(ConfigConstants.SG_SSL_PRINCIPAL)
//                    || k.equals(ConfigConstants.SG_SSL_TRANSPORT_INTERCLUSTER_REQUEST)// - we need to remove this
//                    || k.equals(ConfigConstants.SG_SSL_TRANSPORT_TRUSTED_CLUSTER_REQUEST)// - we need to remove this
//                    || k.equals(ConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL)
//                    || k.equals(ConfigConstants.SG_USER)
//                    || k.equals(ConfigConstants.SG_USER_NAME)
//                    || k.equals(ConfigConstants.SG_XFF_DONE)
//                    || k.equals(ConfigConstants.SSO_LOGOUT_URL)
//                    || k.equals(ConfigConstants.SG_ACTION_NAME)
                    )));
            
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
//                headerMap.remove(ConfigConstants.SG_DLS_QUERY_HEADER);
//                headerMap.remove(ConfigConstants.SG_DLS_MODE_HEADER);
//                headerMap.remove(ConfigConstants.SG_MASKED_FIELD_HEADER);
//                headerMap.remove(ConfigConstants.SG_FLS_FIELDS_HEADER);
//                headerMap.remove(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE);
//                headerMap.remove(ConfigConstants.SG_DLS_FILTER_LEVEL_QUERY_HEADER);
//                headerMap.remove(ConfigConstants.SG_DOC_WHITELST_HEADER);
            } else {
                Predicate<Map.Entry<String, String>> entryPredicate = entry -> (entry.getKey() != null)
                        && (entry.getValue() != null)
                        && CSS_RELATED_REQUEST_HEADERS_NAME.contains(entry.getKey());
                final Map<String, String> cssRequestHeaderMap = new HashMap<>(Maps.filterEntries(origHeaders0, entryPredicate));
                getThreadContext().putHeader(cssRequestHeaderMap);
            }
            
            if (remoteClusterService.isCrossClusterSearchEnabled() 
                  && clusterInfoHolder.isInitialized()
                  && !action.startsWith("internal:") 
                  && !action.equals(TransportClusterSearchShardsAction.TYPE.name())
                  && !clusterInfoHolder.hasNode(connection.getNode())) {
                
                if (log.isDebugEnabled()) {
                    log.debug("add dls/fls/mf from transient");
                }
                
                if (origCCSTransientDls != null && !origCCSTransientDls.isEmpty()) {
//                    headerMap.put(ConfigConstants.SG_DLS_QUERY_HEADER, origCCSTransientDls);
                    getThreadContext().putHeader(ConfigConstants.SG_DLS_QUERY_HEADER, origCCSTransientDls);
                }
                if (origCCSTransientMf != null && !origCCSTransientMf.isEmpty()) {
//                    headerMap.put(ConfigConstants.SG_MASKED_FIELD_HEADER, origCCSTransientMf);
                    getThreadContext().putHeader(ConfigConstants.SG_MASKED_FIELD_HEADER, origCCSTransientMf);
                }
                if (origCCSTransientFls != null && !origCCSTransientFls.isEmpty()) {
//                    headerMap.put(ConfigConstants.SG_FLS_FIELDS_HEADER, origCCSTransientFls);
                    getThreadContext().putHeader(ConfigConstants.SG_FLS_FIELDS_HEADER, origCCSTransientFls);
                }
            }

            if (actionStack != null) {
                getThreadContext().putHeader(DiagnosticContext.ACTION_STACK_HEADER, actionStack);
            }

            // None of header are added to context
//            getThreadContext().putHeader(headerMap);

            ensureCorrectHeaders(remoteAdress0, user0, origin0);

            if(actionTrace.isTraceEnabled()) {
                getThreadContext().putHeader("_sg_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" IC -> "+action+" "+getThreadContext().getHeaders().entrySet().stream().filter(p->!p.getKey().startsWith("_sg_trace")).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));
            }

//            log.info("Context prepared to send request '{}' to other node '{}'", action, printThreadContext(getThreadContext()));
            sender.sendRequest(connection, action, request, options, restoringHandler);
        }
    }

    private void addResponseHeadersToContext(Map<String, List<String>> responseHeaders) {
        for (Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
            List<String> headerValues = entry.getValue();
            if (headerValues != null && !headerValues.isEmpty()) {
                for (String value : headerValues) {
                    getThreadContext().addResponseHeader(entry.getKey(), value);
                }
            }
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
        if (headerKey.startsWith(ConfigConstants.SG_CONFIG_PREFIX)) { // remove all headers related to SG config
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
         public Executor executor() {
             return innerHandler.executor();
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
