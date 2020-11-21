/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.configuration;

import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SgUtils;

public class DlsFlsValveImpl implements DlsFlsRequestValve {

    /**
     * 
     * @param request
     * @param listener
     * @return false on error
     */
    public boolean invoke(final ActionRequest request, final ActionListener<?> listener, 
            final Map<String,Set<String>> allowedFlsFields, 
            final Map<String,Set<String>> maskedFields, 
            final Map<String,Set<String>> queries,
            boolean localHashingEnabled) {
        
        final boolean fls = allowedFlsFields != null && !allowedFlsFields.isEmpty();
        final boolean masked = maskedFields != null && !maskedFields.isEmpty();
        final boolean dls = queries != null && !queries.isEmpty();
        
        if(fls || masked || dls) {
            
            if(request instanceof RealtimeRequest) {
                ((RealtimeRequest) request).realtime(Boolean.FALSE);
            }
            
            if(request instanceof SearchRequest) {
                
                SearchRequest sr = ((SearchRequest)request);
                
                if(localHashingEnabled && !fls && !dls && sr.source().aggregations() != null) {
                
                    boolean cacheable = true;
                    
                    for(AggregationBuilder af: sr.source().aggregations().getAggregatorFactories()) {
                        
                        if(!af.getType().equals("cardinality") && !af.getType().equals("count")) {
                            cacheable = false;
                            continue;
                        }
                        
                        StringBuffer sb = new StringBuffer();
                        //sb.append(System.lineSeparator()+af.getName()+System.lineSeparator());
                        //sb.append(af.getType()+System.lineSeparator());
                        //sb.append(af.getClass().getSimpleName()+System.lineSeparator());
                        
                        if(sr.source() != null) {
                            //sb.append(sr.source().query()+System.lineSeparator());
                            sb.append(Strings.toString(sr.source())+System.lineSeparator());
                        }
                        
                        sb.append(Strings.toString(af)+System.lineSeparator());
                        
                        LogManager.getLogger("debuglogger").error(sb.toString());
                        
                    }
                    
                    if(!cacheable) {
                        sr.requestCache(Boolean.FALSE);
                    } else {
                        LogManager.getLogger("debuglogger").error("Shard requestcache enabled for "+(sr.source()==null?"<NULL>":Strings.toString(sr.source())));
                    }
                
                } else {
                    sr.requestCache(Boolean.FALSE);
                }
            }
            
            if(request instanceof UpdateRequest) {
                listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS or DLS or Fieldmasking is activated"));
                return false;
            }
            
            if(request instanceof BulkRequest) {
                for(DocWriteRequest<?> inner:((BulkRequest) request).requests()) {
                    if(inner instanceof UpdateRequest) {
                        listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS or DLS or Fieldmasking is activated"));
                        return false;
                    }
                }
            }
            
            if(request instanceof BulkShardRequest) {
                for(BulkItemRequest inner:((BulkShardRequest) request).items()) {
                    if(inner.request() instanceof UpdateRequest) {
                        listener.onFailure(new ElasticsearchSecurityException("Update is not supported when FLS or DLS or Fieldmasking is activated"));
                        return false;
                    }
                }
            }
            
            if(request instanceof ResizeRequest) {
                listener.onFailure(new ElasticsearchSecurityException("Resize is not supported when FLS or DLS or Fieldmasking is activated"));
                return false;
            }
        }
        
        if(dls) {
            if(request instanceof SearchRequest) {
                final SearchSourceBuilder source = ((SearchRequest)request).source();
                if(source != null) {
                    
                    if(source.profile()) {
                        listener.onFailure(new ElasticsearchSecurityException("Profiling is not supported when DLS is activated"));
                        return false;
                    }
                    
                }
            }
        }
        
        return true;
    }
    
    @Override
    public void handleSearchContext(SearchContext context, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry) {
        try {
            final Map<String, Set<String>> queries = (Map<String, Set<String>>) HeaderHelper.deserializeSafeFromHeader(threadPool.getThreadContext(),
                    ConfigConstants.SG_DLS_QUERY_HEADER);

            final String dlsEval = SgUtils.evalMap(queries, context.indexShard().indexSettings().getIndex().getName());

            if (dlsEval != null) {
                
                if(context.suggest() != null) {
                    return;
                }
                
                assert context.parsedQuery() != null;
                
                final Set<String> unparsedDlsQueries = queries.get(dlsEval);
                if (unparsedDlsQueries != null && !unparsedDlsQueries.isEmpty()) {
                    final ParsedQuery dlsQuery = DlsQueryParser.parse(unparsedDlsQueries, context.parsedQuery(), context.getQueryShardContext(), namedXContentRegistry);
                    context.parsedQuery(dlsQuery);
                    context.preProcess(true);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error evaluating dls for a search query: " + e, e);
        }

    }
}
