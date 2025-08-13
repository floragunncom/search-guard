/*
 * Copyright 2018-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.access_log.write;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.DeleteResult;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.auditlog.AuditLogConfig;

public final class ComplianceIndexingOperationListenerImpl implements IndexingOperationListener {

    private static final Logger log = LogManager.getLogger(ComplianceIndexingOperationListenerImpl.class);
    private final AuditLogConfig complianceConfig;
    private final AuditLog auditlog;
    private final GuiceDependencies guiceDependencies;

    public ComplianceIndexingOperationListenerImpl(AuditLogConfig complianceConfig, AuditLog auditlog, GuiceDependencies guiceDependencies) {
        super();
        this.complianceConfig = complianceConfig;
        this.auditlog = auditlog;
        this.guiceDependencies = guiceDependencies;
    }

    private static final class Context {
        private final GetResult getResult;

        public Context(GetResult getResult) {
            super();
            this.getResult = getResult;
        }

        public GetResult getGetResult() {
            return getResult;
        }
    }

    private static final ThreadLocal<Context> threadContext = new ThreadLocal<Context>();

    @Override
    public void postDelete(final ShardId shardId, final Delete delete, final DeleteResult result) {
        if (delete.origin() != org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY) {
            return;
        }
        
        if (isEnabled(shardId)) {
            if (result.getFailure() == null && result.isFound()) {
                auditlog.logDocumentDeleted(shardId, delete, result);
            }
        }
    }

    @Override
    public Index preIndex(final ShardId shardId, final Index index) {
        if (index.origin() != org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY) {
            return index;
        }
        
        if (isEnabled(shardId) && complianceConfig.logDiffsForWrite()) {            
            IndexShard shard = getIndexShard(shardId);
    
            if (shard == null) {
                return index;
            }
    
            if (shard.isReadAllowed()) {
                try {
    
                    final GetResult getResult = shard.getService().getForUpdate(index.id(),
                            index.getIfSeqNo(), index.getIfPrimaryTerm(), null); // TODO ES 9.1.x is null a gFields ok here?
    
                    if (getResult.isExists()) {
                        threadContext.set(new Context(getResult));
                    } else {
                        threadContext.set(new Context(null));
                    }
                } catch (Exception e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Cannot retrieve original document due to {}", e.toString());
                    }
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Cannot read from shard {}", shardId);
                }
            }
        }
        
        return index;
    }


    @Override
    public void postIndex(final ShardId shardId, final Index index, final Exception ex) {
        if(complianceConfig.isEnabled() && complianceConfig.logDiffsForWrite()) {
            threadContext.remove();
        }
    }

    @Override
    public void postIndex(ShardId shardId, Index index, IndexResult result) {
        if (isEnabled(shardId)) {
            if (complianceConfig.logDiffsForWrite()) {
                final Context context = threadContext.get();
                final GetResult previousContent = context == null ? null : context.getGetResult();
                threadContext.remove();

                if (result.getFailure() != null || index.origin() != org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY) {
                    return;
                }

                if (previousContent == null) {
                    //no previous content
                    if (!result.isCreated()) {
                        log.warn("No previous content and not created (its an update but do not find orig source) for {}",
                                index.startTime() + "/" + shardId + "/" + index.id());
                    }
                    assert result.isCreated() : "No previous content and not created";
                } else {
                    if (result.isCreated()) {
                        log.warn("Previous content and created for {}", index.startTime() + "/" + shardId + "/" + index.id());
                    }
                    assert !result.isCreated() : "Previous content and created";
                }

                auditlog.logDocumentWritten(shardId, previousContent, index, result);
                
            } else { // logDiffsForWrite() == false                
                if (result.getFailure() != null || index.origin() != org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY) {
                    return;
                }

                auditlog.logDocumentWritten(shardId, null, index, result);
            }
        }
    }
    
    private boolean isEnabled(ShardId shardId) {
        return complianceConfig.isEnabled() && complianceConfig.writeHistoryEnabledForIndex(shardId.getIndex().getName());
    }
    
    private IndexShard getIndexShard(ShardId shardId) {
        IndexService indexService = this.guiceDependencies.getIndicesService().indexServiceSafe(shardId.getIndex());
        
        return indexService.getShardOrNull(shardId.getId());
    }

}
