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

package com.floragunn.searchguard.auditlog.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.index.engine.Engine.Delete;
import org.opensearch.index.engine.Engine.DeleteResult;
import org.opensearch.index.engine.Engine.Index;
import org.opensearch.index.engine.Engine.IndexResult;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.shard.ShardId;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;

import com.floragunn.searchguard.auditlog.routing.AuditMessageRouter;
import com.floragunn.searchguard.compliance.ComplianceConfig;
import com.floragunn.searchguard.user.UserInformation;

public final class AuditLogImpl extends AbstractAuditLog {

    private final AuditMessageRouter messageRouter;
    private final boolean enabled;

    public AuditLogImpl(final Settings settings, final Path configPath, Client clientProvider, ThreadPool threadPool,
            final IndexNameExpressionResolver resolver, final ClusterService clusterService) {
        super(settings, threadPool, resolver, clusterService);

        this.messageRouter = new AuditMessageRouter(settings, clientProvider, threadPool, configPath);
        this.enabled = messageRouter.isEnabled();

        log.info("Message routing enabled: {}", this.enabled);
    }

    @Override
    public void setComplianceConfig(ComplianceConfig complianceConfig) {
        super.setComplianceConfig(complianceConfig);
        messageRouter.setComplianceConfig(complianceConfig);
    }

    @Override
    public void close() throws IOException {
        messageRouter.close();
    }

    @Override
    protected void save(final AuditMessage msg) {
        if (enabled) {
            messageRouter.route(msg);
        }
    }

    @Override
    public void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, Task task) {
        if (enabled) {
            super.logFailedLogin(effectiveUser, sgadmin, initiatingUser, request, task);
        }
    }

    @Override
    public void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        if (enabled) {
            super.logFailedLogin(effectiveUser, sgadmin, initiatingUser, request);
        }
    }
    
    @Override
    public void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request,
    		Task task) {        
    	if (enabled) {
            super.logBlockedUser(effectiveUser, sgadmin, initiatingUser, request, task);
        }    	
    }
    
    @Override
    public void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
    	if (enabled) {
            super.logBlockedUser(effectiveUser, sgadmin, initiatingUser, request);
        }      	
    }

    @Override
    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, String action, Task task) {
        if (enabled) {
            super.logSucceededLogin(effectiveUser, sgadmin, initiatingUser, request, action, task);
        }
    }

    @Override
    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        if (enabled) {
            super.logSucceededLogin(effectiveUser, sgadmin, initiatingUser, request);
        }
    }

    @Override
    public void logMissingPrivileges(String privilege, UserInformation effectiveUser, RestRequest request) {
        if (enabled) {
            super.logMissingPrivileges(privilege, effectiveUser, request);
        }
    }

    @Override
    public void logMissingPrivileges(String privilege, TransportRequest request, Task task) {
        if (enabled) {
            super.logMissingPrivileges(privilege, request, task);
        }
    }

    @Override
    public void logGrantedPrivileges(String privilege, TransportRequest request, Task task) {
        if (enabled) {
            super.logGrantedPrivileges(privilege, request, task);
        }
    }

    @Override
    public void logBadHeaders(TransportRequest request, String action, Task task) {
        if (enabled) {
            super.logBadHeaders(request, action, task);
        }
    }

    @Override
    public void logBadHeaders(RestRequest request) {
        if (enabled) {
            super.logBadHeaders(request);
        }
    }

    @Override
    public void logBlockedIp(TransportRequest request, String action, TransportAddress remoteAddress, Task task) {
        if (enabled) {
            super.logBlockedIp(request, action, remoteAddress, task);
        }  	
    }
    
    @Override
    public void logBlockedIp(RestRequest request, InetSocketAddress remoteAddress) {
        if (enabled) {
            super.logBlockedIp(request, remoteAddress);
        }  	    	
    }    
    
    @Override
    public void logSgIndexAttempt(TransportRequest request, String action, Task task) {
        if (enabled) {
            super.logSgIndexAttempt(request, action, task);
        }
    }
    
    @Override
    public void logImmutableIndexAttempt(TransportRequest request, String action, Task task) {
        if (enabled) {
            super.logImmutableIndexAttempt(request, action, task);
        }
    }

    @Override
    public void logSSLException(TransportRequest request, Throwable t, String action, Task task) {
        if (enabled) {
            super.logSSLException(request, t, action, task);
        }
    }

    @Override
    public void logSSLException(RestRequest request, Throwable t) {
        if (enabled) {
            super.logSSLException(request, t);
        }
    }

    @Override
    public void logDocumentRead(String index, String id, ShardId shardId, Map<String, String> fieldNameValues) {
        if (enabled) {
            super.logDocumentRead(index, id, shardId, fieldNameValues);
        }
    }

    @Override
    public void logDocumentWritten(ShardId shardId, GetResult originalResult, Index currentIndex, IndexResult result) {
        if (enabled) {
            super.logDocumentWritten(shardId, originalResult, currentIndex, result);
        }
    }

    @Override
    public void logDocumentDeleted(ShardId shardId, Delete delete, DeleteResult result) {
        if (enabled) {
            super.logDocumentDeleted(shardId, delete, result);
        }
    }



}
