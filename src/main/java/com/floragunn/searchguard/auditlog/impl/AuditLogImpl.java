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
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.DeleteResult;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

import com.floragunn.searchguard.auditlog.routing.AuditMessageRouter;
import com.floragunn.searchguard.compliance.ComplianceConfig;

public final class AuditLogImpl extends AbstractAuditLog {

    private final AuditMessageRouter messageRouter;
    private final boolean enabled;

    public AuditLogImpl(final Settings settings, final Path configPath, Client clientProvider, ThreadPool threadPool,
            final IndexNameExpressionResolver resolver, final ClusterService clusterService) {
        super(settings, threadPool, resolver, clusterService);

        this.messageRouter = new AuditMessageRouter(settings, clientProvider, threadPool, configPath);
        this.enabled = messageRouter.isEnabled();

        log.info("Message routing enabled: {}", this.enabled);

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            log.debug("Security Manager present");
            sm.checkPermission(new SpecialPermission());
        }

        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                Runtime.getRuntime().addShutdownHook(new Thread() {

                    @Override
                    public void run() {
                        try {
                            close();
                        } catch (final IOException e) {
                            log.warn("Exception while shutting down message router", e);
                        }
                    }
                });
                log.debug("Shutdown Hook registered");
                return null;
            }
        });

    }

    @Override
    public void setComplianceConfig(ComplianceConfig complianceConfig) {
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
    public void logFailedLogin(String effectiveUser, boolean sgadmin, String initiatingUser, TransportRequest request, Task task) {
        if (enabled) {
            super.logFailedLogin(effectiveUser, sgadmin, initiatingUser, request, task);
        }
    }

    @Override
    public void logFailedLogin(String effectiveUser, boolean sgadmin, String initiatingUser, RestRequest request) {
        if (enabled) {
            super.logFailedLogin(effectiveUser, sgadmin, initiatingUser, request);
        }
    }

    @Override
    public void logSucceededLogin(String effectiveUser, boolean sgadmin, String initiatingUser, TransportRequest request, String action, Task task) {
        if (enabled) {
            super.logSucceededLogin(effectiveUser, sgadmin, initiatingUser, request, action, task);
        }
    }

    @Override
    public void logSucceededLogin(String effectiveUser, boolean sgadmin, String initiatingUser, RestRequest request) {
        if (enabled) {
            super.logSucceededLogin(effectiveUser, sgadmin, initiatingUser, request);
        }
    }

    @Override
    public void logMissingPrivileges(String privilege, String effectiveUser, RestRequest request) {
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
    public void logSgIndexAttempt(TransportRequest request, String action, Task task) {
        if (enabled) {
            super.logSgIndexAttempt(request, action, task);
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
    public void logDocumentRead(String index, String id, ShardId shardId, Map<String, String> fieldNameValues, ComplianceConfig complianceConfig) {
        if (enabled) {
            super.logDocumentRead(index, id, shardId, fieldNameValues, complianceConfig);
        }
    }

    @Override
    public void logDocumentWritten(ShardId shardId, GetResult originalResult, Index currentIndex, IndexResult result,
            ComplianceConfig complianceConfig) {
        if (enabled) {
            super.logDocumentWritten(shardId, originalResult, currentIndex, result, complianceConfig);
        }
    }

    @Override
    public void logDocumentDeleted(ShardId shardId, Delete delete, DeleteResult result) {
        if (enabled) {
            super.logDocumentDeleted(shardId, delete, result);
        }
    }

    @Override
    public void logExternalConfig(Settings settings, Environment environment) {
        if (enabled) {
            super.logExternalConfig(settings, environment);
        }
    }

}
