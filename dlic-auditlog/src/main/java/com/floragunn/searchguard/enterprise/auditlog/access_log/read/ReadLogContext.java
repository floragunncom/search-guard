/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.access_log.read;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.auditlog.AuditLogConfig;

public class ReadLogContext {
    private final Index index;
    private final IndexService indexService;
    private final AuditLog auditLog;
    private final AuditLogConfig auditLogConfig;
    private final ThreadContext threadContext;

    ReadLogContext(IndexService indexService, AuditLog auditLog, AuditLogConfig auditLogConfig) {
        this.index = indexService.index();
        this.indexService = indexService;
        this.auditLog = auditLog;
        this.auditLogConfig = auditLogConfig;
        this.threadContext = indexService.getThreadPool().getThreadContext();
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public AuditLog getAuditLog() {
        return auditLog;
    }

    public AuditLogConfig getAuditLogConfig() {
        return auditLogConfig;
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "ReadLogContext [index=" + index + "]";
    }

}
