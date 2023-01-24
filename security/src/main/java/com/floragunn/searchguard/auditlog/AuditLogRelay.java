/*
 * Copyright 2022 floragunn GmbH
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
package com.floragunn.searchguard.auditlog;

import com.floragunn.searchguard.user.UserInformation;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.DeleteResult;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequest;

public class AuditLogRelay implements AuditLog {
    private AuditLog auditLog = NullAuditLog.INSTANCE;

    public void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, Task task) {
        auditLog.logFailedLogin(effectiveUser, sgadmin, initiatingUser, request, task);
    }

    public void close() throws IOException {
        auditLog.close();
    }

    public void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        auditLog.logFailedLogin(effectiveUser, sgadmin, initiatingUser, request);
    }

    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request,
            String action, Task task) {
        auditLog.logSucceededLogin(effectiveUser, sgadmin, initiatingUser, request, action, task);
    }

    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        auditLog.logSucceededLogin(effectiveUser, sgadmin, initiatingUser, request);
    }

    public void logBlockedIp(TransportRequest request, String action, TransportAddress remoteAddress, Task task) {
        auditLog.logBlockedIp(request, action, remoteAddress, task);
    }

    public void logBlockedIp(RestRequest request, InetSocketAddress remoteAddress) {
        auditLog.logBlockedIp(request, remoteAddress);
    }

    public void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, Task task) {
        auditLog.logBlockedUser(effectiveUser, sgadmin, initiatingUser, request, task);
    }

    public void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        auditLog.logBlockedUser(effectiveUser, sgadmin, initiatingUser, request);
    }

    public void logMissingPrivileges(String privilege, UserInformation effectiveUser, RestRequest request) {
        auditLog.logMissingPrivileges(privilege, effectiveUser, request);
    }

    public void logMissingPrivileges(String privilege, TransportRequest request, Task task) {
        auditLog.logMissingPrivileges(privilege, request, task);
    }

    public void logGrantedPrivileges(String privilege, TransportRequest request, Task task) {
        auditLog.logGrantedPrivileges(privilege, request, task);
    }

    public void logBadHeaders(TransportRequest request, String action, Task task) {
        auditLog.logBadHeaders(request, action, task);
    }

    public void logBadHeaders(RestRequest request) {
        auditLog.logBadHeaders(request);
    }

    public void logSgIndexAttempt(TransportRequest request, String action, Task task) {
        auditLog.logSgIndexAttempt(request, action, task);
    }

    public void logImmutableIndexAttempt(TransportRequest request, String action, Task task) {
        auditLog.logImmutableIndexAttempt(request, action, task);
    }

    public void logSSLException(TransportRequest request, Throwable t, String action, Task task) {
        auditLog.logSSLException(request, t, action, task);
    }

    public void logSSLException(RestRequest request, Throwable t) {
        auditLog.logSSLException(request, t);
    }

    public void logDocumentRead(String index, String id, ShardId shardId, Map<String, String> fieldNameValues) {
        auditLog.logDocumentRead(index, id, shardId, fieldNameValues);
    }

    public void logDocumentWritten(ShardId shardId, GetResult originalIndex, Index currentIndex, IndexResult result) {
        auditLog.logDocumentWritten(shardId, originalIndex, currentIndex, result);
    }

    public void logDocumentDeleted(ShardId shardId, Delete delete, DeleteResult result) {
        auditLog.logDocumentDeleted(shardId, delete, result);
    }

    public void logExternalConfig(Settings settings, Environment environment) {
        auditLog.logExternalConfig(settings, environment);
    }

    public void setAuditLog(AuditLog auditLog) {
        if (this.auditLog == NullAuditLog.INSTANCE) {
            this.auditLog = auditLog;
        }
    }
}
