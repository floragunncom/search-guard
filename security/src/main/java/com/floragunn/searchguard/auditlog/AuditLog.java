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

package com.floragunn.searchguard.auditlog;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Map;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.env.Environment;
import org.opensearch.index.engine.Engine.Delete;
import org.opensearch.index.engine.Engine.DeleteResult;
import org.opensearch.index.engine.Engine.Index;
import org.opensearch.index.engine.Engine.IndexResult;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.shard.ShardId;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequest;

import com.floragunn.searchguard.compliance.ComplianceConfig;
import com.floragunn.searchguard.user.UserInformation;

public interface AuditLog extends Closeable {

    //login
    void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, Task task);
    void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request);
    void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, String action, Task task);
    void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request);

    // blocks
    void logBlockedIp(TransportRequest request, String action, TransportAddress remoteAddress, Task task);
    void logBlockedIp(RestRequest request, InetSocketAddress remoteAddress);
    void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, Task task);
    void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request);

    //privs
    void logMissingPrivileges(String privilege, UserInformation effectiveUser, RestRequest request);
    void logMissingPrivileges(String privilege, TransportRequest request, Task task);
    void logGrantedPrivileges(String privilege, TransportRequest request, Task task);

    //spoof
    void logBadHeaders(TransportRequest request, String action, Task task);
    void logBadHeaders(RestRequest request);
        
    void logSgIndexAttempt(TransportRequest request, String action, Task task);
    
    void logImmutableIndexAttempt(TransportRequest request, String action, Task task);

    void logSSLException(TransportRequest request, Throwable t, String action, Task task);
    void logSSLException(RestRequest request, Throwable t);

    void logDocumentRead(String index, String id, ShardId shardId, Map<String, String> fieldNameValues, ComplianceConfig complianceConfig);
    void logDocumentWritten(ShardId shardId, GetResult originalIndex, Index currentIndex, IndexResult result, ComplianceConfig complianceConfig);
    void logDocumentDeleted(ShardId shardId, Delete delete, DeleteResult result);
    void logExternalConfig(Settings settings, Environment environment);
    
    // compliance config
    void setComplianceConfig(ComplianceConfig complianceConfig);
    
    public enum Origin {
        REST, TRANSPORT, LOCAL
    }

    public enum Operation {
        CREATE, UPDATE, DELETE
    }
}
