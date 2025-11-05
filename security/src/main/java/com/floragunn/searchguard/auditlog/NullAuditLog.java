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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
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

import com.floragunn.searchguard.user.UserInformation;

public class NullAuditLog implements AuditLog {

    static final NullAuditLog INSTANCE = new NullAuditLog();
    
    @Override
    public void close() throws IOException {
        //noop, intentionally left empty
    }

    @Override
    public void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        //noop, intentionally left empty
    }

    @Override
    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, String action, Task task) {
        //noop, intentionally left empty
    }

    @Override
    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        //noop, intentionally left empty
    }

    @Override
    public void logMissingPrivileges(String privilege, TransportRequest request, Task task) {
        //noop, intentionally left empty
    }

    @Override
    public void logGrantedPrivileges(String privilege, TransportRequest request, Task task) {
        //noop, intentionally left empty
    }

    @Override
    public void logBadHeaders(TransportRequest request, String action, Task task) {
        //noop, intentionally left empty
    }

    @Override
    public void logBadHeaders(RestRequest request) {
        //noop, intentionally left empty
    }

    @Override
    public void logSgIndexAttempt(TransportRequest request, String action, Task task) {
        //noop, intentionally left empty
    }
    
    @Override
    public void logImmutableIndexAttempt(TransportRequest request, String action, Task task) {
        //noop, intentionally left empty
    }

    @Override
    public void logSSLException(TransportRequest request, Throwable t, String action, Task task) {
        //noop, intentionally left empty
    }

    @Override
    public void logSSLException(RestRequest request, Throwable t) {
        //noop, intentionally left empty
    }

    @Override
    public void logMissingPrivileges(String privilege, UserInformation effectiveUser, RestRequest request) {
        //noop, intentionally left empty
    }

    @Override
    public void logDocumentRead(String index, String id, ShardId shardId, Map<String, String> fieldNameValues) {
        //noop, intentionally left empty
    }

    @Override
    public void logDocumentWritten(ShardId shardId, GetResult originalIndex, Index currentIndex, IndexResult result) {
        //noop, intentionally left empty
    }

    @Override
    public void logDocumentDeleted(ShardId shardId, Delete delete, DeleteResult result) {
        //noop, intentionally left empty
    }

    @Override
    public void logExternalConfig(Settings settings, Environment environment) {
        //noop, intentionally left empty
    }

    @Override
    public void logIndexTemplatePutted(String templateName, ComposableIndexTemplate originalTemplate, ComposableIndexTemplate currentTemplate, String action, TransportRequest transportRequest) {
        //noop, intentionally left empty
    }

    @Override
    public void logIndexTemplatePutted(String templateName, IndexTemplateMetadata originalTemplate, IndexTemplateMetadata currentTemplate, String action, TransportRequest transportRequest) {
        //noop, intentionally left empty
    }

    @Override
    public void logIndexTemplateDeleted(List<String> templateNames, String action, TransportRequest transportRequest) {
        //noop, intentionally left empty
    }

    @Override
    public void logIndexCreated(String unresolvedIndexName, String action, TransportRequest transportRequest) {
        //noop, intentionally left empty
    }

    @Override
    public void logIndicesDeleted(List<String> indexNames, String action, TransportRequest transportRequest) {
        //noop, intentionally left empty
    }

    @Override
    public void logIndexSettingsUpdated(List<String> indexNames, String action, TransportRequest transportRequest) {
        //noop, intentionally left empty
    }

    @Override
    public void logIndexMappingsUpdated(List<String> indexNames, String action, TransportRequest transportRequest) {
        //noop, intentionally left empty
    }

    @Override
    public void logSucceededKibanaLogin(UserInformation effectiveUser) {
        //noop, intentionally left empty
    }

    @Override
    public void logSucceededKibanaLogout(UserInformation effectiveUser) {
        //noop, intentionally left empty
    }

    @Override
	public void logBlockedIp(TransportRequest request, String action, TransportAddress remoteAddress, Task task) {
		//noop, intentionally left empty		
	}

	@Override
	public void logBlockedIp(RestRequest request, InetSocketAddress remoteAddress) {
		//noop, intentionally left empty		
	}

	@Override
	public void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
		//noop, intentionally left empty		
	}
    
}
