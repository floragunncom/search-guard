/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.auditlog.AuditLogConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ComplianceIndexActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(ComplianceIndexActionFilter.class);
    private final AuditLogConfig auditLogConfig;
    private final AuditLog auditLog;

    public ComplianceIndexActionFilter(AuditLogConfig auditLogConfig, AuditLog auditLog) {
        this.auditLogConfig = auditLogConfig;
        this.auditLog = auditLog;
    }

    @Override
    public int order() {
        return 1;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        ActionListener<Response> actualListener = listener;
        if (shouldProceed(task)) {
            if (request instanceof CreateIndexRequest) {
                CreateIndexRequest createIndexRequest = (CreateIndexRequest) request;
                actualListener = new CreateIndexListenerWrapper<>(action, createIndexRequest, listener);
            } else if (request instanceof DeleteIndexRequest) {
                DeleteIndexRequest deleteIndexRequest = (DeleteIndexRequest) request;
                actualListener = new DeleteIndexListenerWrapper<>(action, deleteIndexRequest, listener);
            } else if (request instanceof UpdateSettingsRequest) {
                UpdateSettingsRequest updateSettingsRequest = (UpdateSettingsRequest) request;
                actualListener = new UpdateIndexSettingsListenerWrapper<>(action, updateSettingsRequest, listener);
            } else if (request instanceof PutMappingRequest) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;
                actualListener = new UpdateIndexMappingsListenerWrapper<>(action, putMappingRequest, listener);
            }
        }
        chain.proceed(task, action, request, actualListener);
    }

    private boolean shouldProceed(Task task) {
        //to prevent duplication of audit log messages
        return auditLogConfig.isEnabled() && !task.getParentTaskId().isSet();
    }

    private class CreateIndexListenerWrapper<Response> implements ActionListener<Response> {

        private final String action;
        private final String indexName;
        private final CreateIndexRequest request;
        private final ActionListener<Response> originalListener;

        private CreateIndexListenerWrapper(String action, CreateIndexRequest request, ActionListener<Response> originalListener) {
            this.action = action;
            this.indexName = request.index();
            this.request = request;
            this.originalListener = originalListener;
        }

        @Override
        public void onResponse(Response response) {
            try {
                auditLog.logIndexCreated(indexName, action, request);
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging index '{}' created audit message", indexName, e);
                originalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }
    }

    private class DeleteIndexListenerWrapper<Response> implements ActionListener<Response> {

        private final String action;
        private final List<String> indexNames;
        private final DeleteIndexRequest request;
        private final ActionListener<Response> originalListener;

        private DeleteIndexListenerWrapper(String action, DeleteIndexRequest request, ActionListener<Response> originalListener) {
            this.action = action;
            this.indexNames = Arrays.asList(request.indices());
            this.request = request;
            this.originalListener = originalListener;
        }

        @Override
        public void onResponse(Response response) {
            try {
                auditLog.logIndicesDeleted(indexNames, action, request);
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging indices {} deleted audit message", indexNames, e);
                originalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }
    }

    private class UpdateIndexSettingsListenerWrapper<Response> implements ActionListener<Response> {

        private final String action;
        private final List<String> indexNames;
        private final UpdateSettingsRequest request;
        private final ActionListener<Response> originalListener;

        private UpdateIndexSettingsListenerWrapper(String action, UpdateSettingsRequest request, ActionListener<Response> originalListener) {
            this.action = action;
            this.indexNames = Arrays.asList(request.indices());
            this.request = request;
            this.originalListener = originalListener;
        }

        @Override
        public void onResponse(Response response) {
            try {
                auditLog.logIndexSettingsUpdated(indexNames, action, request);
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging indices {} settings updated audit messages", indexNames, e);
                originalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }
    }

    private class UpdateIndexMappingsListenerWrapper<Response> implements ActionListener<Response> {

        private final String action;
        private final List<String> indexNames;
        private final PutMappingRequest request;
        private final ActionListener<Response> originalListener;

        private UpdateIndexMappingsListenerWrapper(String action, PutMappingRequest request, ActionListener<Response> originalListener) {
            this.action = action;
            this.indexNames = getIndexNames(request);
            this.request = request;
            this.originalListener = originalListener;
        }

        @Override
        public void onResponse(Response response) {
            try {
                auditLog.logIndexMappingsUpdated(indexNames, action, request);
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging indices {} mappings updated audit messages", indexNames, e);
                originalListener.onResponse(response);
            }

        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }

        private List<String> getIndexNames(PutMappingRequest request) {
            if (request.getConcreteIndex() != null) {
                return Collections.singletonList(request.getConcreteIndex().getName());
            } else {
                return Arrays.asList(request.indices());
            }
        }
    }
}
