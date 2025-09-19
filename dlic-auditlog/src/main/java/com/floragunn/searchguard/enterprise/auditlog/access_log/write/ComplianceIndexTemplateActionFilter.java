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
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ComplianceIndexTemplateActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(ComplianceIndexTemplateActionFilter.class);

    private final AuditLogConfig auditLogConfig;
    private final AuditLog auditLog;
    private final ClusterService clusterService;

    public ComplianceIndexTemplateActionFilter(AuditLogConfig auditLogConfig, AuditLog auditLog, ClusterService clusterService) {
        this.auditLogConfig = auditLogConfig;
        this.auditLog = auditLog;
        this.clusterService = clusterService;
    }

    @Override
    public int order() {
        return 2;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        ActionListener<Response> actualListener = listener;
        if (shouldProceed(task)) {
            if (request instanceof TransportPutComposableIndexTemplateAction.Request) {
                TransportPutComposableIndexTemplateAction.Request putIndexTemplateRequest = (TransportPutComposableIndexTemplateAction.Request) request;
                actualListener = new PutIndexTemplateListenerWrapper<>(action, putIndexTemplateRequest, listener);
            } else if (request instanceof PutIndexTemplateRequest) {
                PutIndexTemplateRequest putIndexTemplateRequest = (PutIndexTemplateRequest) request;
                actualListener = new PutIndexTemplateListenerWrapper<>(action, putIndexTemplateRequest, listener);
            } else if (request instanceof TransportDeleteComposableIndexTemplateAction.Request) {
                TransportDeleteComposableIndexTemplateAction.Request deleteIndexTemplateRequest = (TransportDeleteComposableIndexTemplateAction.Request) request;
                actualListener = new DeleteIndexTemplateListenerWrapper<>(action, deleteIndexTemplateRequest, listener);
            } else if (request instanceof DeleteIndexTemplateRequest) {
                DeleteIndexTemplateRequest deleteIndexTemplateRequest = (DeleteIndexTemplateRequest) request;
                actualListener = new DeleteIndexTemplateListenerWrapper<>(action, deleteIndexTemplateRequest, listener);
            }
        }
        chain.proceed(task, action, request, actualListener);
    }

    private boolean shouldProceed(Task task) {
        //to prevent duplication of audit log messages
        return auditLogConfig.isEnabled() && !task.getParentTaskId().isSet();
    }

    private ComposableIndexTemplate getComposableIndexTemplateCurrentState(String indexTemplateName) {
        return clusterService.state().metadata().getProject().templatesV2().get(indexTemplateName);
    }

    private IndexTemplateMetadata getLegacyIndexTemplateCurrentState(String indexTemplateName) {
        return clusterService.state().metadata().getProject().templates().get(indexTemplateName);
    }

    private class PutIndexTemplateListenerWrapper<Response> implements ActionListener<Response> {

        private final String action;
        private final String templateName;
        private final TransportRequest request;
        private final ActionListener<Response> originalListener;
        private final ComposableIndexTemplate originalComposableIndexTemplate;
        private final IndexTemplateMetadata originalLegacyIndexTemplate;
        private final boolean legacyTemplate;

        private PutIndexTemplateListenerWrapper(String action, TransportPutComposableIndexTemplateAction.Request request,
                                                ActionListener<Response> originalListener) {
            this.action = action;
            this.templateName = request.name();
            this.request = request;
            this.originalListener = originalListener;
            this.originalComposableIndexTemplate = getComposableIndexTemplateCurrentState(request.name());
            this.originalLegacyIndexTemplate = null;
            this.legacyTemplate = false;
        }

        private PutIndexTemplateListenerWrapper(String action, PutIndexTemplateRequest request,
                                                ActionListener<Response> originalListener) {
            this.action = action;
            this.templateName = request.name();
            this.request = request;
            this.originalListener = originalListener;
            this.originalComposableIndexTemplate = null;
            this.originalLegacyIndexTemplate = getLegacyIndexTemplateCurrentState(request.name());
            this.legacyTemplate = true;
        }

        @Override
        public void onResponse(Response response) {
            try {
                if (legacyTemplate) {
                    final IndexTemplateMetadata currentTemplate = getLegacyIndexTemplateCurrentState(templateName);
                    auditLog.logIndexTemplatePutted(templateName, originalLegacyIndexTemplate, currentTemplate, action, request);
                } else {
                    final ComposableIndexTemplate currentTemplate = getComposableIndexTemplateCurrentState(templateName);
                    auditLog.logIndexTemplatePutted(templateName, originalComposableIndexTemplate, currentTemplate, action, request);
                }
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging index template '{}' putted audit message", templateName, e);
                originalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }
    }

    private class DeleteIndexTemplateListenerWrapper<Response> implements ActionListener<Response> {

        private final String action;
        private final List<String> templateNames;
        private final TransportRequest request;
        private final ActionListener<Response> originalListener;

        private DeleteIndexTemplateListenerWrapper(String action, TransportDeleteComposableIndexTemplateAction.Request request,
                                                   ActionListener<Response> originalListener) {
            this.action = action;
            this.templateNames = Arrays.asList(request.names());
            this.request = request;
            this.originalListener = originalListener;
        }

        private DeleteIndexTemplateListenerWrapper(String action, DeleteIndexTemplateRequest request,
                                                   ActionListener<Response> originalListener) {
            this.action = action;
            this.templateNames = Collections.singletonList(request.name());
            this.request = request;
            this.originalListener = originalListener;
        }

        @Override
        public void onResponse(Response response) {
            try {
                auditLog.logIndexTemplateDeleted(templateNames, action, request);
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging index templates '{}' deleted audit message", templateNames, e);
                originalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }
    }
}