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
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.tasks.Task;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
        return 1;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        ActionListener<Response> actualListener = listener;
        if (shouldProceed(task)) {
            if (request instanceof PutComposableIndexTemplateAction.Request) {
                PutComposableIndexTemplateAction.Request putIndexTemplateRequest = (PutComposableIndexTemplateAction.Request) request;
                actualListener = new PutIndexTemplateListenerWrapper<>(putIndexTemplateRequest, auditLog, auditLogConfig.logDiffsForWrite(), listener);
            } else if (request instanceof PutIndexTemplateRequest) {
                PutIndexTemplateRequest putIndexTemplateRequest = (PutIndexTemplateRequest) request;
                actualListener = new PutIndexTemplateListenerWrapper<>(putIndexTemplateRequest, auditLog, auditLogConfig.logDiffsForWrite(), listener);
            } else if (request instanceof DeleteComposableIndexTemplateAction.Request) {
                DeleteComposableIndexTemplateAction.Request deleteIndexTemplateRequest = (DeleteComposableIndexTemplateAction.Request) request;
                actualListener = new DeleteIndexTemplateListenerWrapper<>(deleteIndexTemplateRequest, auditLog, listener);
            } else if (request instanceof DeleteIndexTemplateRequest) {
                DeleteIndexTemplateRequest deleteIndexTemplateRequest = (DeleteIndexTemplateRequest) request;
                actualListener = new DeleteIndexTemplateListenerWrapper<>(deleteIndexTemplateRequest, auditLog, listener);
            }
        }
        chain.proceed(task, action, request, actualListener);
    }

    private boolean shouldProceed(Task task) {
        //to prevent duplication of audit log messages
        return auditLogConfig.isEnabled() && !task.getParentTaskId().isSet();
    }

    private ComposableIndexTemplate getComposableIndexTemplateCurrentState(String indexTemplateName) {
        return clusterService.state().metadata().templatesV2().get(indexTemplateName);
    }

    private IndexTemplateMetadata getLegacyIndexTemplateCurrentState(String indexTemplateName) {
        return clusterService.state().metadata().templates().get(indexTemplateName);
    }

    private Set<String> resolveComposableTemplateNames(List<String> templateNames) {
        Set<String> existingTemplates = clusterService.state().metadata().templatesV2().keySet();
        if (templateNames.size() > 1) {
            return templateNames.stream().filter(existingTemplates::contains).collect(Collectors.toSet());
        } else {
            String name = templateNames.get(0);
            if (Regex.isMatchAllPattern(name)) {
                return existingTemplates;
            } else {
                return existingTemplates.stream().filter(existing -> Regex.simpleMatch(name, existing)).collect(Collectors.toSet());
            }
        }
    }

    private Set<String> resolveLegacyTemplateNames(String templateName) {
        Set<String> existingTemplates = clusterService.state().metadata().templates().keySet();
        if (Regex.isMatchAllPattern(templateName)) {
            return existingTemplates;
        }
        return existingTemplates.stream().filter(existing -> Regex.simpleMatch(templateName, existing)).collect(Collectors.toSet());
    }

    private class PutIndexTemplateListenerWrapper<Response> implements ActionListener<Response> {

        private final String templateName;
        private final AuditLog auditLog;
        private final ActionListener<Response> originalListener;
        private final ComposableIndexTemplate originalComposableIndexTemplate;
        private final IndexTemplateMetadata originalLegacyIndexTemplate;
        private final boolean legacyTemplate;

        private PutIndexTemplateListenerWrapper(
                PutComposableIndexTemplateAction.Request request,
                AuditLog auditLog, boolean logDiffsForWrite,
                ActionListener<Response> originalListener) {
            this.templateName = request.name();
            this.auditLog = auditLog;
            this.originalListener = originalListener;
            this.originalComposableIndexTemplate = getComposableIndexTemplateCurrentState(request.name());
            this.originalLegacyIndexTemplate = null;
            this.legacyTemplate = false;

            if (logDiffsForWrite) {
                if (this.originalComposableIndexTemplate == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Original composable index template with name {} does not exist", request.name());
                    }
                }
            }
        }

        private PutIndexTemplateListenerWrapper(
                PutIndexTemplateRequest request,
                AuditLog auditLog, boolean logDiffsForWrite,
                ActionListener<Response> originalListener) {
            this.templateName = request.name();
            this.auditLog = auditLog;
            this.originalListener = originalListener;
            this.originalComposableIndexTemplate = null;
            this.originalLegacyIndexTemplate = getLegacyIndexTemplateCurrentState(request.name());
            this.legacyTemplate = true;

            if (logDiffsForWrite) {
                if (originalLegacyIndexTemplate == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Original legacy index template with name {} does not exist", request.name());
                    }
                }
            }
        }

        @Override
        public void onResponse(Response response) {
            if (legacyTemplate) {
                final IndexTemplateMetadata currentTemplate = getLegacyIndexTemplateCurrentState(templateName);
                auditLog.logIndexTemplatePutted(templateName, originalLegacyIndexTemplate, currentTemplate);
                originalListener.onResponse(response);
            } else {
                final ComposableIndexTemplate currentTemplate = getComposableIndexTemplateCurrentState(templateName);
                auditLog.logIndexTemplatePutted(templateName, originalComposableIndexTemplate, currentTemplate);
                originalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }
    }

    private class DeleteIndexTemplateListenerWrapper<Response> implements ActionListener<Response> {

        private final List<String> templateNames;
        private final Set<String> resolvedTemplateNames;
        private final AuditLog auditLog;
        private final ActionListener<Response> originalListener;

        private DeleteIndexTemplateListenerWrapper(
                DeleteComposableIndexTemplateAction.Request request,
                AuditLog auditLog, ActionListener<Response> originalListener) {
            this.templateNames = Arrays.asList(request.names());
            this.resolvedTemplateNames = resolveComposableTemplateNames(templateNames);
            this.auditLog = auditLog;
            this.originalListener = originalListener;
        }

        private DeleteIndexTemplateListenerWrapper(
                DeleteIndexTemplateRequest request,
                AuditLog auditLog, ActionListener<Response> originalListener) {
            this.templateNames = Collections.singletonList(request.name());
            this.resolvedTemplateNames = resolveLegacyTemplateNames(request.name());
            this.auditLog = auditLog;
            this.originalListener = originalListener;
        }

        @Override
        public void onResponse(Response response) {
            auditLog.logIndexTemplateDeleted(templateNames, resolvedTemplateNames);
            originalListener.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            originalListener.onFailure(e);
        }
    }
}