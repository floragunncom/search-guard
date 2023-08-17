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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComplianceIndexActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(ComplianceIndexActionFilter.class);
    private final AuditLogConfig auditLogConfig;
    private final AuditLog auditLog;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public ComplianceIndexActionFilter(
            AuditLogConfig auditLogConfig, AuditLog auditLog,
            ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.auditLogConfig = auditLogConfig;
        this.auditLog = auditLog;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
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

    private IndexMetadata getIndexCurrentState(String indexName) {
        return clusterService.state().metadata().indices().get(indexName);
    }

    private Map<String, Settings> getIndicesCurrentSettings(Set<String> indexNames) {
        Map<String, Settings> indicesSettings = new HashMap<>();
        indexNames.forEach(index -> {
            IndexMetadata indexMetadata = getIndexCurrentState(index);
            Settings settings = indexMetadata != null? indexMetadata.getSettings() : null;
            indicesSettings.put(index, settings);
        });
        return indicesSettings;
    }

    private Map<String, MappingMetadata> getIndicesCurrentMappings(Set<String> indexNames) {
        Map<String, MappingMetadata> indicesMappings = new HashMap<>();
        indexNames.forEach(index -> {
            IndexMetadata indexMetadata = getIndexCurrentState(index);
            MappingMetadata mapping = indexMetadata != null? indexMetadata.mapping() : null;
            indicesMappings.put(index, mapping);
        });
        return indicesMappings;
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
                final String resolvedIndexName = indexNameExpressionResolver.resolveDateMathExpression(this.indexName);
                final IndexMetadata currentIndexState = getIndexCurrentState(resolvedIndexName);
                auditLog.logIndexCreated(indexName, currentIndexState, action, request);
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
        private final Set<String> resolvedIndexNames;
        private final DeleteIndexRequest request;
        private final ActionListener<Response> originalListener;

        private DeleteIndexListenerWrapper(String action, DeleteIndexRequest request, ActionListener<Response> originalListener) {
            this.action = action;
            this.indexNames = Arrays.asList(request.indices());
            this.resolvedIndexNames = new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request)));
            this.request = request;
            this.originalListener = originalListener;
        }

        @Override
        public void onResponse(Response response) {
            try {
                auditLog.logIndicesDeleted(indexNames, resolvedIndexNames, action, request);
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging indices {} deleted audit message", resolvedIndexNames, e);
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
        private final  Set<String> resolvedIndexNames;
        private final  UpdateSettingsRequest request;
        private final ActionListener<Response> originalListener;

        private UpdateIndexSettingsListenerWrapper(String action, UpdateSettingsRequest request, ActionListener<Response> originalListener) {
            this.action = action;
            this.indexNames = Arrays.asList(request.indices());
            this.request = request;
            this.originalListener = originalListener;
            this.resolvedIndexNames = new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request)));
        }

        @Override
        public void onResponse(Response response) {
            try {
                final Map<String, Settings> currentIndicesSettings = getIndicesCurrentSettings(resolvedIndexNames);
                for(String indexName : resolvedIndexNames) {
                    try {
                        Settings currentSettings = currentIndicesSettings.get(indexName);
                        auditLog.logIndexSettingsUpdated(indexNames, indexName, currentSettings, action, request);
                    } catch (Exception e) {
                        log.debug("An error occurred while logging index '{}' settings updated audit message", indexName, e);
                    }
                }
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging indices {} settings updated audit messages", resolvedIndexNames, e);
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
        private final Set<String> resolvedIndexNames;
        private final PutMappingRequest request;
        private final ActionListener<Response> originalListener;

        private UpdateIndexMappingsListenerWrapper(String action, PutMappingRequest request, ActionListener<Response> originalListener) {
            this.action = action;
            this.indexNames = getIndexNames(request);
            this.request = request;
            this.originalListener = originalListener;
            this.resolvedIndexNames = PutMappingRequestIndicesResolver.resolveIndexNames(
                    request, indexNameExpressionResolver, clusterService.state()
            );
        }

        @Override
        public void onResponse(Response response) {
            try {
                final Map<String, MappingMetadata> currentIndicesMappings = getIndicesCurrentMappings(resolvedIndexNames);
                for(String indexName : resolvedIndexNames) {
                    try {
                        MappingMetadata currentMapping = currentIndicesMappings.get(indexName);
                        auditLog.logIndexMappingsUpdated(indexNames, indexName, currentMapping, action, request);
                    } catch (Exception e) {
                        log.debug("An error occurred while logging index '{}' mappings updated audit message", indexName, e);
                    }
                }
                originalListener.onResponse(response);
            } catch (Exception e) {
                log.debug("An error occurred while logging indices {} mappings updated audit messages", resolvedIndexNames, e);
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
