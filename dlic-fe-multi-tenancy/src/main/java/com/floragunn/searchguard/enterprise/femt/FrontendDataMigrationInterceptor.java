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

package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.enterprise.femt.tenants.MultitenancyActivationService;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.floragunn.searchguard.enterprise.femt.MultiTenancyAuthorizationFilter.SG_FILTER_LEVEL_FEMT_DONE;

class FrontendDataMigrationInterceptor {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final String kibanaServerUsername;
    private final ThreadContext threadContext;
    private final Client nodeClient;

    public FrontendDataMigrationInterceptor(ThreadContext threadContext, Client nodeClient, FeMultiTenancyConfig config) {
        this.threadContext = threadContext;
        this.nodeClient = nodeClient;
        this.kibanaServerUsername = config.getServerUsername();
    }

    public SyncAuthorizationFilter.Result process(Set<String> kibanaIndices, PrivilegesEvaluationContext context, ActionListener<?> listener) {
        try {
            Optional<ActionProcessor> actionProcessor = getActionProcessor(kibanaIndices, context, listener);

            return actionProcessor
                    .map(processor -> (ActionProcessor) new UserAccessGuardWrapper(context.getUser(), processor))
                    .orElseGet(PassOnFastLineProcessor::new)
                    .process();


        } catch (Exception e) {
            log.error("An error occurred while intercepting migration", e);
            listener.onFailure(e);
            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }

    @SuppressWarnings("unchecked")
    private Optional<ActionProcessor> getActionProcessor(Set<String> kibanaIndices, PrivilegesEvaluationContext context, ActionListener<?> listener) {
        if((context.getUser()!= null) && (Strings.isNullOrEmpty(context.getUser().getRequestedTenant())) && (kibanaServerUsername.equals(context.getUser().getName())) && (context.getRequest() instanceof BulkRequest bulkRequest)) {
            log.debug("Index '{}' used during migration detected.", kibanaIndices);
            return Optional.of(() -> handleDataMigration(kibanaIndices, context, bulkRequest, (ActionListener<BulkResponse>) listener));
        } else if (TransportPutMappingAction.TYPE.name().equals(context.getAction().name())) {
            log.debug("Migration of mappings for index '{}' detected ", kibanaIndices);
            return Optional.of(() -> extendIndexMappingWithMultiTenancyData((PutMappingRequest) context.getRequest(), (ActionListener<AcknowledgedResponse>)listener));
        } else if (TransportCreateIndexAction.TYPE.name().equals(context.getAction().name())) {
            log.debug("Creation of index '{}' detected", kibanaIndices);
            return Optional.of(() -> extendIndexMappingWithMultiTenancyData((CreateIndexRequest) context.getRequest(), (ActionListener<CreateIndexResponse>)listener));
        }
        return Optional.empty();
    }

    private SyncAuthorizationFilter.Result handleDataMigration(Set<String> kibanaIndices, PrivilegesEvaluationContext context, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        String actionName = context.getAction().name();
        log.debug("Data migration - action '{}' invoked, request class '{}'.", actionName, context.getRequest().getClass());
        boolean requestExtended = false;
        for (DocWriteRequest<?> item : bulkRequest.requests()) {
            if(item instanceof IndexRequest indexRequest) {
                boolean isKibanaIndex = kibanaIndices.contains(indexRequest.index());
                if(isKibanaIndex) {
                    Map<String, Object> source = XContentHelper.convertToMap(indexRequest.source(), true, indexRequest.getContentType()).v2();
                    if(RequestResponseTenantData.isScopedId(indexRequest.id())) {
                        if (!RequestResponseTenantData.containsSgTenantField(source)) {
                            String tenantName = RequestResponseTenantData.extractTenantFromId(indexRequest.id());
                            log.debug(
                                    "Data migration - adding field '{}' to document '{}' from index '{}' with value '{}'.",
                                    RequestResponseTenantData.getSgTenantField(), indexRequest.id(), indexRequest.index(), tenantName
                            );
                            RequestResponseTenantData.appendSgTenantFieldTo(source, tenantName);
                            indexRequest.source(source);
                            requestExtended = true;
                        } else {
                            log.debug(
                                    "Data migration - document already '{}' contains {} field with value '{}'",
                                    indexRequest.id(), RequestResponseTenantData.getSgTenantField(),
                                    source.get(RequestResponseTenantData.getSgTenantField())
                            );
                        }
                    }
                }
            }
        }
        if(requestExtended) {
            try (ThreadContext.StoredContext ctx = threadContext.newStoredContext()) {
                threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, bulkRequest.toString());
                nodeClient.bulk(bulkRequest, listener);
                log.debug("Data migration - request extended");
                return SyncAuthorizationFilter.Result.INTERCEPTED;
            }
        } else {
            log.debug("Data migration - request not extended");
            return SyncAuthorizationFilter.Result.OK;
        }
    }

    private SyncAuthorizationFilter.Result extendIndexMappingWithMultiTenancyData(PutMappingRequest request,
                                                                                  ActionListener<AcknowledgedResponse> listener) {
        String source = request.source();
        log.debug("Extend put mappings request for '{}' to support multi tenancy, current mappings '{}'", request.indices(), source);
        try (ThreadContext.StoredContext ctx = threadContext.newStoredContext()) {
            Optional<PutMappingRequest> newRequest =  extendMappingsWithMultitenancy(source)
                    .map(docNode -> createExtendedPutMappingRequest(request, docNode));
            if(newRequest.isPresent()) {
                PutMappingRequest putMappingRequest = newRequest.get();
                threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, putMappingRequest.toString());
                nodeClient.admin().indices().putMapping(putMappingRequest, listener);
                log.debug("Extend put mappings request - mappings extended: '{}'", putMappingRequest.source());
                return SyncAuthorizationFilter.Result.INTERCEPTED;
            } else {
                log.debug("Extend put mappings request - mappings not extended");
                return SyncAuthorizationFilter.Result.OK;
            }
        } catch (DocumentParseException e) {
            String message = "Cannot extend put mappings request with information related to multi tenancy";
            log.error(message, e);
            listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
    }

    private SyncAuthorizationFilter.Result extendIndexMappingWithMultiTenancyData(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        String sourceMappings = request.mappings();
        if(Strings.isNullOrEmpty(sourceMappings)) {
            log.debug(
                    "Extend create index request for '{}' to support multi tenancy. Exit early, mappings from request are empty",
                    Arrays.asList(request.indices())
            );
            return SyncAuthorizationFilter.Result.OK;
        }
        log.debug(
                "Extend create index request for '{}' to support multi tenancy, current mappings '{}",
                Arrays.asList(request.indices()), sourceMappings
        );
        try (ThreadContext.StoredContext ctx = threadContext.newStoredContext()) {
            UnparsedDocument<?> mappings = UnparsedDocument.from(sourceMappings, Format.JSON);
            DocNode requestSource = mappings.parseAsDocNode();
            if(requestSource.hasNonNull("_doc")) {
                Optional<String> newMappings = extendMappingsWithMultitenancy(requestSource.getAsNode("_doc"))
                        .map(updatedMappings -> requestSource.with("_doc", updatedMappings))
                        .map(DocNode::toJsonString);
                if (newMappings.isPresent()) {
                    String extendedMappings = newMappings.get();
                    request.mapping(extendedMappings);
                    threadContext.putHeader(SG_FILTER_LEVEL_FEMT_DONE, request.toString());
                    nodeClient.admin().indices().create(request, listener);
                    log.debug("Extend create index - mappings extended: '{}'", extendedMappings);
                    return SyncAuthorizationFilter.Result.INTERCEPTED;
                } else {
                    log.debug("Extend create index - mappings not extended");
                }
            }
        } catch (DocumentParseException e) {
            String message = "Cannot extend index mapping with information related to multi tenancy during index creation";
            log.error(message, e);
            listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
            return SyncAuthorizationFilter.Result.INTERCEPTED;
        }
        return SyncAuthorizationFilter.Result.OK;
    }

    private Optional<DocNode> extendMappingsWithMultitenancy(String sourceMappings) throws DocumentParseException {
        UnparsedDocument<?> mappings = UnparsedDocument.from(sourceMappings, Format.JSON);
        DocNode node = mappings.parseAsDocNode();
        return extendMappingsWithMultitenancy(node);
    }

    private PutMappingRequest createExtendedPutMappingRequest(PutMappingRequest request, DocNode docNode) {
        PutMappingRequest extendedRequest = new PutMappingRequest(request.indices())
                .source(docNode)
                .origin(request.origin())
                .writeIndexOnly(request.writeIndexOnly())
                .masterNodeTimeout(request.masterNodeTimeout())
                .timeout(request.timeout())
                .indicesOptions(request.indicesOptions());
        if(request.getConcreteIndex() != null) {
            extendedRequest.setConcreteIndex(request.getConcreteIndex());
        }
        return extendedRequest;
    }

    private Optional<DocNode> extendMappingsWithMultitenancy(DocNode node) {
        return Optional.of(node) //
                .filter(docNode -> docNode.hasNonNull("properties")) //
                .map(propertiesDocNode -> propertiesDocNode.getAsNode("properties")) //
                .filter(propertiesDocNode -> !RequestResponseTenantData.containsSgTenantField(propertiesDocNode)) //
                .map(propertiesDocNode -> propertiesDocNode.with(MultitenancyActivationService.getSgTenantFieldMapping())) //
                .map(propertiesDocNode -> node.with("properties", propertiesDocNode));
    }

    @FunctionalInterface
    private interface ActionProcessor {

        SyncAuthorizationFilter.Result process();

    }

    private class PassOnFastLineProcessor implements ActionProcessor {

        @Override
        public SyncAuthorizationFilter.Result process() {
            log.debug("Non-migration action, return PASS_ON_FAST_LANE");
            return SyncAuthorizationFilter.Result.PASS_ON_FAST_LANE;
        }
    }

    private class UserAccessGuardWrapper implements ActionProcessor {

        private final User user;
        private final ActionProcessor delegate;

        private UserAccessGuardWrapper(User user, ActionProcessor delegate) {
            this.user = user;
            this.delegate = delegate;
        }

        @Override
        public SyncAuthorizationFilter.Result process() {
            if (!isUserAllowed(user)) {
                log.error("User '{} is not allowed to perform this action", user.getName());
                return SyncAuthorizationFilter.Result.DENIED;
            }
            return delegate.process();
        }

        private boolean isUserAllowed(User user) {
            return kibanaServerUsername.equals(user.getName());
        }
    }
}
