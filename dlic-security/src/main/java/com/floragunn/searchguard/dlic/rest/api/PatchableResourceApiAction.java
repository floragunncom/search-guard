/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.dlic.rest.api;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PATCH;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.floragunn.codova.validation.ValidationErrors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocUpdateException;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.patch.DocPatch;
import com.floragunn.codova.documents.patch.JsonPatch;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchsupport.action.StandardResponse;
import com.google.common.collect.ImmutableList;

public abstract class PatchableResourceApiAction extends AbstractApiAction {

    protected final Logger log = LogManager.getLogger(this.getClass());

    public PatchableResourceApiAction(Settings settings, Path configPath, RestController controller, Client client, AdminDNs adminDNs,
            ConfigurationRepository cl, StaticSgConfig staticSgConfig, ClusterService cs, PrincipalExtractor principalExtractor,
            AuthorizationService authorizationService, SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry,
            ThreadPool threadPool, AuditLog auditLog) {
        super(settings, configPath, controller, client, adminDNs, cl, staticSgConfig, cs, principalExtractor, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog);
    }

    protected List<Route> getStandardResourceRoutes(String resourceName) {
        return ImmutableList.of(new Route(GET, "/_searchguard/api/" + resourceName + "/{name}"),
                new Route(GET, "/_searchguard/api/" + resourceName + "/"), new Route(DELETE, "/_searchguard/api/" + resourceName + "/{name}"),
                new Route(PUT, "/_searchguard/api/" + resourceName + "/{name}"), new Route(PATCH, "/_searchguard/api/" + resourceName + "/"),
                new Route(PATCH, "/_searchguard/api/" + resourceName + "/{name}"));
    }

    private void handlePatch(RestChannel channel, final RestRequest request, final Client client) throws IOException {
        if (request.getXContentType() != XContentType.JSON) {
            badRequestResponse(channel, "PATCH accepts only application/json");
            return;
        }

        String name = request.param("name");
        SgDynamicConfiguration<?> existingConfiguration;
        try {
            existingConfiguration = load(getConfigName(), false);
        } catch (ConfigUnavailableException e1) {
            internalErrorResponse(channel, e1.getMessage());
            return;
        }

        DocPatch jsonPatch;

        try {
            jsonPatch = DocPatch.parse(JsonPatch.MEDIA_TYPE, request.content().utf8ToString());
        } catch (ConfigValidationException e) {
            log.debug("Error while parsing JSON patch", e);
            badRequestResponse(channel, "Error in JSON patch:\n" + e.toString());
            return;
        }

        try {
            if (Strings.isNullOrEmpty(name)) {
                handleBulkPatch(channel, request, client, existingConfiguration, jsonPatch);
            } else {
                handleSinglePatch(channel, request, client, name, existingConfiguration, jsonPatch);
            }
        } catch (ConfigValidationException e) {
            channel.sendResponse(new StandardResponse(e).toRestResponse());
        }
    }

    private void handleSinglePatch(RestChannel channel, RestRequest request, Client client, String name,
            SgDynamicConfiguration<?> existingConfiguration, DocPatch jsonPatch) throws IOException, ConfigValidationException {
        if (isHidden(existingConfiguration, name)) {
            notFound(channel, getResourceName() + " " + name + " not found.");
            return;
        }

        if (isReserved(existingConfiguration, name)) {
            forbidden(channel, "Resource '" + name + "' is read-only.");
            return;
        }

        if (!existingConfiguration.exists(name)) {
            notFound(channel, getResourceName() + " " + name + " not found.");
            return;
        }

        Document<?> existingResource = (Document<?>) existingConfiguration.getCEntry(name);
        DocNode existingResourceDocNode = existingResource.toDocNode().splitDottedAttributeNamesToTree();
        DocNode patchedResourceAsDocNode;

        try {
            patchedResourceAsDocNode = jsonPatch.apply(existingResourceDocNode);
        } catch (DocUpdateException e) {
            log.debug("Error while applying JSON patch", e);
            badRequestResponse(channel, e.getMessage());
            return;
        }

        patchedResourceAsDocNode = postProcessApplyPatchResult(channel, request, existingResourceDocNode,
                patchedResourceAsDocNode, name);
        
        if (patchedResourceAsDocNode.getBoolean("hidden") == Boolean.FALSE) {
            patchedResourceAsDocNode = patchedResourceAsDocNode.without("hidden");
        }

        if (patchedResourceAsDocNode.getBoolean("reserved") == Boolean.FALSE) {
            patchedResourceAsDocNode = patchedResourceAsDocNode.without("reserved");
        }

        if (patchedResourceAsDocNode.getBoolean("static") == Boolean.FALSE) {
            patchedResourceAsDocNode = patchedResourceAsDocNode.without("static");
        }

        AbstractConfigurationValidator validator = getValidator(request, patchedResourceAsDocNode);

        if (!validator.validate()) {
            request.params().clear();
            badRequestResponse(channel, validator);
            return;
        }

        Map<String, Object> updated = new LinkedHashMap<>(existingConfiguration.toDocNode().toMap());
        updated.put(name, patchedResourceAsDocNode.toBasicObject());
        
        try (SgDynamicConfiguration<?> mdc = SgDynamicConfiguration.fromDocNode(DocNode.wrap(updated), null,
                existingConfiguration.getCType(), existingConfiguration.getDocVersion(), existingConfiguration.getSeqNo(),
                existingConfiguration.getPrimaryTerm(), cl.getParserContext()).get();) {

            ValidationErrors validationErrors = new ValidationErrors();
            validationErrors.add(configsRelationsValidator.validateConfigRelations(mdc));

            validationErrors.throwExceptionForPresentErrors();

            saveAnUpdateConfigs(client, request, getConfigName(), mdc, new OnSucessActionListener<DocWriteResponse>(channel) {

                @Override
                public void onResponse(DocWriteResponse response) {
                    successResponse(channel, "'" + name + "' updated.");

                }
            });
        }
    }

    private void handleBulkPatch(RestChannel channel, RestRequest request, Client client, SgDynamicConfiguration<?> existingConfiguration,
            DocPatch jsonPatch) throws IOException, ConfigValidationException {

        LinkedHashMap<String, Object> patchBase = new LinkedHashMap<>(existingConfiguration.getCEntries().size());
         
        for (String resourceName : existingConfiguration.getCEntries().keySet()) {
            Document<?> oldResource = (Document<?>) existingConfiguration.getCEntries().get(resourceName);            
            patchBase.put(resourceName, oldResource.toDocNode().splitDottedAttributeNamesToTree().toBasicObject());
        }

        DocNode patchedAsDocNode;

        try {
            patchedAsDocNode = jsonPatch.apply(DocNode.wrap(patchBase));
        } catch (DocUpdateException e) {
            log.debug("Error while applying JSON patch", e);
            badRequestResponse(channel, e.getMessage());
            return;
        }

        for (String resourceName : existingConfiguration.getCEntries().keySet()) {
            Object oldResource = patchBase.get(resourceName);
            Object patchedResource = patchedAsDocNode.get(resourceName);

            if (oldResource != null && !oldResource.equals(patchedResource)) {

                if (isReserved(existingConfiguration, resourceName)) {
                    forbidden(channel, "Resource '" + resourceName + "' is read-only.");
                    return;
                }

                if (isHidden(existingConfiguration, resourceName)) {
                    badRequestResponse(channel, "Resource name '" + resourceName + "' is reserved");
                    return;
                }
            }
        }

        for (String resourceName : patchedAsDocNode.keySet()) {
            DocNode oldResource = DocNode.wrap(patchBase.get(resourceName));
            DocNode patchedResource = DocNode.wrap(patchedAsDocNode.get(resourceName));

            patchedResource = postProcessApplyPatchResult(channel, request, oldResource, patchedResource,
                    resourceName);

            if (oldResource == null || !oldResource.equals(patchedResource)) {
                if (patchedResource.getBoolean("hidden") == Boolean.FALSE) {
                    patchedResource = patchedResource.without("hidden");
                }

                if (patchedResource.getBoolean("reserved") == Boolean.FALSE) {
                    patchedResource = patchedResource.without("reserved");
                }

                if (patchedResource.getBoolean("static") == Boolean.FALSE) {
                    patchedResource = patchedResource.without("static");
                }
                
                AbstractConfigurationValidator validator = getValidator(request, patchedResource);

                if (!validator.validate()) {
                    request.params().clear();
                    badRequestResponse(channel, validator);
                    return;
                }
            }
        }

        try (SgDynamicConfiguration<?> mdc = SgDynamicConfiguration.fromDocNode(patchedAsDocNode, null,
                existingConfiguration.getCType(), existingConfiguration.getDocVersion(), existingConfiguration.getSeqNo(),
                existingConfiguration.getPrimaryTerm(), cl.getParserContext()).get()) {

            ValidationErrors validationErrors = new ValidationErrors();
            validationErrors.add(configsRelationsValidator.validateConfigRelations(mdc));

            validationErrors.throwExceptionForPresentErrors();

            saveAnUpdateConfigs(client, request, getConfigName(), mdc, new OnSucessActionListener<DocWriteResponse>(channel) {

                @Override
                public void onResponse(DocWriteResponse response) {
                    successResponse(channel, "Resource updated.");
                }
            });
        }
    }

    protected DocNode postProcessApplyPatchResult(RestChannel channel, RestRequest request,
            DocNode existingResourceAsJsonNode, DocNode updatedResourceAsJsonNode, String resourceName) throws ConfigValidationException {
        // do nothing by default
        return updatedResourceAsJsonNode;
    }

    @Override
    protected void handleApiRequest(RestChannel channel, final RestRequest request, final Client client) throws IOException {

        if (request.method() == Method.PATCH) {
            handlePatch(channel, request, client);
        } else {
            super.handleApiRequest(channel, request, client);
        }
    }

    private AbstractConfigurationValidator getValidator(RestRequest request, DocNode patchedResource) throws JsonProcessingException {
        BytesReference patchedResourceAsByteReference = new BytesArray(patchedResource.toJsonString().getBytes(StandardCharsets.UTF_8));
        return getValidator(request, patchedResourceAsByteReference);
    }
}
