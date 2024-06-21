/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.configuration.variables;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.FailureListener;
import com.floragunn.searchguard.configuration.variables.ConfigVarRefreshAction.Response;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.client.Actions;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.google.common.io.BaseEncoding;

public class ConfigVarService implements ComponentStateProvider {
    private final static Logger log = LogManager.getLogger(ConfigVarService.class);

    private final Client client;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Map<String, RequestedValue> requestedValues = new ConcurrentHashMap<>();
    private final ComponentState componentState = new ComponentState(1000, null, "config_var_storage", ConfigVarService.class);
    private final String indexName = ".searchguard_config_vars";
    private volatile Map<String, Object> values;
    private final List<Runnable> changeListeners = new ArrayList<>();
    private final EncryptionKeys encryptionKeys;

    public ConfigVarService(Client client, ClusterService clusterService, ThreadPool threadPool,
            ProtectedConfigIndexService protectedConfigIndexService, EncryptionKeys encryptionKeys) {
        this.client = client;
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.encryptionKeys = encryptionKeys;
        componentState.addPart(protectedConfigIndexService
                .createIndex(new ConfigIndex(indexName).mapping(ConfigVar.INDEX_MAPPING).onIndexReady((onFailure) -> init(onFailure))));
    }

    public Object get(String id) {
        Map<String, Object> values = this.values;
        
        if (values == null) {
            throw new ConfigVarServiceNotYetAvailableException("ConfigVarService is not yet initialized");
        }

        return values.get(id);
    }

    public String getAsString(String id) {
        Object object = get(id);

        if (object != null) {
            return object.toString();
        } else {
            return null;
        }
    }

    public String getAsStringMandatory(String id) throws ConfigValidationException {
        Object object = get(id);

        if (object != null) {
            return object.toString();
        } else {
            throw new ConfigValidationException(new MissingAttribute(id));
        }
    }

    public CompletableFuture<StandardResponse> delete(String id) {
        CompletableFuture<StandardResponse> result = new CompletableFuture<>();

        this.privilegedConfigClient.delete(new DeleteRequest(indexName, id).setRefreshPolicy(RefreshPolicy.IMMEDIATE),
                new ActionListener<DeleteResponse>() {

                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        ConfigVarRefreshAction.send(client, new ActionListener<Response>() {

                            @Override
                            public void onResponse(Response response) {
                                try {
                                    log.info("Result of settings update:\n" + response);

                                    if (response.hasFailures()) {
                                        result.complete(new StandardResponse(500).error(null,
                                                "Index update was successful, but node refresh partially failed", response.failures().toString()));
                                    } else if (deleteResponse.getResult() == Result.DELETED) {
                                        result.complete(new StandardResponse(200).message("Deleted"));
                                    } else {
                                        result.complete(new StandardResponse(404).error("Not found"));
                                    }
                                } catch (Exception e) {
                                    log.error("Error in onResponse", e);
                                    result.completeExceptionally(e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.error("settings update failed", e);
                                result.complete(new StandardResponse(500).error("Index update was successful, but node refresh failed"));
                            }
                        });
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Error while deleting " + id, e);
                        componentState.addLastException("delete", e);
                        result.completeExceptionally(e);
                    }
                });

        return result;
    }

    public CompletableFuture<StandardResponse> update(ConfigVarApi.UpdateAction.Request request) throws EncryptionException {
        String id = request.getId();

        CompletableFuture<StandardResponse> result = new CompletableFuture<>();

        Map<String, Object> doc = new LinkedHashMap<>();

        if (request.isEncrypt()) {
            doc.put("encrypted", encryptionKeys.getEncryptedData(request.getValue()));
        } else {
            doc.put("value", request.getValue());
        }

        if (request.getScope() != null) {
            doc.put("scope", request.getScope());
        }

        doc.put("updated", Instant.now());

        log.info("Writing secret " + id);

        this.privilegedConfigClient.index(new IndexRequest(indexName).opType(request.mustNotExist() ? OpType.CREATE : OpType.INDEX)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE).id(id).source(doc), new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        if (indexResponse.getResult() == Result.CREATED || indexResponse.getResult() == Result.UPDATED) {
                            ConfigVarRefreshAction.send(client, new ActionListener<Response>() {

                                @Override
                                public void onResponse(Response response) {
                                    try {
                                        log.info("Result of settings update:\n" + response);

                                        if (response.hasFailures()) {
                                            result.complete(new StandardResponse(500).error(null,
                                                    "Index update was successful, but node refresh partially failed",
                                                    response.failures().toString()));
                                        } else if (indexResponse.getResult() == Result.CREATED) {
                                            result.complete(new StandardResponse(201).message("Created"));
                                        } else { // indexResponse.getResult() == Result.UPDATED
                                            result.complete(new StandardResponse(200).message("Updated"));
                                        }
                                    } catch (Exception e) {
                                        log.error("Error in onResponse", e);
                                        result.completeExceptionally(e);
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    log.error("settings update failed", e);
                                    result.complete(new StandardResponse(500).error("Index update was successful, but node refresh failed"));
                                }
                            });
                        } else if (indexResponse.getResult() == Result.NOOP) {
                            result.complete(new StandardResponse(200).message("Not changed"));
                        } else {
                            result.complete(new StandardResponse(500).error(null, "Unexpected response", indexResponse.getResult() + ""));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof VersionConflictEngineException) {
                            if (e.getMessage().contains("document already exists")) {
                                result.complete(new StandardResponse(412).error("Variable does already exist"));
                            } else {
                                result.complete(new StandardResponse(412).error(e.getMessage()));
                            }
                        } else {
                            log.error("Error while updating " + id, e);
                            componentState.addLastException("update", e);
                            result.completeExceptionally(e);
                        }
                    }
                });

        return result;

    }

    public CompletableFuture<StandardResponse> updateAll(Map<String, ConfigVar> valueMap) {
        BulkRequest bulkRequest = new BulkRequest();

        bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        ValidationErrors validationErrors = new ValidationErrors();

        for (Map.Entry<String, ConfigVar> entry : valueMap.entrySet()) {
            ConfigVar configVar = entry.getValue();

            if (configVar.getEncValue() != null) {
                try {
                    encryptionKeys.getDecryptedData(configVar);
                } catch (Exception e) {
                    validationErrors.add(new ValidationError(entry.getKey(), e.getMessage()).cause(e));
                }
            }

            bulkRequest.add(new IndexRequest(indexName).id(entry.getKey()).source(DocWriter.json().writeAsString(entry.getValue().updatedNow()),
                    XContentType.JSON));
        }

        if (validationErrors.hasErrors()) {
            return CompletableFuture.completedFuture(new StandardResponse(400).error(validationErrors));
        }

        Set<String> idsForDeletion = this.values != null ? this.values.keySet().stream().filter((id) -> !valueMap.containsKey(id)).collect(Collectors.toSet()) : ImmutableSet.empty();

        for (String idForDeletion : idsForDeletion) {
            bulkRequest.add(new DeleteRequest(indexName).id(idForDeletion));
        }

        if (bulkRequest.numberOfActions() == 0) {
            return CompletableFuture.completedFuture(new StandardResponse(200).message("Nothing to update"));
        }

        CompletableFuture<StandardResponse> result = new CompletableFuture<>();

        this.privilegedConfigClient.bulk(bulkRequest, new ActionListener<BulkResponse>() {

            @Override
            public void onResponse(BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    result.complete(new StandardResponse(500).error("Bulk update partially failed"));
                } else {
                    ConfigVarRefreshAction.send(client, new ActionListener<Response>() {

                        @Override
                        public void onResponse(Response response) {
                            log.info("Result of settings update:\n" + response);

                            if (response.hasFailures()) {
                                result.complete(new StandardResponse(500).error("Index update was successful, but node refresh partially failed"));
                            } else {
                                result.complete(new StandardResponse(200).message(getUpdateMessage(bulkResponse)));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            log.error("settings update failed", e);
                            result.complete(new StandardResponse(500).error("Index update was successful, but node refresh failed"));
                        }
                    });
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while updating secrets", e);
                componentState.addLastException("update", e);
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    private String getUpdateMessage(BulkResponse bulkResponse) {
        int created = 0;
        int updated = 0;
        int deleted = 0;
        int unchanged = 0;
        int notFound = 0;

        for (BulkItemResponse item : bulkResponse.getItems()) {
            DocWriteResponse response = item.getResponse();

            switch (response.getResult()) {
            case CREATED:
                created++;
                break;
            case DELETED:
                deleted++;
                break;
            case NOOP:
                unchanged++;
                break;
            case NOT_FOUND:
                notFound++;
                break;
            case UPDATED:
                updated++;
                break;
            }
        }

        String result = "Update succesful: " + created + " created; " + updated + " updated; " + deleted + " deleted";

        if (unchanged > 0) {
            result += "; " + unchanged + " unchanged";
        }

        if (notFound > 0) {
            result += "; " + notFound + " not found";
        }

        return result;
    }

    public synchronized void requestRandomKey(String id, int bits, String scope) {
        requestedValues.put(id, new RequestedValue(() -> generateKey(bits), scope));
    }

    public synchronized void requestValue(String id, Supplier<Object> supplier, String scope) {
        requestedValues.put(id, new RequestedValue(supplier, scope));
    }

    public Map<String, ConfigVar> getAllFromIndex() {
        SearchResponse response = privilegedConfigClient.search(new SearchRequest(this.indexName)
                .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(1000)).scroll(new TimeValue(10000))).actionGet();

        Map<String, ConfigVar> result = new LinkedHashMap<>();

        try {
            do {
                for (SearchHit searchHit : response.getHits().getHits()) {
                    try {
                        result.put(searchHit.getId(), new ConfigVar(DocNode.wrap(searchHit.getSourceAsMap())));
                    } catch (Exception e) {
                        log.error("Error while reading " + searchHit, e);
                    }
                }

                response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(10000)).execute().actionGet();

            } while (response.getHits().getHits().length != 0);
        } finally {
            Actions.clearScrollAsync(client, response);
        }

        return result;
    }

    public ConfigVar getFromIndex(String id) {
        GetResponse response = privilegedConfigClient.get(new GetRequest(this.indexName, id)).actionGet();

        if (response.isExists()) {
            try {
                return new ConfigVar(DocNode.wrap(response.getSourceAsMap()));
            } catch (ConfigValidationException e) {
                throw new RuntimeException(e);
            }
        } else {
            return null;
        }
    }

    private synchronized void init(FailureListener failureListener) {
        Map<String, RequestedValue> requestedValues = new HashMap<>(this.requestedValues);

        try {
            Map<String, Object> existingValues = new HashMap<>(readValues());

            if (log.isDebugEnabled()) {
                log.debug("Read existing values: " + existingValues.keySet());
            }

            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {

                if (requestedValues.isEmpty()) {
                    log.debug("No secrets need to be generated");
                } else {
                    log.info("Creating secrets: " + requestedValues);
                }

                HashMap<String, Object> newValues = new HashMap<>();

                for (Map.Entry<String, RequestedValue> entry : requestedValues.entrySet()) {
                    String id = entry.getKey();

                    try {
                        if (existingValues.containsKey(id)) {
                            continue;
                        }

                        Object value = entry.getValue().valueSupplier.get();
                        newValues.put(id, value);

                        Map<String, Object> doc = new LinkedHashMap<>();

                        doc.put("encrypted", encryptionKeys.getEncryptedData(value));

                        String scope = entry.getValue().scope;

                        if (scope != null) {
                            doc.put("scope", scope);
                        }

                        doc.put("updated", Instant.now());

                        IndexResponse response = privilegedConfigClient
                                .index(new IndexRequest(this.indexName).id(id).source(doc).setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();

                        this.requestedValues.remove(id);

                        if (log.isDebugEnabled()) {
                            log.debug("Written " + id + ": " + response);
                        }
                    } catch (Exception e) {
                        throw new Exception("Error while initializing value for " + id, e);
                    }
                }

                if (!newValues.isEmpty()) {
                    existingValues.putAll(newValues);
                    ConfigVarRefreshAction.send(client);
                }
            }

            values = existingValues;
            componentState.setInitialized();
            
            notifyChangeListeners();

            failureListener.onSuccess();

        } catch (Exception e) {
            failureListener.onFailure(e);
        }
    }

    public synchronized void addChangeListener(Runnable changeLister) {
        this.changeListeners.add(changeLister);
    }

    private void notifyChangeListeners() {
        for (Runnable changeListener : changeListeners) {
            try {
                changeListener.run();
            } catch (Exception e) {
                componentState.addLastException("notifyChangeListeners", e);
                log.error("Exception in change listener: " + changeListener, e);
            }
        }
    }

    private Map<String, Object> readValues() {
        if (log.isTraceEnabled()) {
            log.trace("SecretsService.readValues()");
        }

        SearchResponse response = privilegedConfigClient.search(new SearchRequest(this.indexName)
                .source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(1000)).scroll(new TimeValue(10000))).actionGet();

        Map<String, Object> values = new HashMap<>();

        try {
            do {
                for (SearchHit searchHit : response.getHits().getHits()) {
                    try {
                        Map<String, Object> source = searchHit.getSourceAsMap();

                        if (source.containsKey("value")) {
                            values.put(searchHit.getId(), source.get("value"));
                        } else if (source.containsKey("encrypted")) {
                            values.put(searchHit.getId(), encryptionKeys.getDecryptedData(source));
                        } else {
                            throw new Exception("Unexpected doc: " + Strings.toString(searchHit));
                        }
                    } catch (Exception e) {
                        componentState.getOrCreatePart("entry", searchHit.getId()).setFailed(e);
                        log.error("Error while reading " + searchHit, e);
                    }
                }

                response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(10000)).execute().actionGet();

            } while (response.getHits().getHits().length != 0);
        } finally {
            Actions.clearScrollAsync(client, response);
        }

        log.debug("Read " + values.size() + " secrets");

        return values;
    }

    public void refresh() {
        log.info("Refreshing config variables");
        threadPool.generic().submit(() -> refreshSync());
    }

    private synchronized void refreshSync() {
        try {
            componentState.setState(State.INITIALIZING, "refreshing");
            Map<String, Object> newValues = readValues();

            if (this.values == null || !this.values.equals(newValues)) {
                log.info("Config variables changed");
                this.values = newValues;
                componentState.setState(State.INITIALIZED);
                notifyChangeListeners();
            } else {
                componentState.setState(State.INITIALIZED);
                log.debug("Config variables did not change");
            }
        } catch (Exception e) {
            log.error("Error while refreshing. Trying again.", e);
            componentState.addLastException("refresh", e);

            threadPool.generic().submit(() -> {
                try {
                    Thread.sleep(10000 + new Random().nextInt(10000));
                    refreshSync();
                } catch (InterruptedException e1) {

                }
            });
        }
    }

    private static String generateKey(int bits) {
        SecureRandom random = new SecureRandom();
        byte bytes[] = new byte[bits / 8];
        random.nextBytes(bytes);
        return BaseEncoding.base64().encode(bytes);
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    private static class RequestedValue {
        final Supplier<Object> valueSupplier;
        final String scope;

        RequestedValue(Supplier<Object> valueSupplier, String scope) {
            this.valueSupplier = valueSupplier;
            this.scope = scope;
        }

        @Override
        public String toString() {
            return "[scope=" + scope + "]";
        }
    }

}
