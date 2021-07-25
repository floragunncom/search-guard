/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.configuration.secrets;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.ConfigIndex;
import com.floragunn.searchguard.configuration.ProtectedConfigIndexService.FailureListener;
import com.floragunn.searchguard.configuration.secrets.SecretsRefreshAction.Response;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.client.Actions;
import com.google.common.io.BaseEncoding;

public class SecretsService implements ComponentStateProvider {
    private final static Logger log = LogManager.getLogger(SecretsService.class);

    private final Client client;
    private final PrivilegedConfigClient privilegedConfigClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Map<String, Supplier<String>> requestedValues = new ConcurrentHashMap<>();
    private final ComponentState componentState = new ComponentState(1000, null, "secrets_storage", SecretsService.class);
    private final String indexName = ".searchguard_secrets";
    private Map<String, Object> values;
    private final List<Runnable> changeListeners = new ArrayList<>();

    public SecretsService(Client client, ClusterService clusterService, ThreadPool threadPool,
            ProtectedConfigIndexService protectedConfigIndexService) {
        this.client = client;
        this.privilegedConfigClient = PrivilegedConfigClient.adapt(client);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        componentState.addPart(protectedConfigIndexService.createIndex(new ConfigIndex(indexName).onIndexReady((onFailure) -> init(onFailure))));
    }

    public Object get(String id) {
        if (values == null) {
            throw new ElasticsearchException("SecretsService is not yet initialized");
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

    public void delete(String id, ActionListener<SecretsConfigApi.DeleteAction.Response> actionListener) {
        this.privilegedConfigClient.delete(new DeleteRequest(indexName, id).setRefreshPolicy(RefreshPolicy.IMMEDIATE), new ActionListener<DeleteResponse>() {

            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                SecretsRefreshAction.send(client, new ActionListener<Response>() {

                    @Override
                    public void onResponse(Response response) {
                        try {
                            log.info("Result of settings update:\n" + response);

                            if (response.hasFailures()) {
                                actionListener.onResponse(new SecretsConfigApi.DeleteAction.Response(id, RestStatus.INTERNAL_SERVER_ERROR,
                                        "Index update was successful, but node refresh partially failed",
                                        DocWriter.writeAsString(response.failures().toString())));
                            } else if (deleteResponse.getResult() == Result.DELETED) {
                                actionListener.onResponse(new SecretsConfigApi.DeleteAction.Response(id, RestStatus.OK, "Deleted"));
                            } else { 
                                actionListener.onResponse(new SecretsConfigApi.DeleteAction.Response(id, RestStatus.NOT_FOUND, "Not found"));
                            }
                        } catch (Exception e) {
                            log.error("Error in onResponse", e);
                            actionListener.onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("settings update failed", e);
                        actionListener.onResponse(new SecretsConfigApi.DeleteAction.Response(id, RestStatus.INTERNAL_SERVER_ERROR,
                                "Index update was successful, but node refresh failed"));
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while deleting " + id, e);
                componentState.addLastException("delete", e);
                actionListener.onFailure(e);
            }
        });
    }

    public void update(String id, Object value, ActionListener<SecretsConfigApi.UpdateAction.Response> actionListener) {
        String json = DocWriter.writeAsString(value);

        log.info("Writing secret " + id);

        this.privilegedConfigClient.index(new IndexRequest(indexName).setRefreshPolicy(RefreshPolicy.IMMEDIATE).id(id).source("value", json),
                new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        if (indexResponse.getResult() == Result.CREATED || indexResponse.getResult() == Result.UPDATED) {
                            SecretsRefreshAction.send(client, new ActionListener<Response>() {

                                @Override
                                public void onResponse(Response response) {
                                    try {
                                        log.info("Result of settings update:\n" + response);

                                        if (response.hasFailures()) {
                                            actionListener.onResponse(new SecretsConfigApi.UpdateAction.Response(id, RestStatus.INTERNAL_SERVER_ERROR,
                                                    "Index update was successful, but node refresh partially failed",
                                                    DocWriter.writeAsString(response.failures().toString())));
                                        } else if (indexResponse.getResult() == Result.CREATED) {
                                            actionListener.onResponse(new SecretsConfigApi.UpdateAction.Response(id, RestStatus.OK, "Created"));
                                        } else { // indexResponse.getResult() == Result.UPDATED
                                            actionListener.onResponse(new SecretsConfigApi.UpdateAction.Response(id, RestStatus.OK, "Updated"));
                                        }
                                    } catch (Exception e) {
                                        log.error("Error in onResponse", e);
                                        actionListener.onFailure(e);
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    log.error("settings update failed", e);
                                    actionListener.onResponse(new SecretsConfigApi.UpdateAction.Response(id, RestStatus.INTERNAL_SERVER_ERROR,
                                            "Index update was successful, but node refresh failed"));
                                }
                            });
                        } else if (indexResponse.getResult() == Result.NOOP) {
                            actionListener.onResponse(new SecretsConfigApi.UpdateAction.Response(id, RestStatus.OK, "Not updated"));
                        } else {
                            actionListener.onResponse(
                                    new SecretsConfigApi.UpdateAction.Response(id, RestStatus.INTERNAL_SERVER_ERROR, indexResponse.getResult() + ""));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Error while updating " + id, e);
                        componentState.addLastException("update", e);
                        actionListener.onFailure(e);
                    }
                });
    }

    public void updateAll(Map<String, Object> valueMap, ActionListener<SecretsConfigApi.UpdateAllAction.Response> actionListener) {
        BulkRequest bulkRequest = new BulkRequest();

        bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            bulkRequest.add(new IndexRequest(indexName).id(entry.getKey()).source(DocWriter.writeAsString(entry.getValue(), XContentType.JSON)));
        }

        Set<String> idsForDeletion = this.values.keySet().stream().filter((id) -> !valueMap.containsKey(id)).collect(Collectors.toSet());

        for (String idForDeletion : idsForDeletion) {
            bulkRequest.add(new DeleteRequest(indexName).id(idForDeletion));
        }

        if (bulkRequest.numberOfActions() == 0) {
            actionListener.onResponse(new SecretsConfigApi.UpdateAllAction.Response(RestStatus.OK, "Nothing to update", null));
            return;
        }

        this.privilegedConfigClient.bulk(bulkRequest, new ActionListener<BulkResponse>() {

            @Override
            public void onResponse(BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    actionListener.onResponse(new SecretsConfigApi.UpdateAllAction.Response(RestStatus.INTERNAL_SERVER_ERROR,
                            "Bulk update partially failed", Strings.toString(bulkResponse)));
                } else {
                    SecretsRefreshAction.send(client, new ActionListener<Response>() {

                        @Override
                        public void onResponse(Response response) {
                            log.info("Result of settings update:\n" + response);

                            if (response.hasFailures()) {
                                actionListener.onResponse(new SecretsConfigApi.UpdateAllAction.Response(RestStatus.INTERNAL_SERVER_ERROR,
                                        "Index update was successful, but node refresh partially failed",
                                        DocWriter.writeAsString(response.failures().toString())));
                            } else {
                                actionListener.onResponse(
                                        new SecretsConfigApi.UpdateAllAction.Response(RestStatus.OK, getUpdateMessage(bulkResponse), null));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            log.error("settings update failed", e);
                            actionListener.onResponse(new SecretsConfigApi.UpdateAllAction.Response(RestStatus.INTERNAL_SERVER_ERROR,
                                    "Index update was successful, but node refresh failed", null));
                        }
                    });
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while updating secrets", e);
                componentState.addLastException("update", e);
                actionListener.onFailure(e);
            }
        });
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

    public synchronized void requestRandomKey(String id, int bits) {
        requestedValues.put(id, () -> generateKey(bits));
    }

    public synchronized void requestValue(String id, Supplier<String> supplier) {
        requestedValues.put(id, supplier);
    }

    public Map<String, Object> getAll() {
        if (values == null) {
            throw new ElasticsearchException("SecretsService is not yet initialized");
        }

        return Collections.unmodifiableMap(values);
    }

    private synchronized void init(FailureListener failureListener) {
        Map<String, Supplier<String>> requestedValues = new HashMap<>(this.requestedValues);

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

                HashMap<String, String> newValues = new HashMap<>();

                for (Map.Entry<String, Supplier<String>> entry : requestedValues.entrySet()) {
                    String id = entry.getKey();

                    try {
                        if (existingValues.containsKey(id)) {
                            continue;
                        }

                        String value = entry.getValue().get();
                        newValues.put(id, value);

                        String valueAsJson = DocWriter.writeAsString(value);

                        IndexResponse response = privilegedConfigClient
                                .index(new IndexRequest(this.indexName).id(id).source("value", valueAsJson).setRefreshPolicy(RefreshPolicy.IMMEDIATE))
                                .actionGet();

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
                    SecretsRefreshAction.send(client);
                }
            }

            values = existingValues;

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
                        String valueJson = String.valueOf(searchHit.getSourceAsMap().get("value"));
                        Object value = DocReader.read(valueJson);
                        values.put(searchHit.getId(), value);
                    } catch (Exception e) {
                        componentState.addLastException("readValues", e);
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
        log.info("Refreshing secrets");
        threadPool.generic().submit(() -> refreshSync());
    }

    private synchronized void refreshSync() {
        try {
            componentState.setState(State.INITIALIZING, "refreshing");
            Map<String, Object> newValues = readValues();

            if (this.values == null || !this.values.equals(newValues)) {
                log.info("Secrets changed");
                this.values = newValues;
                componentState.setState(State.INITIALIZED);
                notifyChangeListeners();
            } else {
                componentState.setState(State.INITIALIZED);
                log.debug("Secrets did not change");
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

}
