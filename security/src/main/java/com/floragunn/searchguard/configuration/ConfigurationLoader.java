/*
 * Copyright 2015-2021 floragunn GmbH
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

package com.floragunn.searchguard.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.MultiGetResponse.Failure;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.PrivilegedConfigClient;

public class ConfigurationLoader {
    private static final Logger log = LogManager.getLogger(ConfigurationLoader.class);

    private final PrivilegedConfigClient client;
    private final String searchguardIndex;
    private final ClusterService clusterService;
    private final Settings settings;
    private final ComponentState componentState;
    private final Map<CType<?>, ComponentState> typeToStateMap;

    public ConfigurationLoader(Client client, Settings settings) {
        this(client, settings, null, null);
    }

    public ConfigurationLoader(Client client, Settings settings, ClusterService clusterService, ComponentState componentState) {
        this.client = PrivilegedConfigClient.adapt(client);
        this.settings = settings;
        this.searchguardIndex = settings.get(ConfigConstants.SEARCHGUARD_CONFIG_INDEX_NAME, ConfigConstants.SG_DEFAULT_CONFIG_INDEX);
        this.clusterService = clusterService;
        this.componentState = componentState;

        if (componentState != null) {
            typeToStateMap = new HashMap<>(CType.all().size());

            for (CType<?> ctype : CType.all()) {
                typeToStateMap.put(ctype, componentState.getOrCreatePart("config", ctype.toLCString()));
            }
        } else {
            typeToStateMap = null;
        }
    }

    public <T> SgDynamicConfiguration<T> loadSync(CType<T> type, String reason) throws ConfigUnavailableException {
        try {
            return load(type, reason).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ConfigUnavailableException) {
                throw (ConfigUnavailableException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public ConfigMap loadSync(Set<CType<?>> types, String reason) throws ConfigUnavailableException {
        try {
            return load(types, reason).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ConfigUnavailableException) {
                throw (ConfigUnavailableException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public <T> CompletableFuture<SgDynamicConfiguration<T>> load(CType<T> type, String reason) {
        return load(Collections.singleton(type), reason).thenApply(configMap -> configMap.get(type));
    }

    public CompletableFuture<ConfigMap> load(Set<CType<?>> types, String reason) {
        MultiGetRequest mget = new MultiGetRequest().refresh(true).realtime(true);

        Set<CType<?>> expectedTypes = new HashSet<>();

        for (CType<?> cType : types) {
            if (!cType.isExternal()) {
                mget.add(searchguardIndex, cType.toLCString());
                expectedTypes.add(cType);
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("Issuing " + mget);
        }

        CompletableFuture<ConfigMap> resultFuture = new CompletableFuture<>();

        client.multiGet(mget, new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse response) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Response: " + Arrays.asList(response.getResponses()).stream()
                                .map(r -> r.getId() + ": failure: " + r.getFailure() + "; exists: " + r.getResponse().isExists() + "; sourceEmpty: "
                                        + r.getResponse().isSourceEmpty() + "; version: " + r.getResponse().getVersion() + "; seqno: "
                                        + r.getResponse().getSeqNo() + "; pt: " + r.getResponse().getPrimaryTerm() + "; size: "
                                        + (r.getResponse().getSourceAsBytes() != null ? r.getResponse().getSourceAsBytes().length : "null")));
                    }

                    List<Failure> failures = new ArrayList<>();
                    ConfigMap.Builder configMapBuilder = new ConfigMap.Builder();

                    for (MultiGetItemResponse item : response.getResponses()) {
                        CType<?> type = item.getId() != null ? CType.fromString(item.getId()) : null;

                        if (item.isFailed()) {
                            failures.add(item.getFailure());
                            failure(type, item.getFailure(), typeToStateMap);
                            continue;
                        }

                        try {
                            SgDynamicConfiguration<?> config = toConfig(type, item.getResponse());
                            configMapBuilder.with(config);
                            success(config, typeToStateMap);
                        } catch (Exception e) {
                            Failure failure = new Failure(searchguardIndex, item.getResponse().getType(), item.getResponse().getId(), e);
                            failures.add(failure);
                            failure(type, failure, typeToStateMap);
                        }
                    }

                    ConfigMap result = configMapBuilder.build();

                    if (result.containsAll(expectedTypes)) {
                        resultFuture.complete(result);
                    } else {
                        throw new ConfigUnavailableException(
                                "Error while loading configuration (for " + reason + "):\n"
                                        + failures.stream().map(f -> Strings.toString(f)).collect(Collectors.toList()),
                                !failures.isEmpty() ? failures.get(0).getFailure() : null);
                    }
                } catch (ConfigUnavailableException e) {
                    log.warn("Error while loading config", e);
                    resultFuture.completeExceptionally(e);
                } catch (Throwable e) {
                    log.error("Error while loading config", e);
                    resultFuture.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Error while loading config", e);
                resultFuture.completeExceptionally(e);
            }
        });

        return resultFuture;
    }

    private SgDynamicConfiguration<?> toConfig(CType<?> type, GetResponse getResponse) throws Exception {
        if (!getResponse.isExists()) {
            if (type != null && type.isOptional()) {
                return SgDynamicConfiguration.empty();
            } else {
                throw new Exception("Document does not exist");
            }
        }

        if (getResponse.isSourceEmpty()) {
            throw new Exception("Document source is empty");
        }

        BytesReference source = getResponse.getSourceAsBytesRef();
        String id = getResponse.getId();

        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source,
                XContentType.JSON)) {
            parser.nextToken();
            parser.nextToken();

            if (!id.equals(parser.currentName())) {
                throw new Exception("Invalid config index: " + id + " vs " + parser.currentName());
            }

            parser.nextToken();

            return SgDynamicConfiguration.fromJson(new String(parser.binaryValue()), type, getResponse.getVersion(), getResponse.getSeqNo(),
                    getResponse.getPrimaryTerm(), settings);
        }
    }

    private void success(SgDynamicConfiguration<?> config, Map<CType<?>, ComponentState> typeToStateMap) {
        if (typeToStateMap == null) {
            return;
        }

        ComponentState configState = typeToStateMap.get(config.getCType());

        if (configState != null) {
            configState.setInitialized();
            configState.setConfigVersion(config.getDocVersion());
        }
    }

    private void failure(CType<?> type, Failure failure, Map<CType<?>, ComponentState> typeToStateMap) {
        if (type == null || typeToStateMap == null) {
            return;
        }

        ComponentState configState = typeToStateMap.get(type);

        if (configState != null) {
            configState.setFailed(failure.getMessage());
            configState.setDetailJson(Strings.toString(failure));
        }
    }
}
