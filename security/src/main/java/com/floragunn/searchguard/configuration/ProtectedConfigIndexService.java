/*
 * Copyright 2020-2021 floragunn GmbH
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.searchguard.modules.state.ComponentState;

public class ProtectedConfigIndexService {
    private final static Logger log = LogManager.getLogger(ProtectedConfigIndexService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ProtectedIndices protectedIndices;

    private final Set<ConfigIndexState> pendingIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<ConfigIndexState> completedIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private volatile boolean ready = false;

    public ProtectedConfigIndexService(Client client, ClusterService clusterService, ThreadPool threadPool, ProtectedIndices protectedIndices) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.protectedIndices = protectedIndices;

        clusterService.addListener(clusterStateListener);
    }

    public ComponentState createIndex(ConfigIndex configIndex) {
        ConfigIndexState configIndexState = new ConfigIndexState(configIndex);

        protectedIndices.add(configIndex.getName());

        if (!ready) {
            pendingIndices.add(configIndexState);
        } else {
            createIndexNow(configIndexState, clusterService.state());
        }

        return configIndexState.moduleState;
    }

    public void flushPendingIndices(ClusterState clusterState) {
        try {
            if (this.pendingIndices.isEmpty()) {
                return;
            }

            Set<ConfigIndexState> pendingIndices = new HashSet<>(this.pendingIndices);

            this.pendingIndices.removeAll(pendingIndices);

            for (ConfigIndexState configIndex : pendingIndices) {
                createIndexNow(configIndex, clusterState);
            }
        } catch (Exception e) {
            log.error("Error in flushPendingIndices()", e);
        }
    }

    public void onNodeStart() {
        ready = true;

        checkClusterState(clusterService.state());
    }

    private void checkClusterState(ClusterState clusterState) {
        if (!ready) {
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("checkClusterState()\npendingIndices: " + pendingIndices);
        }

        if (clusterState.nodes().isLocalNodeElectedMaster() || clusterState.nodes().getMasterNode() != null) {
            flushPendingIndices(clusterState);
        }

        if (!this.pendingIndices.isEmpty()) {
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(30), ThreadPool.Names.GENERIC,
                    () -> checkClusterState(clusterService.state()));
        }
    }

    private void createIndexNow(ConfigIndexState configIndex, ClusterState clusterState) {

        if (log.isTraceEnabled()) {
            log.trace("createIndexNow(" + configIndex + ")");
        }

        if (completedIndices.contains(configIndex)) {
            if (log.isTraceEnabled()) {
                log.trace(configIndex + " is already completed");
            }
            return;
        }

        if (clusterState.getMetadata().getIndices().containsKey(configIndex.getName())) {
            if (log.isTraceEnabled()) {
                log.trace(configIndex + " does already exist.");
            }

            completedIndices.add(configIndex);
            configIndex.setCreated(true);

            if (configIndex.getListener() != null) {
                configIndex.waitForYellowStatus();
            } else {
                configIndex.moduleState.setInitialized();
            }

            return;
        }

        if (!clusterState.nodes().isLocalNodeElectedMaster()) {
            pendingIndices.add(configIndex);
            configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "waiting_for_master");
            return;
        }

        CreateIndexRequest request = new CreateIndexRequest(configIndex.getName());

        if (configIndex.getMapping() != null) {
            request.mapping("_doc", configIndex.getMapping());
        }

        request.settings(Settings.builder().put("index.hidden", true));

        if (log.isDebugEnabled()) {
            log.debug("Creating index " + request.index() + ":\n" + Strings.toString(request, true, true));
        }

        completedIndices.add(configIndex);
        configIndex.moduleState.setState(ComponentState.State.INITIALIZING, "creating");

        client.admin().indices().create(request, new ActionListener<CreateIndexResponse>() {

            @Override
            public void onResponse(CreateIndexResponse response) {
                configIndex.setCreated(true);

                if (log.isDebugEnabled()) {
                    log.debug("Created " + configIndex + ": " + Strings.toString(response));
                }

                if (configIndex.getListener() != null) {
                    configIndex.waitForYellowStatus();
                } else {
                    configIndex.moduleState.setInitialized();
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResourceAlreadyExistsException) {
                    configIndex.setCreated(true);

                    if (configIndex.getListener() != null) {
                        configIndex.waitForYellowStatus();
                    } else {
                        configIndex.moduleState.setInitialized();
                    }
                } else {
                    log.error("Error while creating index " + configIndex, e);
                    configIndex.setFailed(e);
                }
            }
        });

    }

    private final ClusterStateListener clusterStateListener = new ClusterStateListener() {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            checkClusterState(event.state());
        }
    };

    private class ConfigIndexState {
        private final String name;
        private final Map<String, Object> mapping;
        private final IndexReadyListener listener;
        private final String[] allIndices;
        private final ComponentState moduleState;
        private volatile long createdAt;

        ConfigIndexState(ConfigIndex configIndex) {
            this.name = configIndex.name;
            this.mapping = configIndex.mapping;
            this.listener = configIndex.listener;
            this.moduleState = new ComponentState(5, "index", configIndex.name);

            if (configIndex.indexDependencies == null || configIndex.indexDependencies.length == 0) {
                allIndices = new String[] { name };
            } else {
                allIndices = new String[configIndex.indexDependencies.length + 1];
                allIndices[0] = name;
                System.arraycopy(configIndex.indexDependencies, 0, allIndices, 1, configIndex.indexDependencies.length);
            }
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }

        @Override
        public String toString() {
            return "ConfigIndex [name=" + name + "]";
        }

        public void setFailed(Exception failed) {
            this.moduleState.setFailed(failed);
        }

        public void setCreated(boolean created) {
            if (created) {
                this.moduleState.setInitialized();
                this.createdAt = System.currentTimeMillis();
            }
        }

        public IndexReadyListener getListener() {
            return listener;
        }

        public void waitForYellowStatus() {
            if (log.isTraceEnabled()) {
                log.trace("waitForYellowStatus(" + this + ")");
            }

            this.moduleState.setState(ComponentState.State.INITIALIZING, "waiting_for_yellow_status");
            this.moduleState.startNextTry();

            client.admin().cluster().health(new ClusterHealthRequest(allIndices).waitForYellowStatus().timeout(TimeValue.timeValueMinutes(5)),
                    new ActionListener<ClusterHealthResponse>() {

                        @Override
                        public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                            if (clusterHealthResponse.getStatus() == ClusterHealthStatus.YELLOW
                                    || clusterHealthResponse.getStatus() == ClusterHealthStatus.GREEN) {

                                if (log.isDebugEnabled()) {
                                    log.debug(ConfigIndexState.this + " reached status " + Strings.toString(clusterHealthResponse));
                                }

                                threadPool.generic().submit(() -> tryOnIndexReady());

                                return;
                            }

                            if (isTimedOut()) {
                                moduleState.setFailed("Index " + name + " is has not become ready. Giving up");
                                moduleState.setDetailJson(Strings.toString(clusterHealthResponse));
                                log.error("Index " + name + " is has not become ready:\n" + clusterHealthResponse + "\nGiving up.");
                                return;
                            }

                            if (isLate()) {
                                log.error("Index " + name + " is not yet ready:\n" + clusterHealthResponse + "\nRetrying.");
                                moduleState.setDetailJson(Strings.toString(clusterHealthResponse));
                            } else if (log.isTraceEnabled()) {
                                log.trace("Index " + name + " is not yet ready:\n" + clusterHealthResponse + "\nRetrying.");
                            }

                            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC,
                                    () -> waitForYellowStatus());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (isTimedOut()) {
                                log.error("Index " + name + " is has not become ready. Giving up.", e);
                                moduleState.setFailed(e);
                                return;
                            }

                            if (isLate()) {
                                log.warn("Index " + name + " is not yet ready. Retrying.", e);
                                moduleState.addLastException("waiting_for_yellow_status", e);
                            } else if (log.isTraceEnabled()) {
                                log.trace("Index " + name + " is not yet ready. Retrying.", e);
                            }

                            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC,
                                    () -> waitForYellowStatus());
                        }
                    });
        }

        private void tryOnIndexReady() {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("tryOnIndexReady(" + this + ")");
                }

                this.moduleState.setState(ComponentState.State.INITIALIZING, "final_probe");
                this.moduleState.startNextTry();

                listener.onIndexReady(new FailureListener() {

                    @Override
                    public void onFailure(Exception e) {
                        if (isTimedOut()) {
                            log.error("Initialization for " + name + " failed. Giving up.", e);
                            moduleState.setFailed(e);
                            return;
                        }

                        if (isLate()) {
                            log.warn("Initialization for " + name + " not yet successful. Retrying.", e);
                        } else if (log.isTraceEnabled()) {
                            log.trace("Initialization for " + name + " not yet successful. Retrying.", e);
                        }

                        threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC, () -> tryOnIndexReady());

                    }

                    @Override
                    public void onSuccess() {
                        moduleState.setInitialized();
                    }

                });

            } catch (Exception e) {
                log.error("Error in onIndexReady of " + this, e);
            }
        }

        private boolean isTimedOut() {
            return System.currentTimeMillis() > (createdAt + 24 * 60 * 60 * 1000);
        }

        private boolean isLate() {
            return System.currentTimeMillis() > (createdAt + 60 * 1000);
        }

    }

    public static class ConfigIndex {
        private String name;
        private Map<String, Object> mapping;
        private IndexReadyListener listener;
        private String[] indexDependencies = new String[0];

        public ConfigIndex(String name) {
            this.name = name;
        }

        public ConfigIndex mapping(Map<String, Object> mapping) {
            this.mapping = mapping;
            return this;
        }

        public ConfigIndex onIndexReady(IndexReadyListener listener) {
            this.listener = listener;
            return this;
        }

        public ConfigIndex dependsOnIndices(String... indexDependencies) {
            if (indexDependencies != null) {
                this.indexDependencies = indexDependencies;
            }
            return this;
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }

    }

    @FunctionalInterface
    public static interface IndexReadyListener {
        void onIndexReady(FailureListener failureListener);
    }

    public static interface FailureListener {
        void onSuccess();

        void onFailure(Exception e);
    }

}
