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

package com.floragunn.searchguard;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.component.Lifecycle.State;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 * Very hackish way to get hold to Guice components from Non-Guice components. Was earlier GuiceHolder in SearchGuardPlugin
 */
public class GuiceDependencies {

    private RepositoriesService repositoriesService;
    private TransportService transportService;
    private IndicesService indicesService;

    /**
     * Can be only used via instance from SearchGuardPlugin
     */
    GuiceDependencies() {

    }

    public RepositoriesService getRepositoriesService() {
        return repositoriesService;
    }

    public TransportService getTransportService() {
        return transportService;
    }

    public IndicesService getIndicesService() {
        return indicesService;
    }

    public void setRepositoriesService(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    public void setIndicesService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

}
