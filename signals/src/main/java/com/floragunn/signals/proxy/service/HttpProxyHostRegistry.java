/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.signals.proxy.service;

import com.floragunn.signals.proxy.service.persistence.ProxyData;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class HttpProxyHostRegistry {

    private static final Logger log = LogManager.getLogger(HttpProxyHostRegistry.class);

    private final ProxyCrudService proxyCrudService;
    private volatile Map<String, HttpHost> proxyHostMap;

    public HttpProxyHostRegistry(ProxyCrudService proxyCrudService) {
        this.proxyCrudService = Objects.requireNonNull(proxyCrudService, "Proxy crud service is required");
        this.proxyHostMap = Collections.synchronizedMap(new HashMap<>());
        log.info("Http proxy host registry service created");
    }

    public void onProxyUpdate(String proxyId, String operationType) {
        log.debug("Notification about operation '{}' on proxy '{}' received.", operationType, proxyId);
        try {
            Optional<ProxyData> proxyData = proxyCrudService.findOneById(proxyId);
            if (proxyData.isPresent()) {
                this.proxyHostMap.put(proxyId, HttpHost.create(proxyData.get().getUri()));
            } else {
                this.proxyHostMap.remove(proxyId);
                log.info("Proxy with id '{}' not found. Corresponding http proxy host was removed.", proxyId);
            }
            if(log.isInfoEnabled()) {
                String ids = getAvailableProxyHostsIds();
                log.info("Http proxy hosts available after proxy update: '{}'", ids);
            }
        } catch (Exception e) {
            if(log.isDebugEnabled()) {
                String ids = getAvailableProxyHostsIds();
                log.debug("Cannot create http proxy host based on proxy with id '{}', available http proxy hosts '{}'.", proxyId, ids, e);
            }
            throw new RuntimeException("Cannot update http proxy host after operation '" + operationType +
                    "' on proxy '" + proxyId + "'.", e);
        }
    }

    public Optional<HttpHost> findHttpProxyHost(String proxyId) {
        Objects.requireNonNull(proxyId, "Proxy id must not be null");
        Optional<HttpHost> httpHost = Optional.ofNullable(proxyHostMap.get(proxyId));
        log.trace("Http proxy host loaded by id '{}' is '{}'.", proxyId, httpHost);
        return httpHost;
    }

    public void reloadAll() {
        List<ProxyData> proxies = proxyCrudService.loadAll();
        log.info("Loaded '{}' proxies to init cache.", proxies.size());
        Map<String, HttpHost> httpHosts = new HashMap<>();
        for(ProxyData proxyData : proxies) {
            try {
                httpHosts.put(proxyData.getId(), HttpHost.create(proxyData.getUri()));
            } catch (Exception e) {
                log.error("Cannot create http proxy host based on proxy data: {}. Proxy will be not available. Please check proxy data.",
                        proxyData.getId(),  e);
            }
        }
        this.proxyHostMap = Collections.synchronizedMap(httpHosts);
        if(log.isInfoEnabled()) {
            String ids = getAvailableProxyHostsIds();
            log.info("Reloaded all Http proxy hosts, available proxy hosts: '{}'", ids);
        }
    }

    private String getAvailableProxyHostsIds() {
        return new HashSet<>(proxyHostMap.keySet()).stream().sorted().collect(Collectors.joining(", "));
    }

}
