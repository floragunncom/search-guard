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

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.proxy.rest.ProxyApi.CreateOrReplaceProxyAction.CreateOrReplaceProxyRequest;
import com.floragunn.signals.proxy.rest.ProxyRepresentation;
import com.floragunn.signals.proxy.service.persistence.ProxyData;
import com.floragunn.signals.proxy.service.persistence.ProxyRepository;
import com.floragunn.signals.watch.common.ProxyTypeProvider;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.Strings;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ProxyCrudService {

    private final ProxyRepository proxyRepository;

    public ProxyCrudService(ProxyRepository proxyRepository) {
        this.proxyRepository = proxyRepository;
    }

    public StandardResponse createOrReplace(CreateOrReplaceProxyRequest request) throws ConfigValidationException {
        ProxyData proxyData = toProxyData(request);
        proxyRepository.createOrReplace(proxyData);
        return new StandardResponse(200).data(ProxyRepresentation.of(proxyData).toBasicObject());
    }

    public StandardResponse findOne(String proxyId) throws NoSuchProxyException {
        requireNonNull(proxyId, "Proxy id is required");
        return proxyRepository.findOneById(proxyId)//
                .map(ProxyRepresentation::of)//
                .map(ProxyRepresentation::toBasicObject)//
                .map(proxyRepresentation -> new StandardResponse(200).data(proxyRepresentation))//
                .orElseThrow(() -> new NoSuchProxyException("Proxy with id '" + proxyId + "' not found."));
    }

    public StandardResponse delete(String proxyId) {
        requireNonNull(proxyId, "Proxy id is required");
        if (proxyRepository.isProxyUsedByAnyWatch(proxyId)) {
            return new StandardResponse(HttpStatus.SC_CONFLICT).error("The proxy is still in use");
        }
        return proxyRepository.deleteById(proxyId)? new StandardResponse(HttpStatus.SC_OK) :
                new StandardResponse(HttpStatus.SC_NOT_FOUND);
    }

    public StandardResponse findAll() {
        List<ImmutableMap<String, Object>> proxies = proxyRepository.findAll()//
                .stream()//
                .map(ProxyRepresentation::of)//
                .map(ProxyRepresentation::toBasicObject)//
                .collect(Collectors.toList());//
        return new StandardResponse(200).data(proxies);
    }

    Optional<ProxyData> findOneById(String proxyId) {
        return proxyRepository.findOneById(proxyId);
    }

    List<ProxyData> loadAll() {
        return proxyRepository.findAll();
    }

    private ProxyData toProxyData(CreateOrReplaceProxyRequest request) throws ConfigValidationException {
        final String id = request.getId();
        final String name = request.getName();
        final String uri = request.getUri();
        ValidationErrors validationErrors = new ValidationErrors();
        if (Strings.isNullOrEmpty(id)) {
            validationErrors.add(new ValidationError("id", "Id is required"));
        }
        if (ProxyTypeProvider.determineTypeBasedOnValue(id) != ProxyTypeProvider.Type.USE_STORED_PROXY) {
            String expectedMsg = String.format(
                    "String not equal to any of: (%s, %s) and not starting with any of: (%s)",
                    ProxyTypeProvider.DEFAULT_PROXY_KEYWORD, ProxyTypeProvider.NONE_PROXY_KEYWORD,
                    String.join(", ", ProxyTypeProvider.INLINE_PROXY_PREFIXES)
            );
            validationErrors.add(new InvalidAttributeValue("id", id, expectedMsg));
        }
        try {
            HttpHost.create(uri);
        } catch (Exception e) {
            validationErrors.add(new InvalidAttributeValue("uri", uri, "Valid URI"));
        }
        validationErrors.throwExceptionForPresentErrors();
        return new ProxyData(id, name, uri);
    }
}
