/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.truststore.service;


import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.signals.truststore.rest.CreateOrReplaceTruststoreAction.CreateOrReplaceTruststoreRequest;
import com.floragunn.signals.truststore.rest.TruststoreRepresentation;
import com.floragunn.signals.truststore.service.persistence.TruststoreRepository;
import com.floragunn.signals.truststore.rest.CertificateRepresentation;
import com.floragunn.signals.truststore.service.persistence.TruststoreData;
import com.floragunn.searchsupport.action.StandardRequests.IdRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import org.apache.http.HttpStatus;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TruststoreCrudService {
    private final CertificateParser certificateParser;
    private final ConversionService conversionService;
    private final TruststoreRepository truststoreRepository;

    public TruststoreCrudService(TruststoreRepository truststoreRepository) {
        this.truststoreRepository = requireNonNull(truststoreRepository, "Repository must not be null");
        this.certificateParser = new CertificateParser();
        this.conversionService = new ConversionService();
    }

    public StandardResponse createOrReplace(CreateOrReplaceTruststoreRequest request) throws ConfigValidationException {
            TruststoreRepresentation representation = createUploadCertificateResponse(request);
            TruststoreData truststoreData = conversionService.representationToTruststoreData(representation);
            String truststoreId = request.getId();
            truststoreRepository.createOrReplace(truststoreId, truststoreData);
            return new StandardResponse(HttpStatus.SC_OK).data(representation.toBasicObject());
    }

    private TruststoreRepresentation createUploadCertificateResponse(CreateOrReplaceTruststoreRequest request)
        throws ConfigValidationException {
        List<String> certificates = request.getCertificates();
        if(certificates.isEmpty()) {
            ValidationError error = new ValidationError(null, "Cannot extract certificates from provided PEM string.");
            throw new ConfigValidationException(error);
        }
        ImmutableList<CertificateRepresentation> certificateRepresentations = certificateParser.parse(certificates);
        return new TruststoreRepresentation(request.getId(), request.getName(), request.getPem(), certificateRepresentations);
    }

    public StandardResponse findOne(IdRequest request) throws NoSuchTruststoreException {
        requireNonNull(request, "Get truststore request is required");
        String notFoundMessage = "Truststore with id " + request.getId() + " not found.";
        return truststoreRepository.findOneById(request.getId())//
            .map(conversionService::truststoreDataToRepresentation)//
                .map(TruststoreRepresentation::toBasicObject)//
                .map(truststoreRepresentation -> new StandardResponse(HttpStatus.SC_OK).data(truststoreRepresentation))//
                .orElseThrow(() -> new NoSuchTruststoreException(notFoundMessage));

    }

    public StandardResponse findAll() {
        List<ImmutableMap<String, Object>> truststores = truststoreRepository.findAll()//
            .stream()//
            .map(conversionService::truststoreDataToRepresentation)//
            .map(TruststoreRepresentation::toBasicObject)//
            .collect(Collectors.toList());//
        return new StandardResponse(HttpStatus.SC_OK).data(truststores);
    }

    public StandardResponse delete(IdRequest request) {
        if (truststoreRepository.isTruststoreUsedByAnyWatch(request.getId())) {
            return new StandardResponse(HttpStatus.SC_CONFLICT).error("The truststore is still in use");
        }
        boolean deleted = truststoreRepository.deleteById(request.getId());
        return deleted ? new StandardResponse(HttpStatus.SC_OK) : new StandardResponse(HttpStatus.SC_NOT_FOUND);
    }

    List<TruststoreData> loadAll() {
        return truststoreRepository.findAll();
    }

    Optional<TruststoreData> findOneById(String truststoreId) {
        return truststoreRepository.findOneById(truststoreId);
    }
}
