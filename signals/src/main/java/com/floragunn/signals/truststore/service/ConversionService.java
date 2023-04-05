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

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.signals.truststore.rest.CertificateRepresentation;
import com.floragunn.signals.truststore.rest.TruststoreRepresentation;
import com.floragunn.signals.truststore.service.persistence.CertificateData;
import com.floragunn.signals.truststore.service.persistence.TruststoreData;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

class ConversionService {

    TruststoreData representationToTruststoreData(TruststoreRepresentation representation) {
        List<CertificateData> certificates = representation.getCertificates()//
            .stream()//
            .map(this::toCertificateData)//
            .collect(Collectors.toList());
        return new TruststoreData(representation.getId(), representation.getName(), Instant.now(),//
            representation.getPem(), ImmutableList.of(certificates));
    }

    TruststoreRepresentation truststoreDataToRepresentation(TruststoreData truststoreData) {
        List<CertificateRepresentation> certificates = truststoreData.getCertificates()
            .stream()//
            .map(this::toCertificateRepresentation)//
            .collect(Collectors.toList());
        return new TruststoreRepresentation(truststoreData.getId(), truststoreData.getName(), truststoreData.getPem(),//
            ImmutableList.of(certificates));
    }

    private CertificateRepresentation toCertificateRepresentation(CertificateData certificateData) {
        return new CertificateRepresentation(certificateData.getSerialNumber(), certificateData.getNotBefore(),//
            certificateData.getNotAfter(), certificateData.getIssuer(), certificateData.getSubject(), certificateData.getPem());
    }

    private CertificateData toCertificateData(CertificateRepresentation representation) {
        return new CertificateData(representation.getSerialNumber(), representation.getNotBefore(), representation.getNotAfter(),//
            representation.getIssuer(), representation.getSubject(), representation.getPem());
    }
}
