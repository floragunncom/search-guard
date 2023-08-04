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
import com.floragunn.signals.truststore.service.persistence.TruststoreData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.floragunn.signals.CertificatesParser.parseCertificates;
import static com.floragunn.signals.CertificatesParser.toTruststore;

public class TrustManagerRegistry {

    private static final Logger log = LogManager.getLogger(TrustManagerRegistry.class);

    private final TruststoreCrudService truststoreCrudService;
    private volatile Map<String, X509ExtendedTrustManager> trustManagerMap;

    public TrustManagerRegistry(TruststoreCrudService truststoreCrudService) {
        this.truststoreCrudService = Objects.requireNonNull(truststoreCrudService, "Truststore crud service is required");
        this.trustManagerMap = Collections.synchronizedMap(new HashMap<>());
        log.info("Truststore registry service created");
    }

    public void onTruststoreUpdate(String truststoreId, String operationType) {
        log.debug("Notification about operation '{}' on truststore '{}' received.", operationType, truststoreId);
        try {
            Optional<TruststoreData> truststoreData = truststoreCrudService.findOneById(truststoreId);
            if (truststoreData.isPresent()) {
                this.trustManagerMap.put(truststoreId, truststoreDataToTrustManager(truststoreData.get()));
            } else {
                this.trustManagerMap.remove(truststoreId);
                log.info("Truststore with id '{}' not found. Corresponding trust manager was removed.", truststoreId);
            }
            if(log.isInfoEnabled()) {
                String ids = getAvailableTrustManagersIds();
                log.info("Trust managers available after trust store updates: '{}'", ids);
            }
        } catch (KeyStoreException | NoSuchAlgorithmException | CannotCreateTrustManagerException | ConfigValidationException ex) {
            if(log.isDebugEnabled()) {
                String ids = getAvailableTrustManagersIds();
                log.debug("Cannot create trust manager for truststore '{}', available trust managers '{}'.", truststoreId, ids, ex);
            }
            throw new RuntimeException("Cannot update trust manager after operation '" + operationType +
                "' on trust store '" + truststoreId + "'.", ex);
        }
    }

    public void reloadAll() {
        List<TruststoreData> truststores = truststoreCrudService.loadAll();
        log.info("Loaded '{}' trust stores to init cache.", truststores.size());
        Map<String, X509ExtendedTrustManager> trustManagers = new HashMap<>();
        for(TruststoreData truststoreData : truststores) {
            try {
                X509ExtendedTrustManager x509TrustManager = truststoreDataToTrustManager(truststoreData);
                trustManagers.put(truststoreData.getId(), x509TrustManager);
            } catch (KeyStoreException | NoSuchAlgorithmException | CannotCreateTrustManagerException | ConfigValidationException e) {
                log.error("Cannot parse certificates in truststore '{}' or create trust manager. Truststore will be not available. Please check truststore data.",
                    truststoreData.getId(),  e);
            }
        }
        this.trustManagerMap = Collections.synchronizedMap(trustManagers);
        if(log.isInfoEnabled()) {
            String ids = getAvailableTrustManagersIds();
            log.info("Reloaded all trust stores and created trust managers, available trust managers: '{}'", ids);
        }
    }

    private X509ExtendedTrustManager truststoreDataToTrustManager(TruststoreData truststoreData)
        throws ConfigValidationException, KeyStoreException, NoSuchAlgorithmException, CannotCreateTrustManagerException {
        Collection<? extends Certificate> certificates = parseCertificates(truststoreData.getPem());
        KeyStore truststore = toTruststore(truststoreData.getId(), certificates);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(truststore);
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        List<TrustManager> x509TrustManagers = Arrays.stream(trustManagers)//
            .filter(X509ExtendedTrustManager.class::isInstance)//
            .collect(Collectors.toList());//
        if(x509TrustManagers.size() != 1) {
            throw new CannotCreateTrustManagerException("Incorrect number of x509 trust managers: " + x509TrustManagers.size());
        }
        return (X509ExtendedTrustManager) x509TrustManagers.get(0);
    }

    public Optional<X509ExtendedTrustManager> findTrustManager(String truststoreId) {
        Objects.requireNonNull(truststoreId, "Truststore id must not be null");
        Optional<X509ExtendedTrustManager> x509TrustManager = Optional.ofNullable(trustManagerMap.get(truststoreId));
        log.trace("Trust manager loaded by id '{}' is '{}'.", truststoreId, x509TrustManager);
        return x509TrustManager;
    }

    private String getAvailableTrustManagersIds() {
        return new HashSet<>(trustManagerMap.keySet()).stream().sorted().collect(Collectors.joining(", "));
    }
}
