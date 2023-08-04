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
package com.floragunn.signals.watch.common;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.function.Supplier;

class RefreshableX509TrustManager extends X509ExtendedTrustManager {

    private static final Logger log = LogManager.getLogger(RefreshableX509TrustManager.class);

    private final String name;
    private final LocalDateTime creationTime;
    private final Supplier<X509ExtendedTrustManager> trustManagerSupplier;

    public RefreshableX509TrustManager(String name, Supplier<X509ExtendedTrustManager> trustManagerSupplier) {
        this.name = name;
        this.creationTime = LocalDateTime.now();
        this.trustManagerSupplier = Objects.requireNonNull(trustManagerSupplier);
        log.info("Refreshable x509 trust manager '{}' created at '{}'.", name, creationTime);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        getTrustManager().checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        getTrustManager().checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        getTrustManager().checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        getTrustManager().checkServerTrusted(chain, authType, engine);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        getTrustManager().checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        getTrustManager().checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return getTrustManager().getAcceptedIssuers();
    }

    private X509ExtendedTrustManager getTrustManager() {
        X509ExtendedTrustManager x509TrustManager = trustManagerSupplier.get();
        log.trace("Retrieved trust manager '{}'", x509TrustManager);
        return x509TrustManager;
    }
    @Override
    public String toString() {
        return "RefreshableX509TrustManager{" + "name='" + name + '\'' + ", creationTime=" + creationTime + '}';
    }
}
