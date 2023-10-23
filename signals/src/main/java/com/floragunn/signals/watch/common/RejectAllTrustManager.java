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

class RejectAllTrustManager extends X509ExtendedTrustManager {

    private static final Logger log = LogManager.getLogger(RejectAllTrustManager.class);

    private final String truststoreId;

    public RejectAllTrustManager(String truststoreId) {
        this.truststoreId = truststoreId;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        throwExceptionRelatedToInvalidCertificates();
    }

    private void throwExceptionRelatedToInvalidCertificates() throws CertificateException {
        String message = "Watch uses not existing truststore with id " + truststoreId + ". Please correct watch or create truststore.";
        log.warn(message);
        throw new CertificateException(message);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        throwExceptionRelatedToInvalidCertificates();
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    @Override
    public String toString() {
        return "RejectAllTrustManager{" + "truststoreId='" + truststoreId + '\'' + '}';
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        throwExceptionRelatedToInvalidCertificates();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        throwExceptionRelatedToInvalidCertificates();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        throwExceptionRelatedToInvalidCertificates();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        throwExceptionRelatedToInvalidCertificates();
    }
}
