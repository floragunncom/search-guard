/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.cluster;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import com.floragunn.searchguard.test.helper.certificate.TestCertificate;

import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.PemUtils;

public class TestCertificateBasedSSLContextProvider implements SSLContextProvider {

    private final TestCertificate caCertificate;
    private final TestCertificate certificate;

    public TestCertificateBasedSSLContextProvider(TestCertificate caCertificate, TestCertificate certificate) {
        this.caCertificate = caCertificate;
        this.certificate = certificate;
    }

    @Override
    public SSLContext getSslContext(boolean clientAuthentication) {
        X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(caCertificate.getCertificateFile().toPath());

        SSLFactory.Builder builder = SSLFactory.builder().withTrustMaterial(trustManager);

        if (clientAuthentication) {
            X509ExtendedKeyManager keyManager;

            if (certificate.getPrivateKeyPassword() != null) {
                keyManager = PemUtils.loadIdentityMaterial(certificate.getCertificateFile().toPath(), certificate.getPrivateKeyFile().toPath(),
                        certificate.getPrivateKeyPassword().toCharArray());
            } else {
                keyManager = PemUtils.loadIdentityMaterial(certificate.getCertificateFile().toPath(), certificate.getPrivateKeyFile().toPath());
            }

            builder.withIdentityMaterial(keyManager);
        }

        return builder.build().getSslContext();
    }
}
