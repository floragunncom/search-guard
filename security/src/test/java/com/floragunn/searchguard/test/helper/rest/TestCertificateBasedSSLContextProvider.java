package com.floragunn.searchguard.test.helper.rest;

import com.floragunn.searchguard.test.helper.certificate.TestCertificate;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.PemUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.util.Optional;

public class TestCertificateBasedSSLContextProvider implements SSLContextProvider {

    private final TestCertificate caCertificate;
    private final TestCertificate certificate;

    public TestCertificateBasedSSLContextProvider(TestCertificate caCertificate,
                                                  TestCertificate certificate) {
        this.caCertificate = caCertificate;
        this.certificate = certificate;
    }

    @Override
    public SSLContext getSslContext(boolean clientAuthentication) {
        X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(caCertificate.getCertificateFile().toPath());

        SSLFactory.Builder builder = SSLFactory.builder()
                .withTrustMaterial(trustManager);

        if (clientAuthentication) {
            X509ExtendedKeyManager keyManager = Optional.ofNullable(certificate.getPrivateKeyPassword())
                    .map(password -> PemUtils.loadIdentityMaterial(
                            certificate.getCertificateFile().toPath(),
                            certificate.getPrivateKeyFile().toPath(),
                            password.toCharArray()))
                    .orElse(PemUtils.loadIdentityMaterial(
                            certificate.getCertificateFile().toPath(),
                            certificate.getPrivateKeyFile().toPath()));
            builder.withIdentityMaterial(keyManager);
        }

        return builder.build().getSslContext();
    }
}
