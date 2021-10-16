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
    private final boolean sendHTTPClientCertificate;
    public final boolean trustHTTPServerCertificate;


    public TestCertificateBasedSSLContextProvider(TestCertificate caCertificate,
                                                  TestCertificate certificate,
                                                  boolean sendHTTPClientCertificate,
                                                  boolean trustHTTPServerCertificate) {
        this.caCertificate = caCertificate;
        this.certificate = certificate;
        this.sendHTTPClientCertificate = sendHTTPClientCertificate;
        this.trustHTTPServerCertificate = trustHTTPServerCertificate;
    }

    @Override
    public SSLContext getSslContext() throws Exception {
        SSLFactory.Builder builder = SSLFactory.builder();

        if (trustHTTPServerCertificate) {
            X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(caCertificate.getCertificateFile().toPath());
            builder.withTrustMaterial(trustManager);
        }

        if (sendHTTPClientCertificate) {
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
