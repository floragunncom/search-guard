package com.floragunn.searchguard.test.helper.rest;

import com.floragunn.searchguard.test.helper.file.FileHelper;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.security.KeyStore;

public class StaticCertificatesBasedSSLContextProvider implements SSLContextProvider {

    private final String prefix;
    private final String keystore;
    private final String truststore;
    private final boolean sendHTTPClientCertificate;
    private final boolean trustHTTPServerCertificate;

    public StaticCertificatesBasedSSLContextProvider(String prefix, String keyStore, String truststore, boolean sendHTTPClientCertificate, boolean trustHTTPServerCertificate) {
        this.prefix = prefix;
        this.keystore = keyStore;
        this.truststore = truststore;
        this.sendHTTPClientCertificate = sendHTTPClientCertificate;
        this.trustHTTPServerCertificate = trustHTTPServerCertificate;
    }

    @Override
    public SSLContext getSslContext() {
        try {
            final SSLContextBuilder sslContextbBuilder = SSLContexts.custom();

            if (trustHTTPServerCertificate) {
                final KeyStore myTrustStore = KeyStore.getInstance("JKS");
                myTrustStore.load(new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath(prefix, truststore).toFile()),
                        "changeit".toCharArray());
                sslContextbBuilder.loadTrustMaterial(myTrustStore, null);
            }

            if (sendHTTPClientCertificate) {
                final KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath(prefix, keystore).toFile()), "changeit".toCharArray());
                sslContextbBuilder.loadKeyMaterial(keyStore, "changeit".toCharArray());
            }

            return sslContextbBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Getting SSL context failed", e);
        }

    }

}
