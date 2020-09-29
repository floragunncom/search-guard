package com.floragunn.searchguard.ssl.util.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import com.floragunn.searchguard.support.PemKeyReader;

public class TrustStore {
    public static Builder from() {
        return new Builder();
    }

    private KeyStore keyStore;
    private char[] keyPassword;
    private String keyAlias;

    public KeyStore getKeyStore() {
        return keyStore;
    }

    public char[] getKeyPassword() {
        return keyPassword;
    }

    public String getKeyAlias() {
        return keyAlias;
    }

    public static class Builder {
        private X509Certificate[] certificates;
        private KeyStore keyStore;

        public Builder certPem(File file) throws GenericSSLConfigException {
            try (FileInputStream in = new FileInputStream(file)) {
                return certPem(in);
            } catch (FileNotFoundException e) {
                throw new GenericSSLConfigException("Could not find certificate file " + file, e);
            } catch (IOException | CertificateException e) {
                throw new GenericSSLConfigException("Error while reading certificate file " + file, e);
            }
        }
        
        public Builder certPem(Path path) throws GenericSSLConfigException {
            return certPem(path.toFile());
        }

        public Builder certPem(InputStream inputStream) throws CertificateException {
            certificates = PemKeyReader.loadCertificatesFromStream(inputStream);
            return this;
        }

        public Builder jks(File file, String password) throws GenericSSLConfigException {
            return keyStore(file, password, "JKS");
        }

        public Builder pkcs12(File file, String password) throws GenericSSLConfigException {
            return keyStore(file, password, "PKCS12");
        }

        public Builder keyStore(File file, String password) throws GenericSSLConfigException {
            return keyStore(file, password, null);
        }

        public Builder keyStore(File file, String password, String type) throws GenericSSLConfigException {

            try {
                if (type == null) {
                    String fileName = file.getName();

                    if (fileName.endsWith(".jks")) {
                        type = "JKS";
                    } else if (fileName.endsWith(".pfx") || fileName.endsWith(".p12")) {
                        type = "PKCS12";
                    } else {
                        throw new IllegalArgumentException("Unknwon file type: " + fileName);
                    }
                }

                keyStore = KeyStore.getInstance(type.toUpperCase());
                keyStore.load(new FileInputStream(file), password == null ? null : password.toCharArray());

                return this;

            } catch (Exception e) {
                throw new GenericSSLConfigException("Error loading client auth key store from " + file, e);
            }

        }

        public TrustStore build() throws GenericSSLConfigException {

            try {
                TrustStore result = new TrustStore();

                if (keyStore != null) {
                    result.keyStore = keyStore;
                } else if (certificates != null) {
                    result.keyStore = PemKeyReader.toTruststore("al", certificates);
                } else {
                    throw new IllegalStateException("Builder not completely initialized: " + this);
                }

                return result;
            } catch (Exception e) {
                throw new GenericSSLConfigException("Error initializing client auth credentials", e);
            }
        }

    }
}
