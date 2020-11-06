package com.floragunn.searchguard.ssl.util.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.NoSuchPaddingException;

import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCSException;

import com.floragunn.searchguard.support.PemKeyReader;

public class ClientAuthCredentials {
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
        private X509Certificate[] authenticationCertificate;
        private PrivateKey authenticationKey;
        private KeyStore keyStore;
        private String keyAlias;
        private String keyPassword;

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
            authenticationCertificate = PemKeyReader.loadCertificatesFromStream(inputStream);
            return this;
        }

        public Builder certKeyPem(File file, String password) throws GenericSSLConfigException, OperatorCreationException, PKCSException {
            try (FileInputStream in = new FileInputStream(file)) {
                return certKeyPem(in, password);
            } catch (FileNotFoundException e) {
                throw new GenericSSLConfigException("Could not find certificate key file " + file, e);
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeySpecException | InvalidAlgorithmParameterException | KeyException
                    | IOException e) {
                throw new GenericSSLConfigException("Error while reading certificate key file " + file, e);
            }
        }

        public Builder certKeyPem(Path path, String password) throws GenericSSLConfigException, OperatorCreationException, PKCSException {
            return certKeyPem(path.toFile(), password);
        }
        
        public Builder certKeyPem(InputStream inputStream, String password) throws NoSuchAlgorithmException, NoSuchPaddingException,
                InvalidKeySpecException, InvalidAlgorithmParameterException, KeyException, IOException, OperatorCreationException, PKCSException {
            authenticationKey = PemKeyReader.toPrivateKey(inputStream, password);

            return this;
        }

        public Builder jks(File file, String alias, String password) throws GenericSSLConfigException {
            return keyStore(file, alias, password, "JKS");
        }

        public Builder pkcs12(File file, String alias, String password) throws GenericSSLConfigException {
            return keyStore(file, alias, password, "PKCS12");
        }

        public Builder keyStore(File file, String alias, String password) throws GenericSSLConfigException {
            return keyStore(file, alias, password, null);
        }

        public Builder keyStore(File file, String alias, String password, String type) throws GenericSSLConfigException {

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
                keyAlias = alias;
                keyPassword = password;

                return this;

            } catch (Exception e) {
                throw new GenericSSLConfigException("Error loading client auth key store from " + file, e);
            }

        }

        public ClientAuthCredentials build() throws GenericSSLConfigException {

            try {
                ClientAuthCredentials result = new ClientAuthCredentials();

                if (keyStore != null) {
                    result.keyStore = keyStore;
                    result.keyAlias = keyAlias;
                    result.keyPassword = keyPassword != null ? keyPassword.toCharArray() : null;
                } else if (authenticationCertificate != null && authenticationKey != null) {
                    result.keyPassword = PemKeyReader.randomChars(12);
                    result.keyAlias = "al";
                    result.keyStore = PemKeyReader.toKeystore(result.keyAlias, result.keyPassword,
                            authenticationCertificate, authenticationKey);
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
