/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.ssl.util;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CRL;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

public class SSLCertificateHelper {

    private static final Logger log = LogManager.getLogger(SSLCertificateHelper.class);
    private static boolean stripRootFromChain = true; //TODO check
    
    public static X509Certificate[] exportRootCertificates(final KeyStore ks, final String alias) throws KeyStoreException {
        logKeyStore(ks);
        
        final List<X509Certificate> trustedCerts = new ArrayList<X509Certificate>();
        
        if (Strings.isNullOrEmpty(alias)) {
            
            if(log.isDebugEnabled()) {
                log.debug("No alias given, will trust all of the certificates in the store");
            }
            
            final List<String> aliases = toList(ks.aliases());
            
            for (final String _alias : aliases) {

                if (ks.isCertificateEntry(_alias)) {
                    final X509Certificate cert = (X509Certificate) ks.getCertificate(_alias);
                    if (cert != null) {
                        trustedCerts.add(cert);
                    } else {
                        log.error("Alias {} does not exist", _alias);
                    }
                }
            }
        } else {
            if (ks.isCertificateEntry(alias)) {
                final X509Certificate cert = (X509Certificate) ks.getCertificate(alias);
                if (cert != null) {
                    trustedCerts.add(cert);
                } else {
                    log.error("Alias {} does not exist", alias);
                }
            } else {
                log.error("Alias {} does not contain a certificate entry", alias);
            }
        }

        return trustedCerts.toArray(new X509Certificate[0]);
    }   
    
    public static X509Certificate[] exportServerCertChain(final KeyStore ks, String alias) throws KeyStoreException {
        logKeyStore(ks);
        final List<String> aliases = toList(ks.aliases());
        
        if (Strings.isNullOrEmpty(alias)) {
            if(aliases.isEmpty()) {
                log.error("Keystore does not contain any aliases");
            } else {
                alias = aliases.get(0);
                log.info("No alias given, use the first one: {}", alias);
            }
        } 

        final Certificate[] certs = ks.getCertificateChain(alias);
        if (certs != null && certs.length > 0) {
            X509Certificate[] x509Certs = Arrays.copyOf(certs, certs.length, X509Certificate[].class);

            final X509Certificate lastCertificate = x509Certs[x509Certs.length - 1];

            if (lastCertificate.getBasicConstraints() > -1
                    && lastCertificate.getSubjectX500Principal().equals(lastCertificate.getIssuerX500Principal())) {
                log.warn("Certificate chain for alias {} contains a root certificate", alias);
                
                if(stripRootFromChain ) {
                    x509Certs = Arrays.copyOf(certs, certs.length-1, X509Certificate[].class);
                }
            }

            return x509Certs;
        } else {
            log.error("Alias {} does not exist or contain a certificate chain", alias);
        }

        return new X509Certificate[0];
    }

    public static PrivateKey exportDecryptedKey(final KeyStore ks, final String alias, final char[] keyPassword) throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
        logKeyStore(ks);
        final List<String> aliases = toList(ks.aliases());

        String evaluatedAlias = alias;

        if (alias == null && aliases.size() > 0) {
            evaluatedAlias = aliases.get(0);
        }

        if (evaluatedAlias == null) {
            throw new KeyStoreException("null alias, current aliases: " + aliases);
        }

        final Key key = ks.getKey(evaluatedAlias, (keyPassword == null || keyPassword.length == 0) ? null:keyPassword);

        if (key == null) {
            throw new KeyStoreException("no key alias named " + evaluatedAlias);
        }

        if (key instanceof PrivateKey) {
            return (PrivateKey) key;
        }

        return null;
    }
    
    private static void logKeyStore(final KeyStore ks) {
        try {
            final List<String> aliases = toList(ks.aliases());
            if (log.isDebugEnabled()) {
                log.debug("Keystore has {} entries/aliases", ks.size());
                for (String _alias : aliases) {
                    log.debug("Alias {}: is a certificate entry?{}/is a key entry?{}", _alias, ks.isCertificateEntry(_alias),
                            ks.isKeyEntry(_alias));
                    Certificate[] certs = ks.getCertificateChain(_alias);

                    if (certs != null) {
                        log.debug("Alias {}: chain len {}", _alias, certs.length);
                        for (int i = 0; i < certs.length; i++) {
                            X509Certificate certificate = (X509Certificate) certs[i];
                            log.debug("cert {} of type {} -> {}", certificate.getSubjectX500Principal(), certificate.getBasicConstraints(),
                                    certificate.getSubjectX500Principal().equals(certificate.getIssuerX500Principal()));
                        }
                    }

                    X509Certificate cert = (X509Certificate) ks.getCertificate(_alias);

                    if (cert != null) {
                        log.debug("Alias {}: single cert {} of type {} -> {}", _alias, cert.getSubjectX500Principal(),
                                cert.getBasicConstraints(), cert.getSubjectX500Principal().equals(cert.getIssuerX500Principal()));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error logging keystore due to "+e, e);
        }
    }
    
    private static List<String> toList(final Enumeration<String> enumeration) {
        final List<String> aliases = new ArrayList<>();

        while (enumeration.hasMoreElements()) {
            aliases.add(enumeration.nextElement());
        }
        
        return Collections.unmodifiableList(aliases);
    }

    public static boolean validate(X509Certificate[] x509Certs, final Settings settings, final Path configPath) {

        final boolean validateCrl = settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_VALIDATE, false);

        if(log.isTraceEnabled()) {
            log.trace("validateCrl: "+validateCrl);
        }

        if(!validateCrl) {
            return true;
        }

        final Environment env = new Environment(settings, configPath);

        try {

            Collection<? extends CRL> crls = null;
            final String crlFile = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_FILE);

            if(crlFile != null) {
                final File crl = env.configDir().resolve(crlFile).toAbsolutePath().toFile();
                try(FileInputStream crlin = new FileInputStream(crl)) {
                    crls = CertificateFactory.getInstance("X.509").generateCRLs(crlin);
                }

                if(log.isTraceEnabled()) {
                    log.trace("crls from file: "+crls.size());
                }
            } else {
                if(log.isTraceEnabled()) {
                    log.trace("no crl file configured");
                }
            }

            final String truststore = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_FILEPATH);
            CertificateValidator validator = null;

            if(truststore != null) {
                final String truststoreType = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_TYPE, "JKS");
                final String truststorePassword = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_PASSWORD, "changeit");
                //final String truststoreAlias = settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_TRUSTSTORE_ALIAS, null);

                final KeyStore ts = KeyStore.getInstance(truststoreType);
                try(FileInputStream fin = new FileInputStream(new File(env.configDir().resolve(truststore).toAbsolutePath().toString()))) {
                    ts.load(fin, (truststorePassword == null || truststorePassword.length() == 0) ?null:truststorePassword.toCharArray());
                }
                validator = new CertificateValidator(ts, crls);
            } else {
                final File trustedCas = env.configDir().resolve(settings.get(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH, "")).toAbsolutePath().toFile();
                try(FileInputStream trin = new FileInputStream(trustedCas)) {
                    Collection<? extends Certificate> cert =  (Collection<? extends Certificate>) CertificateFactory.getInstance("X.509").generateCertificates(trin);
                    validator = new CertificateValidator(cert.toArray(new X509Certificate[0]), crls);
                }
            }

            validator.setEnableCRLDP(!settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_DISABLE_CRLDP, false));
            validator.setCheckOnlyEndEntities(settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_CHECK_ONLY_END_ENTITIES, true));
            validator.setPreferCrl(settings.getAsBoolean(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_PREFER_CRLFILE_OVER_OCSP, false));
            Long dateTimestamp = settings.getAsLong(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_VALIDATION_DATE, null);
            if(dateTimestamp != null && dateTimestamp.longValue() < 0) {
                dateTimestamp = null;
            }
            validator.setDate(dateTimestamp==null?null:new Date(dateTimestamp.longValue()));
            validator.validate(x509Certs);

            return true;

        } catch (Exception e) {
            if(log.isDebugEnabled()) {
                log.debug("Unable to validate CRL: "+ ExceptionsHelper.stackTrace(e));
            }
            log.warn("Unable to validate CRL: "+ExceptionUtils.getRootCause(e));
        }

        return false;
    }
}
