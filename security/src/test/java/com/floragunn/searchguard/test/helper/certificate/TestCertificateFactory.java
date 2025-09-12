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

package com.floragunn.searchguard.test.helper.certificate;

import com.floragunn.searchguard.test.helper.certificate.asymmetricscryptography.AsymmetricCryptographyAlgorithm;
import com.floragunn.searchguard.test.helper.certificate.asymmetricscryptography.ECDSAAsymmetricCryptographyAlgorithm;
import com.floragunn.searchguard.test.helper.certificate.asymmetricscryptography.RSAAsymmetricCryptographyAlgorithm;
import com.floragunn.searchguard.test.helper.certificate.utils.CertificateSerialNumberGenerator;
import com.floragunn.searchguard.test.helper.certificate.utils.DnGenerator;
import com.floragunn.searchguard.test.helper.certificate.utils.SubjectAlternativesNameGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.CRLException;
import java.security.cert.X509CRL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class TestCertificateFactory {

    private static final Logger log = LogManager.getLogger(TestCertificateFactory.class);

    private static final Provider DEFAULT_SECURITY_PROVIDER = new BouncyCastleProvider();

    private final Provider securityProvider;
    private final AsymmetricCryptographyAlgorithm asymmetricCryptographyAlgorithm;
    private final JcaX509ExtensionUtils extUtils;

    public static TestCertificateFactory rsaBaseCertificateFactory() {
        return rsaBaseCertificateFactory(null);
    }

    public static TestCertificateFactory rsaBaseCertificateFactory(Provider securityProvider) {
        Provider provider = Optional.ofNullable(securityProvider).orElse(DEFAULT_SECURITY_PROVIDER);
        return new TestCertificateFactory(provider, new RSAAsymmetricCryptographyAlgorithm(provider, 2048));
    }

    public static TestCertificateFactory ecdsaBaseCertificatesFactory() {
        return ecdsaBaseCertificatesFactory(null);
    }

    public static TestCertificateFactory ecdsaBaseCertificatesFactory(Provider securityProvider) {
        Provider provider = Optional.ofNullable(securityProvider).orElse(DEFAULT_SECURITY_PROVIDER);
        return new TestCertificateFactory(provider, new ECDSAAsymmetricCryptographyAlgorithm(provider, "P-384"));
    }

    private TestCertificateFactory(Provider securityProvider, AsymmetricCryptographyAlgorithm asymmetricCryptographyAlgorithm) {
        this.securityProvider = securityProvider;
        this.asymmetricCryptographyAlgorithm = asymmetricCryptographyAlgorithm;
        this.extUtils = getExtUtils();
    }

    public CertificateWithKeyPair createCaCertificate(String dn, int validityDays) {
        Date validityStartDate = new Date(System.currentTimeMillis());
        Date validityEndDate = getEndDate(validityStartDate, validityDays);
        return getCertificateWithKeyPair(dn, validityStartDate, validityEndDate);
    }

    public CertificateWithKeyPair createCaCertificate(String dn, Date validityStartDate, Date validityEndDate) {
        return getCertificateWithKeyPair(dn, validityStartDate, validityEndDate);
    }

    private CertificateWithKeyPair getCertificateWithKeyPair(String dn, Date validityStartDate, Date validityEndDate) {
        try {
            KeyPair keyPair = asymmetricCryptographyAlgorithm.generateKeyPair();
            X500Name subjectName = DnGenerator.rootDn.apply(dn);

            ContentSigner contentSigner = new JcaContentSignerBuilder(asymmetricCryptographyAlgorithm.getSignatureAlgorithmName()).setProvider(
                    securityProvider).build(keyPair.getPrivate());

            X509CertificateHolder x509CertificateHolder = new X509v3CertificateBuilder(subjectName, BigInteger.valueOf(1),
                validityStartDate,
                validityEndDate, subjectName, SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded())).addExtension(
                            Extension.basicConstraints, true, new BasicConstraints(true)) // Mark this as root CA
                    .addExtension(Extension.authorityKeyIdentifier, false, extUtils.createAuthorityKeyIdentifier(keyPair.getPublic()))
                    .addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()))
                    .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyCertSign | KeyUsage.cRLSign))
                    .build(contentSigner);
            return new CertificateWithKeyPair(x509CertificateHolder, keyPair);
        } catch (OperatorCreationException | CertIOException e) {
            log.error("Error while generating CA certificate", e);
            throw new RuntimeException("Error while generating CA certificate", e);
        }
    }

    public CertificateWithKeyPair createClientCertificate(String dn, int validityDays, X509CertificateHolder signingCertificate,
                                                          PrivateKey signingPrivateKey) {
        try {
            KeyPair keyPair = asymmetricCryptographyAlgorithm.generateKeyPair();
            X500Name subjectName = DnGenerator.clientDn.apply(dn);

            ContentSigner contentSigner = new JcaContentSignerBuilder(asymmetricCryptographyAlgorithm.getSignatureAlgorithmName()).setProvider(
                    securityProvider).build(signingPrivateKey);

            Date validityStartDate = new Date(System.currentTimeMillis());
            Date validityEndDate = getEndDate(validityStartDate, validityDays);
            X509CertificateHolder x509CertificateHolder = new X509v3CertificateBuilder(signingCertificate.getSubject(),
                    CertificateSerialNumberGenerator.generateNextCertificateSerialNumber(), validityStartDate, validityEndDate, subjectName,
                    SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded())).addExtension(Extension.authorityKeyIdentifier, false,
                            extUtils.createAuthorityKeyIdentifier(signingCertificate))
                    .addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()))
                    .addExtension(Extension.basicConstraints, true, new BasicConstraints(false)).addExtension(Extension.keyUsage, true,
                            new KeyUsage(KeyUsage.digitalSignature | KeyUsage.nonRepudiation | KeyUsage.keyEncipherment)).build(contentSigner);
            return new CertificateWithKeyPair(x509CertificateHolder, keyPair);
        } catch (OperatorCreationException | CertIOException e) {
            log.error("Error while generating client certificate", e);
            throw new RuntimeException("Error while generating client certificate", e);
        }
    }

    public CertificateWithKeyPair createNodeCertificate(String dn, int validityDays, String nodeOid, List<String> dnsList, List<String> ipList,
                                                        X509CertificateHolder signingCertificate, PrivateKey signingPrivateKey) {

        try {
            KeyPair keyPair = asymmetricCryptographyAlgorithm.generateKeyPair();
            X500Name subjectName = DnGenerator.nodeDn.apply(dn);

            ContentSigner contentSigner = new JcaContentSignerBuilder(asymmetricCryptographyAlgorithm.getSignatureAlgorithmName()).setProvider(
                    securityProvider).build(signingPrivateKey);

            Date validityStartDate = new Date(System.currentTimeMillis());
            Date validityEndDate = getEndDate(validityStartDate, validityDays);
            X509CertificateHolder x509CertificateHolder = new X509v3CertificateBuilder(signingCertificate.getSubject(),
                    CertificateSerialNumberGenerator.generateNextCertificateSerialNumber(), validityStartDate, validityEndDate, subjectName,
                    SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded())).addExtension(Extension.authorityKeyIdentifier, false,
                            extUtils.createAuthorityKeyIdentifier(signingCertificate))
                    .addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()))
                    .addExtension(Extension.basicConstraints, true, new BasicConstraints(false)).addExtension(Extension.keyUsage, true,
                            new KeyUsage(KeyUsage.digitalSignature | KeyUsage.nonRepudiation | KeyUsage.keyEncipherment))
                    .addExtension(Extension.extendedKeyUsage, true,
                            new ExtendedKeyUsage(new KeyPurposeId[]{KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth}))
                    .addExtension(Extension.subjectAlternativeName, false,
                            SubjectAlternativesNameGenerator.createSubjectAlternativeNameList(nodeOid, dnsList, ipList)).build(contentSigner);
            return new CertificateWithKeyPair(x509CertificateHolder, keyPair);
        } catch (OperatorCreationException | CertIOException e) {
            log.error("Error while generating node certificate", e);
            throw new RuntimeException("Error while generating node certificate", e);
        }
    }

    public X509CRL createCertificatesRevocationList(TestCertificate caCert, List<TestCertificate> revokedCerts) {
        try {
            X500Name issuer = caCert.getCertificate().getSubject();

            Date now = Date.from(Instant.now());
            Date nextUpdate = Date.from(Instant.now().plus(30, ChronoUnit.DAYS));
            X509v2CRLBuilder crlBuilder = new X509v2CRLBuilder(issuer, now);
            crlBuilder.setNextUpdate(nextUpdate);

            revokedCerts.forEach(cert -> {
                BigInteger revokedSerial = cert.getCertificate().getSerialNumber();
                Date revocationDate = new Date();
                crlBuilder.addCRLEntry(revokedSerial, revocationDate, CRLReason.unspecified);
            });


            ContentSigner signer = new JcaContentSignerBuilder(asymmetricCryptographyAlgorithm.getSignatureAlgorithmName()).setProvider(
                    securityProvider).build(caCert.getKeyPair().getPrivate());

            JcaX509CRLConverter crlConverter = new JcaX509CRLConverter().setProvider(securityProvider);
            return crlConverter.getCRL(crlBuilder.build(signer));
        } catch (OperatorCreationException | CRLException e) {
            log.error("Error while generating certificates revocation list", e);
            throw new RuntimeException("Error while generating certificates revocation list", e);
        }
    }

    private Date getEndDate(Date startDate, int validityDays) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        calendar.add(Calendar.DATE, validityDays);
        return calendar.getTime();
    }

    private static JcaX509ExtensionUtils getExtUtils() {
        try {
            return new JcaX509ExtensionUtils();
        } catch (NoSuchAlgorithmException e) {
            log.error("Getting ext utils failed", e);
            throw new RuntimeException(e);
        }
    }
}
