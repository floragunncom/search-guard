/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchguard.jwt;

import static com.floragunn.searchguard.jwt.NimbusUtils.convertToCxf;

import java.text.ParseException;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.cxf.rs.security.jose.jwa.ContentAlgorithm;
import org.apache.cxf.rs.security.jose.jwe.JweDecryptionOutput;
import org.apache.cxf.rs.security.jose.jwe.JweDecryptionProvider;
import org.apache.cxf.rs.security.jose.jwe.JweUtils;
import org.apache.cxf.rs.security.jose.jws.JwsHeaders;
import org.apache.cxf.rs.security.jose.jws.JwsJwtCompactConsumer;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureVerifier;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;

import com.floragunn.searchsupport.PrivilegedCode;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.factories.DefaultJWEDecrypterFactory;
import com.nimbusds.jose.crypto.factories.DefaultJWSVerifierFactory;
import com.nimbusds.jose.jwk.AsymmetricJWK;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.SecretJWK;
import com.nimbusds.jose.proc.JWEDecrypterFactory;
import com.nimbusds.jose.proc.JWSVerifierFactory;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.BadJWTException;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.JWTClaimsSetVerifier;

/**
 * A class that can use both com.nimbusds and org.apache.cxf for validating JWTs. 
 * JWTs which have the claim "sg_p" = "n" will be validated by NimbusDS. That should be all newly produced JWTs.
 * Other JWTs will be validated by CXF.
 */
public class JwtVerifier {

    public static final String PRODUCER_CLAIM = "sg_p";
    public static final String PRODUCER_CLAIM_NIMBUS = "n";

    private static final JWEDecrypterFactory JWE_DECRYPTER_FACTORY = new DefaultJWEDecrypterFactory();
    private static final JWSVerifierFactory JWS_VERIFIER_FACTORY = new DefaultJWSVerifierFactory();

    private final JWK signingKey;
    private final JWK encryptionKey;
    private final JWTClaimsSetVerifier<SecurityContext> jwtClaimsVerifier;
    private final CxfBased cxfBased;

    public JwtVerifier(JWK signingKey, JWK encryptionKey, String requiredAudience) {
        this.signingKey = signingKey;
        this.encryptionKey = encryptionKey;
        this.jwtClaimsVerifier = new DefaultJWTClaimsVerifier<>(requiredAudience, null, null);
        this.cxfBased = new CxfBased(signingKey, encryptionKey, requiredAudience);
    }

    public JWT getVerfiedJwt(String encodedJwt) throws ParseException, JOSEException, BadJWTException {
        return getVerfiedJwt(encodedJwt, this.jwtClaimsVerifier, this.cxfBased);
    }

    public JWT getVerfiedJwt(String encodedJwt, String requiredAudience) throws ParseException, JOSEException, BadJWTException {
        return getVerfiedJwt(encodedJwt, new DefaultJWTClaimsVerifier<>(requiredAudience, null, null), cxfBased.requiredAudience(requiredAudience));
    }

    private JWT getVerfiedJwt(String encodedJwt, JWTClaimsSetVerifier<SecurityContext> jwtClaimsVerifier, CxfBased cxfBased)
            throws ParseException, JOSEException, BadJWTException {
        if (this.encryptionKey != null) {
            JWEObject jweObject = JWEObject.parse(encodedJwt);

            if (jweObject.getHeader().getCustomParam(PRODUCER_CLAIM) == null) {
                // This JWT was produced by CXF
                return cxfBased.getVerifiedJwt(encodedJwt);
            }
            jweObject.decrypt(decrypter(jweObject));
            encodedJwt = jweObject.getPayload().toSignedJWT().serialize();
        }

        SignedJWT signedJwt = SignedJWT.parse(encodedJwt);

        boolean signatureValid = signedJwt.verify(verifier(signedJwt));

        if (!signatureValid) {
            throw new JOSEException("Invalid JWT signature");
        }

        jwtClaimsVerifier.verify(signedJwt.getJWTClaimsSet(), null);

        return signedJwt;
    }

    JWEDecrypter decrypter(JWEObject jweObject) throws JOSEException {
        if (encryptionKey == null) {
            return null;
        }
        if (encryptionKey instanceof AsymmetricJWK) {
            return JWE_DECRYPTER_FACTORY.createJWEDecrypter(jweObject.getHeader(), ((AsymmetricJWK) encryptionKey).toPrivateKey());
        } else if (encryptionKey instanceof SecretJWK) {
            return JWE_DECRYPTER_FACTORY.createJWEDecrypter(jweObject.getHeader(), ((SecretJWK) encryptionKey).toSecretKey());
        } else {
            throw new RuntimeException("Unknown key type: " + encryptionKey);
        }
    }

    JWSVerifier verifier(SignedJWT signedJWT) throws JOSEException {
        if (signingKey instanceof AsymmetricJWK) {
            return JWS_VERIFIER_FACTORY.createJWSVerifier(signedJWT.getHeader(), ((AsymmetricJWK) signingKey).toPublicKey());
        } else if (signingKey instanceof SecretJWK) {
            return JWS_VERIFIER_FACTORY.createJWSVerifier(signedJWT.getHeader(), ((SecretJWK) signingKey).toSecretKey());
        } else {
            throw new RuntimeException("Unknown key type: " + signingKey);
        }
    }

    static class CxfBased {
        private final JwsSignatureVerifier jwsSignatureVerifier;
        private final JweDecryptionProvider jweDecryptionProvider;
        private final String requiredJwtAudience;

        CxfBased(JWK signingKey, JWK encryptionKey, String requiredJwtAudience) {
            this.requiredJwtAudience = requiredJwtAudience;
            this.jwsSignatureVerifier = JwsUtils.getSignatureVerifier(convertToCxf(signingKey));
            this.jweDecryptionProvider = encryptionKey != null
                    ? JweUtils.createJweDecryptionProvider(convertToCxf(encryptionKey), ContentAlgorithm.A256CBC_HS512)
                    : null;
        }

        private CxfBased(JwsSignatureVerifier jwsSignatureVerifier, JweDecryptionProvider jweDecryptionProvider, String requiredJwtAudience) {
            this.jwsSignatureVerifier = jwsSignatureVerifier;
            this.jweDecryptionProvider = jweDecryptionProvider;
            this.requiredJwtAudience = requiredJwtAudience;
        }

        CxfBased requiredAudience(String requiredAudience) {
            return new CxfBased(jwsSignatureVerifier, jweDecryptionProvider, requiredAudience);
        }

        JWT getVerifiedJwt(String encodedJwt) {
            if (this.jweDecryptionProvider != null) {
                JweDecryptionOutput decOutput = this.jweDecryptionProvider.decrypt(encodedJwt);
                encodedJwt = decOutput.getContentText();
            }

            JwsJwtCompactConsumer jwtConsumer = new JwsJwtCompactConsumer(encodedJwt);
            JwtToken jwt = jwtConsumer.getJwtToken();

            if (!validateAudience(jwt.getClaims())) {
                return null;
            }

            if (this.jwsSignatureVerifier != null) {
                boolean signatureValid = jwtConsumer.verifySignatureWith(jwsSignatureVerifier);

                if (!signatureValid) {
                    throw new JwtException("Invalid JWT signature for token " + jwt.getClaims().asMap());
                }
            }

            validateClaims(jwt);

            return PrivilegedCode
                    .execute(() -> new SignedJWT(cxfHeaderToNimbusHeader(jwt.getJwsHeaders()), cxfClaimsToNimbusClaims(jwt.getClaims())));
        }

        static JWSHeader cxfHeaderToNimbusHeader(JwsHeaders cxfHeaders) {
            JWSHeader.Builder nimbusBuilder = new JWSHeader.Builder(JWSAlgorithm.parse(cxfHeaders.getAlgorithm()));
            if (cxfHeaders.getType() != null) {
                nimbusBuilder.type(new JOSEObjectType(cxfHeaders.getType().toString()));
            }
            if (cxfHeaders.getContentType() != null) {
                nimbusBuilder.contentType(cxfHeaders.getContentType());
            }
            if (cxfHeaders.getCritical() != null) {
                nimbusBuilder.criticalParams(new HashSet<>(cxfHeaders.getCritical()));
            }
            if (cxfHeaders.getKeyId() != null) {
                nimbusBuilder.keyID(cxfHeaders.getKeyId());
            }
            for (Map.Entry<String, Object> entry : cxfHeaders.asMap().entrySet()) {
                if (!JWSHeader.getRegisteredParameterNames().contains(entry.getKey())) {
                    nimbusBuilder.customParam(entry.getKey(), entry.getValue());
                }
            }
            return nimbusBuilder.build();
        }

        static JWTClaimsSet cxfClaimsToNimbusClaims(JwtClaims cxfClaimsSet) {
            JWTClaimsSet.Builder nimbusBuilder = new JWTClaimsSet.Builder();

            if (cxfClaimsSet.getIssuer() != null) {
                nimbusBuilder.issuer(cxfClaimsSet.getIssuer());
            }
            if (cxfClaimsSet.getSubject() != null) {
                nimbusBuilder.subject(cxfClaimsSet.getSubject());
            }
            if (cxfClaimsSet.getAudience() != null) {
                nimbusBuilder.audience(cxfClaimsSet.getAudience());
            }
            if (cxfClaimsSet.getExpiryTime() != null) {
                nimbusBuilder.expirationTime(Date.from(Instant.ofEpochMilli(cxfClaimsSet.getExpiryTime() * 1000L)));
            }
            if (cxfClaimsSet.getNotBefore() != null) {
                nimbusBuilder.notBeforeTime(Date.from(Instant.ofEpochMilli(cxfClaimsSet.getNotBefore() * 1000L)));
            }
            if (cxfClaimsSet.getIssuedAt() != null) {
                nimbusBuilder.issueTime(Date.from(Instant.ofEpochMilli(cxfClaimsSet.getIssuedAt() * 1000L)));
            }
            if (cxfClaimsSet.getTokenId() != null) {
                nimbusBuilder.jwtID(cxfClaimsSet.getTokenId());
            }

            Map<String, Object> customClaims = cxfClaimsSet.asMap();
            for (Map.Entry<String, Object> entry : customClaims.entrySet()) {
                if (!JWTClaimsSet.getRegisteredNames().contains(entry.getKey())) {
                    nimbusBuilder.claim(entry.getKey(), entry.getValue());
                }
            }

            return nimbusBuilder.build();
        }

        private void validateClaims(JwtToken jwt) throws JwtException {
            JwtClaims claims = jwt.getClaims();

            if (claims == null) {
                throw new JwtException("The JWT does not have any claims");
            }

            org.apache.cxf.rs.security.jose.jwt.JwtUtils.validateJwtExpiry(claims, 0, false);
            org.apache.cxf.rs.security.jose.jwt.JwtUtils.validateJwtNotBefore(claims, 0, false);

        }

        private boolean validateAudience(JwtClaims claims) throws JwtException {
            if (requiredJwtAudience != null) {
                for (String audience : claims.getAudiences()) {
                    if (requiredJwtAudience.equals(audience)) {
                        return true;
                    }
                }
            }
            return false;
        }

    }

}
