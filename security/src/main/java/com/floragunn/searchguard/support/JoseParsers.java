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

package com.floragunn.searchguard.support;

import org.apache.cxf.rs.security.jose.common.JoseUtils;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

public class JoseParsers {
    public static JsonWebKey parseJwkSigningKey(String jwkJsonString) throws ConfigValidationException {
        JsonWebKey result = JwkUtils.readJwkKey(jwkJsonString);

        PublicKeyUse publicKeyUse = result.getPublicKeyUse();

        if (publicKeyUse != null && publicKeyUse != PublicKeyUse.SIGN) {
            throw new ConfigValidationException(new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for signing"));
        }

        return result;
    }

    public static JsonWebKey parseJwkHs512SigningKey(String value) throws ConfigValidationException {
        byte[] key;

        try {
            key = JoseUtils.decode(value);
        } catch (Exception e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(),
                    "A Base64URL encoded HMAC512 key with at least 512 bit (64 bytes, 86 Base64 encoded characters)").cause(e));
        }

        if (key.length < 64) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 512 bit",
                    "A Base64URL encoded HMAC512 key with at least 512 bit (64 bytes, 86 Base64 encoded characters)"));
        }

        JsonWebKey jwk = new JsonWebKey();

        jwk.setKeyType(KeyType.OCTET);
        jwk.setAlgorithm("HS512");
        jwk.setPublicKeyUse(PublicKeyUse.SIGN);
        jwk.setProperty("k", value);

        return jwk;
    }

    public static JsonWebKey parseJwkEcryptionKey(String jwkJsonString) throws ConfigValidationException {
        JsonWebKey result = JwkUtils.readJwkKey(jwkJsonString);

        PublicKeyUse publicKeyUse = result.getPublicKeyUse();

        if (publicKeyUse != null && publicKeyUse != PublicKeyUse.ENCRYPT) {
            throw new ConfigValidationException(
                    new InvalidAttributeValue("use", publicKeyUse, "The use claim must designate the JWK for encryption"));
        }

        return result;
    }

    public static JsonWebKey parseJwkA256kwEncryptionKey(String value) throws ConfigValidationException {
        byte[] key;

        try {
            key = JoseUtils.decode(value);
        } catch (Exception e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, e.getMessage(),
                    "A Base64URL encoded A256KW key with at least 256 bit (32 bytes, 43 Base64 encoded characters)").cause(e));
        }

        if (key.length < 32) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, "The key contains less than 256 bit",
                    "A Base64URL encoded A256KW key with at least 256 bit (32 bytes, 43 Base64 encoded characters)"));
        }

        JsonWebKey jwk = new JsonWebKey();

        jwk.setKeyType(KeyType.OCTET);
        jwk.setAlgorithm("A256KW");
        jwk.setPublicKeyUse(PublicKeyUse.ENCRYPT);
        jwk.setProperty("k", value);

        return jwk;
    }
}
