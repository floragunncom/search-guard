package com.floragunn.searchguard.jwt;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.JwkUtils;

import com.floragunn.codova.documents.DocNode;
import com.nimbusds.jose.jwk.JWK;

public class NimbusUtils {
    public static String toJsonString(JWK jwk) {
        return DocNode.wrap(jwk.toJSONObject()).toJsonString();
    }

    public static JsonWebKey convertToCxf(JWK jwk) {
        return JwkUtils.readJwkKey(toJsonString(jwk));
    }
}
