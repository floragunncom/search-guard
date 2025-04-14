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
