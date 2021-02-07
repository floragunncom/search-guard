/*
 * Copyright 2016-2021 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.dlic.auth.http.jwt.oidc.json;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

public class OidcProviderConfig {

    private final Map<String, Object> parsedJson;

    private final String jwksUri;
    private final String tokenEndpoint;

    public OidcProviderConfig(Map<String, Object> map) {
        this.parsedJson = Collections.unmodifiableMap(map);
        this.jwksUri = map.containsKey("jwks_uri") ? String.valueOf(map.get("jwks_uri")) : null;
        this.tokenEndpoint = map.containsKey("token_endpoint") ? String.valueOf(map.get("token_endpoint")) : null;
    }

    public String getJwksUri() {
        return jwksUri;
    }

    public String getTokenEndpoint() {
        return tokenEndpoint;
    }
    
    public URI getAuthorizationEndpoint() {
        Object rawUri = parsedJson.get("authorization_endpoint");
        
        if (rawUri == null) {
            return null;
        }
        
        return URI.create(String.valueOf(rawUri));
    }
    
    public URI getEndSessionEndpoint() {
        Object rawUri = parsedJson.get("end_session_endpoint");
        
        if (rawUri == null) {
            return null;
        }
        
        return URI.create(String.valueOf(rawUri));        
    }

    public Map<String, Object> getParsedJson() {
        return parsedJson;
    }

}