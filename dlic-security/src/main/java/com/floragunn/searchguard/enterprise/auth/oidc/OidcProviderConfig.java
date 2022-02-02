/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.oidc;

import java.net.URI;
import java.util.Map;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;

public class OidcProviderConfig implements Document<OidcProviderConfig> {

    private final DocNode source;
    private final URI jwksUri;
    private final URI tokenEndpoint;
    private final URI authorizationEndpoint;
    private final URI endSessionEndpoint;
    private final URI userinfoEndpoint;

    public OidcProviderConfig(DocNode source, URI jwksUri, URI tokenEndpoint, URI authorizationEndpoint, URI endSessionEndpoint,
            URI userinfoEndpoint) {
        super();
        this.source = source;
        this.jwksUri = jwksUri;
        this.tokenEndpoint = tokenEndpoint;
        this.authorizationEndpoint = authorizationEndpoint;
        this.endSessionEndpoint = endSessionEndpoint;
        this.userinfoEndpoint = userinfoEndpoint;
    }

    public URI getJwksUri() {
        return jwksUri;
    }

    public URI getTokenEndpoint() {
        return tokenEndpoint;
    }

    public URI getAuthorizationEndpoint() {
        return authorizationEndpoint;
    }

    public URI getEndSessionEndpoint() {
        return endSessionEndpoint;
    }

    public URI getUserinfoEndpoint() {
        return userinfoEndpoint;
    }

    public static ValidationResult<OidcProviderConfig> parse(DocNode docNode) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        URI jwksUri = vNode.get("jwks_uri").asAbsoluteURI();
        URI tokenEndpoint = vNode.get("token_endpoint").asAbsoluteURI();
        URI authorizationEndpoint = vNode.get("authorization_endpoint").asAbsoluteURI();
        URI endSessionEndpoint = vNode.get("end_session_endpoint").asAbsoluteURI();
        URI userinfoEndpoint = vNode.get("userinfo_endpoint").asAbsoluteURI();

        return new ValidationResult<OidcProviderConfig>(
                new OidcProviderConfig(docNode, jwksUri, tokenEndpoint, authorizationEndpoint, endSessionEndpoint, userinfoEndpoint),
                validationErrors);
    }

    @Override
    public Map<String, Object> toBasicObject() {
        return source;
    }

}