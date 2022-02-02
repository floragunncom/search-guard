/*
 * Copyright 2016-2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.jwt.keybyoidc;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;

import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;


public class KeySetRetriever implements KeySetProvider {	
	private final OpenIdProviderClient openIdProviderClient;

	public KeySetRetriever(OpenIdProviderClient openIdProviderClient) {
		this.openIdProviderClient = openIdProviderClient;
	}

	public JsonWebKeys get() throws AuthenticatorUnavailableException {
		return openIdProviderClient.getJsonWebKeys();
	}
}
