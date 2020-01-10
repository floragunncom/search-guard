/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

import org.apache.cxf.rs.security.jose.jwa.SignatureAlgorithm;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jws.JwsJwtCompactConsumer;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureVerifier;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.cxf.rs.security.jose.jwt.JwtUtils;

import com.google.common.base.Strings;

public class JwtVerifier {

	private final KeyProvider keyProvider;

	public JwtVerifier(KeyProvider keyProvider) {
		this.keyProvider = keyProvider;
	}

	public JwtToken getVerifiedJwtToken(String encodedJwt) throws BadCredentialsException {
		try {
			JwsJwtCompactConsumer jwtConsumer = new JwsJwtCompactConsumer(encodedJwt);
			JwtToken jwt = jwtConsumer.getJwtToken();
			JsonWebKey key = keyProvider.getKey(jwt.getJwsHeaders().getKeyId());
			JwsSignatureVerifier signatureVerifier = getInitializedSignatureVerifier(key, jwt);

			boolean signatureValid = jwtConsumer.verifySignatureWith(signatureVerifier);

			if (!signatureValid && Strings.isNullOrEmpty(jwt.getJwsHeaders().getKeyId())) {
				key = keyProvider.getKeyAfterRefresh(null);
				signatureVerifier = getInitializedSignatureVerifier(key, jwt);
				signatureValid = jwtConsumer.verifySignatureWith(signatureVerifier);
			}

			if (!signatureValid) {
				throw new BadCredentialsException("Invalid JWT signature");
			}

			validateClaims(jwt);

			return jwt;
		} catch (JwtException e) {
			throw new BadCredentialsException(e.getMessage(), e);
		}
	}
	
	private void validateSignatureAlgorithm(JsonWebKey key, JwtToken jwt) throws BadCredentialsException {
	    if (Strings.isNullOrEmpty(key.getAlgorithm())) {
	        return;
	    }
	    
	    SignatureAlgorithm keyAlgorithm =SignatureAlgorithm.getAlgorithm(key.getAlgorithm());
	    SignatureAlgorithm tokenAlgorithm = SignatureAlgorithm.getAlgorithm(jwt.getJwsHeaders().getAlgorithm());
	    
	    if (!keyAlgorithm.equals(tokenAlgorithm)) {
	        throw new BadCredentialsException("Algorithm of JWT does not match algorithm of JWK (" + keyAlgorithm + " != " + tokenAlgorithm + ")");
	    }
	}

	private JwsSignatureVerifier getInitializedSignatureVerifier(JsonWebKey key, JwtToken jwt)
			throws BadCredentialsException, JwtException {
	    
	    validateSignatureAlgorithm(key, jwt);
	    
		JwsSignatureVerifier result = JwsUtils.getSignatureVerifier(key, jwt.getJwsHeaders().getSignatureAlgorithm());

		if (result == null) {
			throw new BadCredentialsException("Cannot verify JWT");
		} else {
			return result;
		}
	}

	private void validateClaims(JwtToken jwt) throws BadCredentialsException, JwtException {
		JwtClaims claims = jwt.getClaims();

		if (claims != null) {
			JwtUtils.validateJwtExpiry(claims, 0, false);
			JwtUtils.validateJwtNotBefore(claims, 0, false);
		}
	}
}
