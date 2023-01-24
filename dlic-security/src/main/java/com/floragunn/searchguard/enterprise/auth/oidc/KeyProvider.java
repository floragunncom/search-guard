/*
  * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import java.util.ArrayList;
import java.util.List;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;

public interface KeyProvider {
    public JsonWebKey getKey(String kid) throws AuthenticatorUnavailableException, BadCredentialsException;

    public JsonWebKey getKeyAfterRefresh(String kid) throws AuthenticatorUnavailableException, BadCredentialsException;

    public static KeyProvider combined(KeyProvider... providers) {
        List<KeyProvider> providersList = new ArrayList<>();

        for (KeyProvider keyProvider : providers) {
            if (keyProvider != null) {
                providersList.add(keyProvider);
            }
        }

        return new KeyProvider() {

            @Override
            public JsonWebKey getKeyAfterRefresh(String kid) throws AuthenticatorUnavailableException, BadCredentialsException {
                for (KeyProvider keyProvider : providersList) {
                    JsonWebKey result = keyProvider.getKeyAfterRefresh(kid);

                    if (result != null) {
                        return result;
                    }
                }

                return null;
            }

            @Override
            public JsonWebKey getKey(String kid) throws AuthenticatorUnavailableException, BadCredentialsException {
                for (KeyProvider keyProvider : providersList) {
                    JsonWebKey result = keyProvider.getKey(kid);

                    if (result != null) {
                        return result;
                    }
                }

                return null;
            }
        };
    }

}
