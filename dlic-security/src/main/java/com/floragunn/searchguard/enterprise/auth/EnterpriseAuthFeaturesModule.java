/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.enterprise.auth.jwt.JwtAuthenticator;
import com.floragunn.searchguard.enterprise.auth.kerberos.HTTPSpnegoAuthenticator;
import com.floragunn.searchguard.enterprise.auth.ldap.LDAPAuthenticationBackend;
import com.floragunn.searchguard.enterprise.auth.oidc.OidcAuthenticator;
import com.floragunn.searchguard.enterprise.auth.saml.SamlAuthenticator;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchsupport.util.ImmutableList;

public class EnterpriseAuthFeaturesModule implements SearchGuardModule {

    private boolean enterpriseModulesEnabled;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        enterpriseModulesEnabled = baseDependencies.getSettings().getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true);

        return Collections.emptyList();
    }

    @Override
    public List<TypedComponent.Info<?>> getTypedComponents() {
        if (enterpriseModulesEnabled) {
            return ImmutableList.<TypedComponent.Info<?>>of(JwtAuthenticator.INFO, HTTPSpnegoAuthenticator.INFO, OidcAuthenticator.INFO,
                    SamlAuthenticator.INFO).with(LDAPAuthenticationBackend.INFOS);
        } else {
            return Collections.emptyList();
        }
    }

}
