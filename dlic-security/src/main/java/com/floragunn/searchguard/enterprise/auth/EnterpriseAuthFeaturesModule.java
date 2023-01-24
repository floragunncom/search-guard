/*
  * Copyright 2022 by floragunn GmbH - All rights reserved
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

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.BaseDependencies;
import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.enterprise.auth.jwt.JwtAuthenticator;
import com.floragunn.searchguard.enterprise.auth.kerberos.KerberosAuthenticationFrontend;
import com.floragunn.searchguard.enterprise.auth.ldap.LDAPAuthenticationBackend;
import com.floragunn.searchguard.enterprise.auth.oidc.OidcAuthenticator;
import com.floragunn.searchguard.enterprise.auth.saml.SamlAuthenticator;
import com.floragunn.searchguard.enterprise.auth.session.ExternalSearchGuardSessionAuthenticationBackend;
import com.floragunn.searchguard.support.ConfigConstants;
import java.util.Collection;
import java.util.List;

public class EnterpriseAuthFeaturesModule implements SearchGuardModule {

    private boolean enterpriseModulesEnabled;

    @Override
    public Collection<Object> createComponents(BaseDependencies baseDependencies) {
        enterpriseModulesEnabled = baseDependencies.getSettings().getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true);

        return ImmutableList.empty();
    }

    @Override
    public List<TypedComponent.Info<?>> getTypedComponents() {
        if (enterpriseModulesEnabled) {
            return ImmutableList.<TypedComponent.Info<?>>of(JwtAuthenticator.INFO, KerberosAuthenticationFrontend.INFO, OidcAuthenticator.INFO,
                    SamlAuthenticator.INFO, ExternalSearchGuardSessionAuthenticationBackend.INFO).with(LDAPAuthenticationBackend.INFOS);
        } else {
            return ImmutableList.empty();
        }
    }
}
