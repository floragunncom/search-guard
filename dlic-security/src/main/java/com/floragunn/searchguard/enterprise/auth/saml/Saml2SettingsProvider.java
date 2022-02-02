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

package com.floragunn.searchguard.enterprise.auth.saml;

import java.net.URI;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;
import org.joda.time.DateTime;
import org.opensaml.core.criterion.EntityIdCriterion;
import org.opensaml.saml.metadata.resolver.ExtendedRefreshableMetadataResolver;
import org.opensaml.saml.metadata.resolver.MetadataResolver;
import org.opensaml.saml.metadata.resolver.RefreshableMetadataResolver;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.KeyDescriptor;
import org.opensaml.saml.saml2.metadata.SingleLogoutService;
import org.opensaml.saml.saml2.metadata.SingleSignOnService;
import org.opensaml.security.credential.UsageType;
import org.opensaml.xmlsec.signature.X509Certificate;
import org.opensaml.xmlsec.signature.X509Data;

import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.onelogin.saml2.settings.Saml2Settings;
import com.onelogin.saml2.settings.SettingsBuilder;

import net.shibboleth.utilities.java.support.resolver.CriteriaSet;
import net.shibboleth.utilities.java.support.resolver.ResolverException;

public class Saml2SettingsProvider {
    protected final static Logger log = LogManager.getLogger(Saml2SettingsProvider.class);

    private MetadataResolver metadataResolver;
    private String idpEntityId;
    private String spEntityId;
    private Settings validatorSettings;
    private Cache<URI, Entry> settingsCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterAccess(Duration.ofDays(10)).build();

    public Saml2SettingsProvider(String idpEntityId, String spEntityId, Settings validatorSettings, MetadataResolver metadataResolver) {
        this.metadataResolver = metadataResolver;
        this.idpEntityId = idpEntityId;
        this.spEntityId = spEntityId;
        this.validatorSettings = validatorSettings;
    }

    public Saml2Settings get(URI frontendBaseUrl) throws AuthenticatorUnavailableException {
        try {
            HashMap<String, Object> configProperties = new HashMap<>();
            Map<String, Object> details = new LinkedHashMap<>();
            
            if (this.metadataResolver instanceof ExtendedRefreshableMetadataResolver) {
                details.put("last_successful_refresh", ((ExtendedRefreshableMetadataResolver) this.metadataResolver).getLastSuccessfulRefresh());
            }
            
            if (this.metadataResolver instanceof ExtendedRefreshableMetadataResolver
                    && ((ExtendedRefreshableMetadataResolver) this.metadataResolver).getLastSuccessfulRefresh() == null) {
                // SAML resolver has not yet been initialized
                ResolverException lastRefreshException = null;
                
                if (this.metadataResolver instanceof SamlHTTPMetadataResolver) {
                    lastRefreshException = ((SamlHTTPMetadataResolver) this.metadataResolver).getLastRefreshException();
                }
                
                if (lastRefreshException != null) {
                   if (lastRefreshException.getCause() instanceof ResolverException) {
                       lastRefreshException = (ResolverException) lastRefreshException.getCause();
                   }
                   
                   if (lastRefreshException.getCause() != null) {
                       details.put("cause", lastRefreshException.getCause().toString());
                   }
                   
                   throw new AuthenticatorUnavailableException("Error retrieving SAML metadata", lastRefreshException.getMessage(),
                           lastRefreshException).details(details);
                } else {                
                    throw new AuthenticatorUnavailableException("SAML metadata is not yet available", "");
                }
            }

            EntityDescriptor entityDescriptor = this.metadataResolver.resolveSingle(new CriteriaSet(new EntityIdCriterion(this.idpEntityId)));

            if (entityDescriptor == null) {
                throw new AuthenticatorUnavailableException("IdP configuration error", "Could not find entity descriptor for " + this.idpEntityId).details(details);
            }
            
            details.put("role_descriptors", entityDescriptor.getRoleDescriptors().toString());

            IDPSSODescriptor idpSsoDescriptor = entityDescriptor.getIDPSSODescriptor("urn:oasis:names:tc:SAML:2.0:protocol");

            if (idpSsoDescriptor == null) {
                throw new AuthenticatorUnavailableException("IdP configuration error", "Could not find IDPSSODescriptor supporting SAML 2.0 in " + this.idpEntityId)
                        .details(details);
            }

            initIdpEndpoints(idpSsoDescriptor, configProperties);
            initIdpCerts(idpSsoDescriptor, configProperties);

            initSpEndpoints(frontendBaseUrl, configProperties);

            initMisc(configProperties);

            SettingsBuilder settingsBuilder = new SettingsBuilder();

            settingsBuilder.fromValues(configProperties);
            settingsBuilder.fromValues(new SamlSettingsMap(this.validatorSettings));

            SecurityManager sm = System.getSecurityManager();

            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }

            return AccessController.doPrivileged((PrivilegedAction<Saml2Settings>) () -> settingsBuilder.build());
        } catch (ResolverException e) {
            throw new AuthenticatorUnavailableException("Error retrieving SAML metadata", e);
        }
    }

    public Saml2Settings getCached(URI frontendBaseUrl) throws AuthenticatorUnavailableException {
        Entry entry = settingsCache.getIfPresent(frontendBaseUrl);
        DateTime tempLastUpdate = null;

        if (entry != null && isUpdateRequired(entry)) {
            entry = null;
            tempLastUpdate = ((RefreshableMetadataResolver) this.metadataResolver).getLastUpdate();
        }

        if (entry == null) {
            Saml2Settings settings = get(frontendBaseUrl);
            entry = new Entry(settings, tempLastUpdate);
            settingsCache.put(frontendBaseUrl, entry);
        }

        return entry.getSaml2Settings();
    }

    private boolean isUpdateRequired(Entry entry) {
        if (!(this.metadataResolver instanceof RefreshableMetadataResolver)) {
            return false;
        }

        RefreshableMetadataResolver refreshableMetadataResolver = (RefreshableMetadataResolver) this.metadataResolver;

        if (refreshableMetadataResolver.getLastUpdate() == null) {
            return true;
        }

        if (refreshableMetadataResolver.getLastUpdate().isAfter(entry.metadataUpdateTime)) {
            return true;
        } else {
            return false;
        }
    }

    private void initMisc(HashMap<String, Object> configProperties) {
        configProperties.put(SettingsBuilder.STRICT_PROPERTY_KEY, true);
        configProperties.put(SettingsBuilder.SECURITY_REJECT_UNSOLICITED_RESPONSES_WITH_INRESPONSETO, true);
    }

    private void initSpEndpoints(URI frontendBaseUrl, HashMap<String, Object> configProperties) {
        configProperties.put(SettingsBuilder.SP_ASSERTION_CONSUMER_SERVICE_URL_PROPERTY_KEY,
                this.buildKibanaAssertionConsumerEndpoint(frontendBaseUrl.toASCIIString()));
        configProperties.put(SettingsBuilder.SP_ASSERTION_CONSUMER_SERVICE_BINDING_PROPERTY_KEY, "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST");
        configProperties.put(SettingsBuilder.SP_ENTITYID_PROPERTY_KEY, this.spEntityId);
    }

    private void initIdpEndpoints(IDPSSODescriptor idpSsoDescriptor, HashMap<String, Object> configProperties)
            throws AuthenticatorUnavailableException {
        SingleSignOnService singleSignOnService = this.findSingleSignOnService(idpSsoDescriptor,
                "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect");

        configProperties.put(SettingsBuilder.IDP_SINGLE_SIGN_ON_SERVICE_URL_PROPERTY_KEY, singleSignOnService.getLocation());
        configProperties.put(SettingsBuilder.IDP_SINGLE_SIGN_ON_SERVICE_BINDING_PROPERTY_KEY, singleSignOnService.getBinding());
        configProperties.put(SettingsBuilder.IDP_ENTITYID_PROPERTY_KEY, this.idpEntityId);

        SingleLogoutService singleLogoutService = this.findSingleLogoutService(idpSsoDescriptor,
                "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect");

        if (singleLogoutService != null) {
            configProperties.put(SettingsBuilder.IDP_SINGLE_LOGOUT_SERVICE_URL_PROPERTY_KEY, singleLogoutService.getLocation());
            configProperties.put(SettingsBuilder.IDP_SINGLE_LOGOUT_SERVICE_BINDING_PROPERTY_KEY, singleLogoutService.getBinding());
        } else {
            log.warn(
                    "The IdP does not provide a Single Logout Service. In order to ensure that users have to re-enter their password after logging out, Search Guard will issue all SAML authentication requests with a mandatory password input (ForceAuthn=true)");
        }
    }

    private void initIdpCerts(IDPSSODescriptor idpSsoDescriptor, HashMap<String, Object> configProperties) {
        int i = 0;

        for (KeyDescriptor keyDescriptor : idpSsoDescriptor.getKeyDescriptors()) {
            if (UsageType.SIGNING.equals(keyDescriptor.getUse()) || UsageType.UNSPECIFIED.equals(keyDescriptor.getUse())) {
                for (X509Data x509data : keyDescriptor.getKeyInfo().getX509Datas()) {
                    for (X509Certificate x509Certificate : x509data.getX509Certificates()) {
                        configProperties.put(SettingsBuilder.IDP_X509CERTMULTI_PROPERTY_KEY + "." + (i++), x509Certificate.getValue());
                    }
                }
            }
        }
    }

    private SingleSignOnService findSingleSignOnService(IDPSSODescriptor idpSsoDescriptor, String binding) throws AuthenticatorUnavailableException {
        for (SingleSignOnService singleSignOnService : idpSsoDescriptor.getSingleSignOnServices()) {
            if (binding.equals(singleSignOnService.getBinding())) {
                return singleSignOnService;
            }
        }

        throw new AuthenticatorUnavailableException("IdP configuration error", "Could not find SingleSignOnService endpoint for binding " + binding
                + "; available services: " + idpSsoDescriptor.getSingleSignOnServices());
    }

    private SingleLogoutService findSingleLogoutService(IDPSSODescriptor idpSsoDescriptor, String binding) throws AuthenticatorUnavailableException {
        for (SingleLogoutService singleLogoutService : idpSsoDescriptor.getSingleLogoutServices()) {
            if (binding.equals(singleLogoutService.getBinding())) {
                return singleLogoutService;
            }
        }

        return null;
    }

    private String buildKibanaAssertionConsumerEndpoint(String kibanaRoot) {

        if (kibanaRoot.endsWith("/")) {
            return kibanaRoot + "searchguard/saml/acs";
        } else {
            return kibanaRoot + "/searchguard/saml/acs";
        }
    }

    static class SamlSettingsMap implements Map<String, Object> {

        private static final String KEY_PREFIX = "onelogin.saml2.";

        private Settings settings;

        SamlSettingsMap(Settings settings) {
            this.settings = settings;
        }

        @Override
        public int size() {
            return this.settings.size();
        }

        @Override
        public boolean isEmpty() {
            return this.settings.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return this.settings.hasValue(this.adaptKey(key));
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(Object key) {
            return this.settings.get(this.adaptKey(key));
        }

        @Override
        public Object put(String key, Object value) {
            throw new UnsupportedOperationException();

        }

        @Override
        public Object remove(Object key) {
            throw new UnsupportedOperationException();

        }

        @Override
        public void putAll(Map<? extends String, ? extends Object> m) {
            throw new UnsupportedOperationException();

        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> keySet() {
            return this.settings.keySet().stream().map((s) -> KEY_PREFIX + s).collect(Collectors.toSet());
        }

        @Override
        public Collection<Object> values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            Set<Entry<String, Object>> result = new HashSet<>();

            for (String key : this.settings.keySet()) {
                result.add(new AbstractMap.SimpleEntry<String, Object>(KEY_PREFIX + key, this.settings.get(key)));
            }

            return result;
        }

        private String adaptKey(Object keyObject) {
            if (keyObject == null) {
                return null;
            }

            String key = String.valueOf(keyObject);

            if (key.startsWith(KEY_PREFIX)) {
                return key.substring(KEY_PREFIX.length());
            } else {
                return key;
            }
        }
    }

    static class Entry {
        private final Saml2Settings saml2Settings;

        private final DateTime metadataUpdateTime;

        public Entry(Saml2Settings saml2Settings, DateTime metadataUpdateTime) {
            this.saml2Settings = saml2Settings;
            this.metadataUpdateTime = metadataUpdateTime;
        }

        public Saml2Settings getSaml2Settings() {
            return saml2Settings;
        }

        public DateTime getMetadataUpdateTime() {
            return metadataUpdateTime;
        }

    }
}
