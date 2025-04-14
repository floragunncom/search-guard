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

package com.floragunn.searchguard.enterprise.auth.saml;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivateKey;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import javax.xml.xpath.XPathExpressionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;
import org.opensaml.core.config.InitializationException;
import org.opensaml.core.config.InitializationService;
import org.opensaml.core.config.Initializer;
import org.opensaml.saml.metadata.resolver.MetadataResolver;

import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.session.ActivatedFrontendConfig;
import com.floragunn.searchguard.authc.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.authc.session.GetActivatedFrontendConfigAction;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.Attributes;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.PrivilegedCode;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.google.common.base.Strings;
import com.onelogin.saml2.authn.AuthnRequest;
import com.onelogin.saml2.authn.SamlResponse;
import com.onelogin.saml2.exception.ValidationError;
import com.onelogin.saml2.logout.LogoutRequest;
import com.onelogin.saml2.settings.Saml2Settings;
import com.onelogin.saml2.util.Constants;
import com.onelogin.saml2.util.Util;

import net.shibboleth.utilities.java.support.component.ComponentInitializationException;
import net.shibboleth.utilities.java.support.component.DestructableComponent;
import net.shibboleth.utilities.java.support.resolver.ResolverException;

public class SamlAuthenticator implements ApiAuthenticationFrontend, AutoCloseable {
    private static final String SSO_CONTEXT_PREFIX = "saml_request_id:";
    protected final static Logger log = LogManager.getLogger(SamlAuthenticator.class);
    private static boolean openSamlInitialized = false;

    private URI idpMetadataUrl;
    private String idpMetadataXml;
    private String spSignatureAlgorithm;
    private Boolean useForceAuthn;
    private PrivateKey spSignaturePrivateKey;
    private Saml2SettingsProvider saml2SettingsProvider;
    private MetadataResolver metadataResolver;
    private boolean checkIssuer;

    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "saml", SamlAuthenticator.class).initialized()
            .requiresEnterpriseLicense();

    public SamlAuthenticator(Map<String, Object> config, ConfigurationRepository.Context context) throws ConfigValidationException {
        ensureOpenSamlInitialization();

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors, context);

        idpMetadataUrl = vNode.get("idp.metadata_url").asURI();
        idpMetadataXml = vNode.get("idp.metadata_xml").asString();

        spSignatureAlgorithm = vNode.get("sp.signature_algorithm").withDefault(Constants.RSA_SHA256).asString();
        String spSignaturePrivateKeyPassword = vNode.get("sp.signature_private_key_password").asString();
        spSignaturePrivateKey = vNode.get("sp.signature_private_key").byString((pem) -> TLSConfig.toPrivateKey(pem, spSignaturePrivateKeyPassword));
        useForceAuthn = vNode.get("sp.forceAuthn").asBoolean();
        checkIssuer = vNode.get("check_issuer").withDefault(true).asBoolean();

        String idpEntityId = vNode.get("idp.entity_id").required().asString();
        String spEntityId = vNode.get("sp.entity_id").asString();

        if (idpMetadataUrl == null && idpMetadataXml == null) {
            validationErrors.add(new com.floragunn.codova.validation.errors.ValidationError("idp.metadata_url",
                    "idp.metadata_url and idp.metadata_xml are unconfigured"));
        }

        TLSConfig tlsConfig = vNode.get("idp.tls").by((Parser<TLSConfig, Parser.Context>) TLSConfig::parse);

        if (!context.isExternalResourceCreationEnabled()) {
            this.metadataResolver = null;
        } else if (idpMetadataUrl != null) {
            try {
                SamlHTTPMetadataResolver metadataResolver = PrivilegedCode.execute(() -> new SamlHTTPMetadataResolver(idpMetadataUrl, tlsConfig), ResolverException.class);

                long refreshDelayMillis = vNode.get("idp.min_refresh_delay").withDefault(60L * 1000L).asLong();
                metadataResolver.setMinRefreshDelay(millisToDuration(refreshDelayMillis));
                long maxRefreshDurationMillis = vNode.get("idp.max_refresh_delay").withDefault(14400000L).asLong();
                metadataResolver.setMaxRefreshDelay(millisToDuration(maxRefreshDurationMillis));
                metadataResolver.setRefreshDelayFactor(vNode.get("idp.refresh_delay_factor").withDefault(0.75f).asFloat());

                metadataResolver.initializePrivileged();

                this.metadataResolver = metadataResolver;
            } catch (ResolverException | ComponentInitializationException e) {
                log.warn("Error while initializing " + this, e);
                validationErrors.add(new com.floragunn.codova.validation.errors.ValidationError("idp.metadata_url", e.getMessage()).cause(e));
            }
        } else if (idpMetadataXml != null) {
            try {
                StaticMetadataResolver metadataResolver = PrivilegedCode.execute(() -> new StaticMetadataResolver(idpMetadataXml.trim()), ConfigValidationException.class);

                metadataResolver.initializePrivileged();

                this.metadataResolver = metadataResolver;
            } catch (ConfigValidationException e) {
                log.warn("Error while initializing " + this, e);
                validationErrors.add("idp.metadata_xml", e);
            } catch (ComponentInitializationException e) {
                log.warn("Error while initializing " + this, e);
                validationErrors.add(new com.floragunn.codova.validation.errors.ValidationError("idp.metadata_xml", e.getMessage()).cause(e));
            }
        }

        vNode.used("validator", "idp.min_refresh_delay", "idp.max_refresh_delay", "idp.refresh_delay_factor");
        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        this.saml2SettingsProvider = new Saml2SettingsProvider(idpEntityId, spEntityId,
                Settings.builder().loadFromMap(vNode.getDocumentNode().toMap()).build().getAsSettings("validator"), this.metadataResolver);
        try {
            this.saml2SettingsProvider.get(URI.create("https://kibana.test"));
        } catch (Exception e) {
            log.debug(
                    "Exception while initializing Saml2SettingsProvider. Possibly, the IdP is unreachable right now. This is recoverable by a meta data refresh.",
                    e);
        }
    }

    private static Duration millisToDuration(long refreshDelayMillis) {
        return Duration.of(refreshDelayMillis, ChronoUnit.MILLIS);
    }

    @Override
    public ActivatedFrontendConfig.AuthMethod activateFrontendConfig(ActivatedFrontendConfig.AuthMethod frontendConfig,
            GetActivatedFrontendConfigAction.Request request) throws AuthenticatorUnavailableException {
        try {
            if (request.getFrontendBaseUrl() == null) {
                throw new AuthenticatorUnavailableException("Configuration error", "frontend_base_url is required for SAML authentication")
                        .details("request", request.toBasicObject());
            }

            URI frontendBaseUrl = new URI(request.getFrontendBaseUrl());

            Saml2Settings saml2Settings = this.saml2SettingsProvider.getCached(frontendBaseUrl);
            AuthnRequest authnRequest = this.buildAuthnRequest(saml2Settings);

            String samlRequest;

            try {
                samlRequest = authnRequest.getEncodedAuthnRequest(true);
            } catch (IOException e) {
                throw new AuthenticatorUnavailableException("Internal error while creating SAML request", e);
            }

            String ssoLocation = getSamlRequestRedirectBindingLocation(IdpEndpointType.SSO, saml2Settings, samlRequest, request.getNextURL());
            String ssoContext = SSO_CONTEXT_PREFIX + authnRequest.getId();

            return frontendConfig.ssoLocation(ssoLocation).ssoContext(ssoContext);
        } catch (URISyntaxException e) {
            log.error("Error while activating SAML authenticator", e);
            throw new AuthenticatorUnavailableException("frontend_base_url is not a valid URL", e).details("request", request.toBasicObject());
        }
    }

    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request)
            throws CredentialsException, ConfigValidationException, AuthenticatorUnavailableException {
        Map<String, Object> debugDetails = new HashMap<>();

        if (!request.containsKey("saml_response")) {
            throw new ConfigValidationException(new MissingAttribute("saml_response"));
        }

        if (!request.containsKey("frontend_base_url")) {
            throw new ConfigValidationException(new MissingAttribute("frontend_base_url"));
        }

        URI frontendBaseUrl;

        try {
            frontendBaseUrl = new URI(String.valueOf(request.get("frontend_base_url")));
        } catch (URISyntaxException e) {
            throw new ConfigValidationException(new InvalidAttributeValue("frontend_base_url", request.get("frontend_base_url"), "A URL"));
        }

        String samlResponseBase64 = String.valueOf(request.get("saml_response"));

        String acsEndpoint;
        String samlRequestId;

        String ssoContext = request.containsKey("sso_context") ? String.valueOf(request.get("sso_context")) : null;

        debugDetails.put("saml_response", samlResponseBase64);
        debugDetails.put("sso_context", ssoContext);

        Saml2Settings saml2Settings = this.saml2SettingsProvider.getCached(frontendBaseUrl);

        if (ssoContext != null) {
            log.debug("Detected IdP initiated SSO; ssoContext: " + ssoContext);

            if (!ssoContext.startsWith(SSO_CONTEXT_PREFIX)) {
                throw new ConfigValidationException(new InvalidAttributeValue("saml_response", ssoContext, "Must start with " + SSO_CONTEXT_PREFIX));
            }

            samlRequestId = ssoContext.substring(SSO_CONTEXT_PREFIX.length());
            acsEndpoint = saml2Settings.getSpAssertionConsumerServiceUrl().toString();
        } else {
            log.debug("Detected IdP initiated SSO");

            acsEndpoint = saml2Settings.getSpAssertionConsumerServiceUrl().toString() + "/idpinitiated";
            samlRequestId = null;
        }

        try {
            SamlResponse samlResponse = createSamlResponse(saml2Settings, acsEndpoint, samlResponseBase64, debugDetails);

            PrivilegedCode.execute(() -> {
                try {
                    debugDetails.put("saml_response_attributes", samlResponse.getAttributes());
                    debugDetails.put("saml_response_issuer", samlResponse.getResponseIssuer());
                    debugDetails.put("saml_response_assertion_issuer", samlResponse.getAssertionIssuer());
                    debugDetails.put("saml_response_audiences", samlResponse.getAudiences());
                    debugDetails.put("saml_response_name_id_data", samlResponse.getNameIdData());
                } catch (Exception e) {
                    log.warn("Got Exception while collecting debug details", e);
                }
            });

            if (checkIssuer) {
                this.ensureResponseFromConfiguredEntity(samlResponse, saml2Settings, debugDetails);
            }

            if (!PrivilegedCode.execute(() -> samlResponse.isValid(samlRequestId))) {
                log.info("Error while validating SAML response " + samlRequestId, samlResponse.getValidationException());
                throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false,
                        "Invalid SAML response: " + samlResponse.getValidationException(), debugDetails));
            }

            String sessionIndex;
            String nameId;
            String nameIdFormat;

            try {
                sessionIndex = PrivilegedCode.execute(() -> samlResponse.getSessionIndex(), XPathExpressionException.class);
                nameId = PrivilegedCode.execute(() -> samlResponse.getNameId(), Exception.class);
                nameIdFormat = PrivilegedCode.execute(
                        () -> samlResponse.getNameIdFormat() != null ? SamlNameIdFormat.getByUri(samlResponse.getNameIdFormat()).getShortName()
                                : null,
                        Exception.class);
            } catch (Exception e) {
                throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false,
                        "Error while extracting meta data from SAML response: " + e.getMessage(), debugDetails), e);
            }

            return AuthCredentials.forUser(nameId).userMappingAttribute("saml_response", extractAttributes(samlResponse))
                    .attribute(Attributes.AUTH_TYPE, "saml").attribute("__saml_si", sessionIndex).attribute("__saml_nif", nameIdFormat)
                    .attribute("__saml_nid", nameId).attribute("__fe_base_url", frontendBaseUrl.toString()).complete().build();
        } catch (ValidationError e) {
            log.warn("Error while validating SAML response", e);
            throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false,
                    "Invalid SAML response: " + e.getMessage() + "; error_code: " + e.getErrorCode(), debugDetails), e);
        }
    }

    @Override
    public String getLogoutUrl(User user) throws AuthenticatorUnavailableException {
        try {
            if (user == null) {
                return null;
            }

            if (user.getStructuredAttributes().get("__fe_base_url") == null) {
                return null;
            }

            URI frontendBaseUrl;

            try {
                frontendBaseUrl = new URI(String.valueOf(user.getStructuredAttributes().get("__fe_base_url")));
            } catch (URISyntaxException e) {
                log.error("Cannot parse __fe_base_url of " + user + ": " + user.getStructuredAttributes().get("__fe_base_url"), e);

                return null;
            }

            Saml2Settings saml2Settings = this.saml2SettingsProvider.getCached(frontendBaseUrl);

            if (!isSingleLogoutAvailable(saml2Settings)) {
                return null;
            }

            String nameId = user.getStructuredAttributes().get("__saml_nid") != null ? user.getStructuredAttributes().get("__saml_nid").toString()
                    : user.getName();
            String nameIdFormat = user.getStructuredAttributes().get("__saml_nif") != null
                    ? user.getStructuredAttributes().get("__saml_nif").toString()
                    : null;
            String sessionIndex = user.getStructuredAttributes().get("__saml_si") != null ? user.getStructuredAttributes().get("__saml_si").toString()
                    : null;

            LogoutRequest logoutRequest = new LogoutRequest(saml2Settings, null, nameId, sessionIndex, nameIdFormat);

            return getSamlRequestRedirectBindingLocation(IdpEndpointType.SLO, saml2Settings, logoutRequest.getEncodedLogoutRequest(true), null);

        } catch (IOException e) {
            log.error("Error while building logout URL for " + this, e);
            return null;
        }
    }

    @Override
    public String getType() {
        return "saml";
    }

    private SamlResponse createSamlResponse(Saml2Settings saml2Settings, String acsEndpoint, String samlResponseBase64,
            Map<String, Object> debugDetails) throws ValidationError {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<SamlResponse>) () -> {
                return new SamlResponse(saml2Settings, acsEndpoint, samlResponseBase64);
            });
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof ValidationError) {
                throw (ValidationError) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    private void ensureResponseFromConfiguredEntity(SamlResponse samlResponse, Saml2Settings saml2Settings, Map<String, Object> debugDetails)
            throws CredentialsException {

        try {
            String issuer = PrivilegedCode.<String, ValidationError, XPathExpressionException>execute(() -> samlResponse.getResponseIssuer(),
                    ValidationError.class, XPathExpressionException.class);
            if (issuer != null) {

                if (!issuer.equals(saml2Settings.getIdpEntityId())) {
                    if (log.isDebugEnabled()) {
                        log.debug("Response is not from " + saml2Settings.getIdpEntityId() + " but from " + issuer
                                + "\nWill abort check. If there are other SAML auth frontends, these might be successfully authenticate using this response");
                    }
                    throw new CredentialsException(
                            new AuthcResult.DebugInfo(getType(), false, "Expected issuer " + saml2Settings.getIdpEntityId(), debugDetails));
                }
            }
        } catch (ValidationError | XPathExpressionException e) {
            log.error("Error while checking issuer in " + samlResponse, e);
            throw new CredentialsException(new AuthcResult.DebugInfo(getType(), false, "Error while checking issuer: " + e, debugDetails), e);

        }
    }

    private AuthnRequest buildAuthnRequest(Saml2Settings saml2Settings) {
        boolean forceAuthn = false;

        if (this.useForceAuthn != null) {
            forceAuthn = this.useForceAuthn.booleanValue();
        } else {
            if (!this.isSingleLogoutAvailable(saml2Settings)) {
                forceAuthn = true;
            }
        }

        return new AuthnRequest(saml2Settings, forceAuthn, false, true);
    }

    private URL getIdpUrl(IdpEndpointType endpointType, Saml2Settings saml2Settings) {
        if (endpointType == IdpEndpointType.SSO) {
            return saml2Settings.getIdpSingleSignOnServiceUrl();
        } else {
            return saml2Settings.getIdpSingleLogoutServiceUrl();
        }
    }

    private boolean isSingleLogoutAvailable(Saml2Settings saml2Settings) {
        return saml2Settings.getIdpSingleLogoutServiceUrl() != null;
    }

    @Override
    public void close() {
        if (this.metadataResolver instanceof DestructableComponent) {
            ((DestructableComponent) this.metadataResolver).destroy();
        }
    }

    private Map<String, ?> extractAttributes(SamlResponse samlResponse) {
        try {
            return PrivilegedCode.<Map<String, ?>, ValidationError, XPathExpressionException>execute(() -> samlResponse.getAttributes(),
                    ValidationError.class, XPathExpressionException.class);
        } catch (ValidationError | XPathExpressionException e) {
            log.warn("Got exception while extracting attributes from " + samlResponse, e);
            return ImmutableMap.empty();
        }
    }

    static void ensureOpenSamlInitialization() {
        if (openSamlInitialized) {
            return;
        }

        try {
            PrivilegedCode.execute(() -> {
                Thread thread = Thread.currentThread();
                ClassLoader originalClassLoader = thread.getContextClassLoader();

                try {

                    thread.setContextClassLoader(InitializationService.class.getClassLoader());
                    
                    synchronized (SamlAuthenticator.class) {
                        Iterator<Initializer> iter = ServiceLoader.load(Initializer.class).iterator();
                        while (iter.hasNext()) {
                            try {
                                Initializer initializer  = iter.next();

                                if (initializer instanceof org.opensaml.security.config.GlobalNamedCurveRegistryInitializer) {
                                    // Skip this one. It is not necessary and makes issues with BouncyCastle
                                    continue;
                                }
                                
                                initializer.init();
                            } catch (final InitializationException e) {
                                log.error("Error initializing module: {}", e.getMessage());
                                throw e;
                            }
                        }
                        
                        new org.opensaml.saml.config.impl.XMLObjectProviderInitializer().init();
                        new org.opensaml.saml.config.impl.SAMLConfigurationInitializer().init();
                        new org.opensaml.xmlsec.config.impl.XMLObjectProviderInitializer().init();
                        
                    }

                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }

                openSamlInitialized = true;
            }, InitializationException.class);
        } catch (InitializationException e) {
            throw new RuntimeException("Error while initializing SAML", e);
        }
    }

    private String getSamlRequestRedirectBindingLocation(IdpEndpointType idpEndpointType, Saml2Settings saml2Settings, String samlRequest,
            String relayState) throws AuthenticatorUnavailableException {

        URL idpUrl = getIdpUrl(idpEndpointType, saml2Settings);

        if (Strings.isNullOrEmpty(idpUrl.getQuery())) {
            return getIdpUrl(idpEndpointType, saml2Settings) + "?" + this.getSamlRequestQueryString(samlRequest, relayState);
        } else {
            return getIdpUrl(idpEndpointType, saml2Settings) + "&" + this.getSamlRequestQueryString(samlRequest, relayState);
        }

    }

    private String getSamlRequestQueryString(String samlRequest, String relayState) throws AuthenticatorUnavailableException {

        String queryString = "SAMLRequest=" + Util.urlEncoder(samlRequest);

        if (relayState != null) {
            queryString += "&RelayState=" + Util.urlEncoder(relayState);
        }

        if (this.spSignaturePrivateKey == null) {
            return queryString;
        }

        queryString += "&SigAlg=" + Util.urlEncoder(this.spSignatureAlgorithm);

        String signature = getSamlRequestQueryStringSignature(queryString);

        queryString += "&Signature=" + Util.urlEncoder(signature);

        return queryString;
    }

    private String getSamlRequestQueryStringSignature(String samlRequestQueryString) throws AuthenticatorUnavailableException {
        try {
            return Util.base64encoder(Util.sign(samlRequestQueryString, this.spSignaturePrivateKey, this.spSignatureAlgorithm));
        } catch (Exception e) {
            throw new AuthenticatorUnavailableException("Error while signing SAML request", e);
        }
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    private enum IdpEndpointType {
        SSO, SLO
    }

    public static TypedComponent.Info<ApiAuthenticationFrontend> INFO = new TypedComponent.Info<ApiAuthenticationFrontend>() {

        @Override
        public Class<ApiAuthenticationFrontend> getType() {
            return ApiAuthenticationFrontend.class;
        }

        @Override
        public String getName() {
            return "saml";
        }

        @Override
        public Factory<ApiAuthenticationFrontend> getFactory() {
            return SamlAuthenticator::new;
        }
    };

}
