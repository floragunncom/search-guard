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

package com.floragunn.searchguard.enterprise.auth.kerberos;

import java.io.File;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.TypedComponent.Factory;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.authc.rest.HttpAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.PrivilegedCode;
import com.floragunn.searchsupport.cstate.ComponentState;

public class KerberosAuthenticationFrontend implements HttpAuthenticationFrontend {

    private static final String TYPE = "kerberos";

    private static final Oid SPNEGO_OID = createOid("1.3.6.1.5.5.2");
    private static final Oid KRB5MECH_OID = createOid("1.2.840.113554.1.2.2");

    private static final Oid[] KRB_OIDS = new Oid[] { SPNEGO_OID, KRB5MECH_OID };

    private static final Logger log = LogManager.getLogger(KerberosAuthenticationFrontend.class);

    private final boolean stripRealmFromPrincipalName;
    private final boolean challenge;
    private final boolean debug;
    private final ImmutableSet<KerberosPrincipal> acceptorPrincipal;
    private final Path acceptorKeyTabFile;
    private final javax.security.auth.login.Configuration keytabConfiguration;

    private final ComponentState componentState = new ComponentState(0, "authentication_frontend", TYPE, KerberosAuthenticationFrontend.class).requiresEnterpriseLicense();

    public KerberosAuthenticationFrontend(DocNode docNode, ConfigurationRepository.Context context) throws ConfigValidationException {
        log.info("KerberosAuthenticationFrontend docNode: {}", docNode.toPrettyJsonString());
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        this.challenge = vNode.get("challenge").withDefault(true).asBoolean();
        this.debug = vNode.get("debug").withDefault(false).asBoolean();

        boolean useSystemProperties = vNode.get("use_system_properties").withDefault(false).asBoolean();

        Path krb5configFile;

        if (!useSystemProperties) {
            krb5configFile = resolve(vNode.get("krb5_config_file").withDefault("/etc/krb5.conf").asString(), "krb5_config_file", validationErrors,
                    context);
        } else {
            krb5configFile = null;
            vNode.used("krb5_config_file");
        }

        this.stripRealmFromPrincipalName = vNode.get("strip_realm_from_principal").withDefault(true).asBoolean();
        this.acceptorPrincipal = ImmutableSet.of(vNode.get("acceptor_principal").asList().withEmptyListAsDefault()
                .ofObjectsParsedByString((s) -> PrivilegedCode.execute(() -> new KerberosPrincipal(s))));
        String acceptorKeyTabFile = vNode.get("acceptor_keytab").required().asString();
        log.info("acceptorKeyTabFile: {}", acceptorKeyTabFile);
        this.acceptorKeyTabFile = resolve(acceptorKeyTabFile, "acceptor_keytab", validationErrors, context);

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        if (!useSystemProperties) {
            PrivilegedCode.execute(() -> {
                try {
                    if (this.debug) {
                        System.setProperty("sun.security.krb5.debug", "true");
                        System.setProperty("java.security.debug", "gssloginconfig,logincontext,configparser,configfile");
                        System.setProperty("sun.security.spnego.debug", "true");
                        System.out.println("Kerberos debug is enabled");
                        System.err.println("Kerberos debug is enabled");
                        log.info("Kerberos debug is enabled on stdout");
                    }
                } catch (Throwable e) {
                    log.error("Unable to enable krb_debug due to ", e);
                    System.err.println("Unable to enable krb_debug due to " + e);
                    System.out.println("Unable to enable krb_debug due to " + e);
                }

                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                System.setProperty("java.security.krb5.conf", krb5configFile.toString());
            });
        }

        this.keytabConfiguration = new KeytabJaasConf("*", this.acceptorKeyTabFile, false, this.debug);

        try {
            Subject loginSubject = PrivilegedCode.execute(() -> getLoginSubject(), LoginException.class);

            if (log.isDebugEnabled()) {
                log.debug("loginSubject: " + loginSubject);
            }
        } catch (LoginException e) {
            log.error("Got login exception", e);
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }
        
        this.componentState.initialized();
    }

    @Override
    public AuthCredentials extractCredentials(RequestMetaData<?> request) throws CredentialsException {
        String negotiateHeader = request.getAuthorizationByScheme("negotiate");

        if (negotiateHeader == null) {
            log.debug("No negotiate authorization header found");
            return null;
        }

        byte[] decodedNegotiateHeader = Base64.getDecoder().decode(negotiateHeader);

        return PrivilegedCode.execute(() -> {
            Subject loginSubject;

            try {
                loginSubject = PrivilegedCode.execute(() -> getLoginSubject(), LoginException.class);
            } catch (LoginException e) {
                throw new CredentialsException("Unable to authenticate with SPNEGO", new AuthcResult.DebugInfo(getType(), false, e.getMessage(),
                        ImmutableMap.of("acceptor_principal", this.acceptorPrincipal.toString(), "keytab", this.keytabConfiguration.toString())), e);
            }

            GSSManager manager = GSSManager.getInstance();

            GSSContext gssContext;
            try {
                gssContext = manager.createContext(Subject.doAs(loginSubject, (PrivilegedExceptionAction<GSSCredential>) () -> manager
                        .createCredential(null, GSSCredential.INDEFINITE_LIFETIME, KRB_OIDS, GSSCredential.ACCEPT_ONLY)));
            } catch (GSSException e) {
                log.warn("Exception while creating GSSContext", e);
                throw new CredentialsException("Unable to authenticate with SPNEGO", new AuthcResult.DebugInfo(getType(), false,
                        "Exception while creating GSSContext", ImmutableMap.of("login_subject", loginSubject.toString())), e);
            } catch (PrivilegedActionException e) {
                log.warn("Exception while creating GSSContext", e.getCause());
                throw new CredentialsException("Unable to authenticate with SPNEGO", new AuthcResult.DebugInfo(getType(), false,
                        "Exception while creating GSSContext", ImmutableMap.of("login_subject", loginSubject.toString())), e.getCause());
            }

            try {
                byte[] outToken;
                try {
                    outToken = Subject.doAs(loginSubject, (PrivilegedExceptionAction<byte[]>) () -> gssContext
                            .acceptSecContext(decodedNegotiateHeader, 0, decodedNegotiateHeader.length));
                } catch (PrivilegedActionException e) {
                    log.info("Exception while GSSContext.acceptSecContext()", e.getCause());
                    throw new CredentialsException("Unable to authenticate with SPNEGO", new AuthcResult.DebugInfo(getType(), false,
                            "Did not accept negotiate header", ImmutableMap.of("login_subject", loginSubject.toString())), e.getCause());
                }

                if (outToken == null) {
                    log.debug("Ticket validation not successful, outToken is null");
                    throw new CredentialsException("Unable to authenticate with SPNEGO", new AuthcResult.DebugInfo(getType(), false,
                            "Ticket validation not successful, outToken is null", ImmutableMap.of("login_subject", loginSubject.toString())));
                }

                String username = Subject.doAs(loginSubject, (PrivilegedAction<String>) () -> getUsername(gssContext));

                if (username == null) {
                    // challenge
                    return AuthCredentials.forUser("_incomplete_").authenticatorType(getType()).nativeCredentials(outToken).build();
                }

                if (stripRealmFromPrincipalName) {
                    username = stripRealm(username);
                }

                return AuthCredentials.forUser(username).authenticatorType(getType()).nativeCredentials(outToken).complete().build();
            } finally {
                try {
                    gssContext.dispose();
                } catch (GSSException e) {
                    log.warn("Exception while disposing gssContext", e);
                }
            }
        }, CredentialsException.class);
    }

    @Override
    public String getChallenge(AuthCredentials credentials) {
        if (challenge) {
            if (credentials != null && credentials.getNativeCredentials() != null) {
                return "Negotiate " + Base64.getEncoder().encodeToString((byte[]) credentials.getNativeCredentials());
            } else {
                return "Negotiate";
            }
        } else {
            return null;
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    private Path resolve(String value, String property, ValidationErrors validationErrors, ConfigurationRepository.Context context) {
        if (value == null) {
            return null;
        }

        File file = new File(value);
        Path path;

        if (file.isAbsolute()) {
            path = file.toPath();
        } else {
            path = context.getStaticSettings().getPatformConfigDirectory().resolve(file.toPath());
        }

        return path;
    }

    private Subject getLoginSubject() throws LoginException {
        Subject subject = new Subject(false, this.acceptorPrincipal, ImmutableSet.empty(), ImmutableSet.empty());
        LoginContext loginContext = new LoginContext("KeytabConf", subject, null, keytabConfiguration);
        loginContext.login();
        return loginContext.getSubject();
    }

    private static String stripRealm(String name) {
        if (name == null) {
            return null;
        }
        int pos = name.indexOf('@');
        if (pos > 0) {
            return name.substring(0, pos);
        } else {
            return name;
        }
    }

    public static TypedComponent.Info<HttpAuthenticationFrontend> INFO = new TypedComponent.Info<HttpAuthenticationFrontend>() {

        @Override
        public Class<HttpAuthenticationFrontend> getType() {
            return HttpAuthenticationFrontend.class;
        }

        @Override
        public String getName() {
            return TYPE;
        }

        @Override
        public Factory<HttpAuthenticationFrontend> getFactory() {
            return (config, context) -> new KerberosAuthenticationFrontend(config, context);
        }
    };

    private static Oid createOid(String string) {
        try {
            return new Oid(string);
        } catch (GSSException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getUsername(GSSContext gssContext) {
        try {
            if (!gssContext.isEstablished()) {
                return null;
            }

            GSSName gssName = gssContext.getSrcName();

            if (gssName != null) {
                return gssName.toString();
            } else {
                return null;
            }
        } catch (GSSException e) {
            log.warn("Error while retrieving name from " + gssContext, e);
            return null;
        }
    }
}
