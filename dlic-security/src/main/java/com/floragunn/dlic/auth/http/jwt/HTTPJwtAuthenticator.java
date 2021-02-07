/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.jwt;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.dlic.util.Roles;
import com.floragunn.searchguard.auth.HTTPAuthenticator;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.UserAttributes;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.WeakKeyException;

public class HTTPJwtAuthenticator implements HTTPAuthenticator, ApiAuthenticationFrontend {

    
    private static final Logger log = LogManager.getLogger(HTTPJwtAuthenticator.class);
    
    private static final String BEARER = "bearer ";
    private final JwtParser jwtParser;
    private final String jwtHeaderName;
    private final String jwtUrlParameter;
    private final String rolesKey;
    private final String subjectKey;
    private final String jsonSubjectPath;
    private final String jsonRolesPath;
    private final String requireAudience;
    private final String requireIssuer;
    private Configuration jsonPathConfig;
    private Map<String, JsonPath> attributeMapping;
    private final Pattern subjectPattern;

    public HTTPJwtAuthenticator(final Settings settings, final Path configPath) {
        super();
        
        subjectPattern = getSubjectPattern(settings);

        JwtParser _jwtParser = null;
        
        try {
            String signingKey = settings.get("signing_key");
            
            if(signingKey == null || signingKey.length() == 0) {
                log.error("signingKey must not be null or empty. JWT authentication will not work");
            } else {

                signingKey = signingKey.replace("-----BEGIN PUBLIC KEY-----\n", "");
                signingKey = signingKey.replace("-----END PUBLIC KEY-----", "");

                byte[] decoded = Decoders.BASE64.decode(signingKey);
                Key key = null;

                try {
                    key = getPublicKey(decoded, "RSA");
                } catch (Exception e) {
                    log.debug("No public RSA key, try other algos ({})", e.toString());
                }

                try {
                    key = getPublicKey(decoded, "EC");
                } catch (Exception e) {
                    log.debug("No public ECDSA key, try other algos ({})", e.toString());
                }

                if(key != null) {
                    _jwtParser = Jwts.parser().setSigningKey(key);
                } else {
                    _jwtParser = Jwts.parser().setSigningKey(decoded);
                }

            }  
        } catch (Throwable e) {
            log.error("Error creating JWT authenticator: "+e+". JWT authentication will not work", e);
        }
        
        jwtUrlParameter = settings.get("jwt_url_parameter");
        jwtHeaderName = settings.get("jwt_header","Authorization");
        rolesKey = settings.get("roles_key");
        subjectKey = settings.get("subject_key");
        jsonRolesPath = settings.get("roles_path");
        jsonSubjectPath = settings.get("subject_path");
        requireAudience = settings.get("required_audience");
        requireIssuer = settings.get("required_issuer");
        
        if (requireAudience != null) {
            _jwtParser.requireAudience(requireAudience);
        }
        
        if (requireIssuer != null) {
            _jwtParser.requireIssuer(requireIssuer);
        }
        
        jwtParser = _jwtParser;
        attributeMapping = UserAttributes.getAttributeMapping(settings.getAsSettings("map_claims_to_user_attrs"));

        if ((subjectKey != null && jsonSubjectPath != null) || (rolesKey != null && jsonRolesPath != null)) {
            throw new IllegalStateException("Both, subject_key and subject_path or roles_key and roles_path have simultaneously provided." +
                    " Please provide only one combination.");
        }

        jsonPathConfig = BasicJsonPathDefaultConfiguration.builder().options(Option.ALWAYS_RETURN_LIST).build();
    }
    
    @Override
    public AuthCredentials extractCredentials(RestRequest request, ThreadContext context) throws ElasticsearchSecurityException {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged((PrivilegedAction<AuthCredentials>) () -> extractCredentials0(request));
    }
    
    @Override
    public AuthCredentials extractCredentials(Map<String, Object> request) throws ElasticsearchSecurityException, ConfigValidationException {
        String jwtString = request.containsKey("jwt") ? String.valueOf(request.get("jwt")) : null;

        if (jwtString == null) {
            throw new ConfigValidationException(new MissingAttribute("jwt"));
        }
        
        return extractCredentials(jwtString);
    }
 

    private AuthCredentials extractCredentials0(final RestRequest request) {        
        if (jwtParser == null) {
            log.error("Missing Signing Key. JWT authentication will not work");
            return null;
        }
        
        String jwtToken = request.header(jwtHeaderName);
        
        if((jwtToken == null || jwtToken.isEmpty()) && jwtUrlParameter != null) {
            jwtToken = request.param(jwtUrlParameter);
        } else {
            //just consume to avoid "contains unrecognized parameter"
            request.param(jwtUrlParameter);
        }
        
        if (jwtToken == null || jwtToken.length() == 0) {
            if(log.isDebugEnabled()) {
                log.debug("No JWT token found in '{}' {} header", jwtUrlParameter==null?jwtHeaderName:jwtUrlParameter, jwtUrlParameter==null?"header":"url parameter");
            }
            return null;
        }
        
        final int index;
        if((index = jwtToken.toLowerCase().indexOf(BEARER)) > -1) { //detect Bearer 
            jwtToken = jwtToken.substring(index+BEARER.length());
        } else {
            if(log.isDebugEnabled()) {
        	    log.debug("No Bearer scheme found in header");
            }
        }
        
        
        return extractCredentials(jwtToken);
    }
    
    private AuthCredentials extractCredentials(String jwtToken) {        
        if (jwtParser == null) {
            log.error("Missing Signing Key. JWT authentication will not work");
            return null;
        }
        
        try {

            Claims claims;
            
            try {
                claims = AccessController.doPrivileged((PrivilegedExceptionAction<Claims>) () -> jwtParser.parseClaimsJws(jwtToken).getBody());
            } catch (PrivilegedActionException e) {
                if (e.getCause() instanceof Exception) {
                    throw (Exception) e.getCause();
                } else {
                    throw new RuntimeException(e.getCause());
                }
            }
            
            final String subject = extractSubject(claims);
            
            if (subject == null) {
                log.error("No subject found in JWT token");
                return null;
            }
            
            final String[] roles = extractRoles(claims);
            
            return AuthCredentials.forUser(subject).authenticatorType(getType()).backendRoles(roles).attributesByJsonPath(attributeMapping, claims)
                    .prefixOldAttributes("attr.jwt.", claims).complete().build();
            
        } catch (WeakKeyException e) {
            log.error("Cannot authenticate user with JWT because of "+e, e);
            return null;
        } catch (Exception e) {
            if(log.isDebugEnabled()) {
                log.debug("Invalid or expired JWT token.", e);
            }
            return null;
        }
    }

    @Override
    public boolean reRequestAuthentication(final RestChannel channel, AuthCredentials creds) {
        final BytesRestResponse wwwAuthenticateResponse = new BytesRestResponse(RestStatus.UNAUTHORIZED,"");
        wwwAuthenticateResponse.addHeader("WWW-Authenticate", "Bearer realm=\"Search Guard\"");
        channel.sendResponse(wwwAuthenticateResponse);
        return true;
    }

    @Override
    public String getType() {
        return "jwt";
    }
    
    protected String extractSubject(final Claims claims) {
        String subject = claims.getSubject();        
        if(subjectKey != null) {
    		// try to get roles from claims, first as Object to avoid having to catch the ExpectedTypeException
            Object subjectObject = claims.get(subjectKey, Object.class);
            if(subjectObject == null) {
                log.warn("Failed to get subject from JWT claims, check if subject_key '{}' is correct.", subjectKey);
                return null;
            }
        	// We expect a String. If we find something else, convert to String but issue a warning
            if(!(subjectObject instanceof String)) {
        		log.warn("Expected type String for roles in the JWT for subject_key {}, but value was '{}' ({}). Will convert this value to String.", subjectKey, subjectObject, subjectObject.getClass());    					
            }
            subject = String.valueOf(subjectObject);
        }
        else if (jsonSubjectPath != null) {
            try {
                Object subjectObject = JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration()).parse(claims).read(jsonSubjectPath);

                if (subjectObject == null) {
                    log.error("The subject is null: " + jsonSubjectPath);
                    return null;
                }

                if (subjectObject instanceof Collection) {
                    Collection<?> subjectCollection = (Collection<?>) subjectObject;

                    if (subjectCollection.size() == 0) {
                        log.error("The subject is empty: " + jsonSubjectPath);
                        return null;
                    }

                    if (subjectCollection.size() > 1) {
                        log.error("More than one subject was found. Failing authentication: " + subjectObject + "; " + jsonSubjectPath);
                        return null;
                    }

                    subject = String.valueOf(subjectCollection.iterator().next());

                } else {
                    subject = String.valueOf(subjectObject);
                }

            } catch (PathNotFoundException e) {
                log.error("The provided JSON path {} could not be found ", jsonSubjectPath);
                return null;
            }
        }
        
        if (subject != null && subjectPattern != null) {
            Matcher matcher = subjectPattern.matcher(subject);
            
            if (!matcher.matches()) {
                log.warn("Subject " + subject + " does not match subject_pattern " + subjectPattern);
                return null;
            }
            
            if (matcher.groupCount() == 1) {
                subject = matcher.group(1);
            } else if (matcher.groupCount() > 1) {
                StringBuilder subjectBuilder = new StringBuilder();
                
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    if (matcher.group(i) != null) {
                        subjectBuilder.append(matcher.group(i));
                    }
                }
                
                if (subjectBuilder.length() != 0) {
                    subject = subjectBuilder.toString();
                } else {
                    subject = null;
                }
            }
        }
        
        return subject;
    }

    protected String[] extractRoles(final Claims claims) {
        if (rolesKey == null && jsonRolesPath == null) {
            return new String[0];
        }

        if (jsonRolesPath != null) {
            try {
                return Roles.split(JsonPath.using(jsonPathConfig).parse(claims).read(jsonRolesPath));
            } catch (PathNotFoundException e) {
                log.error("The provided JSON path {} could not be found ", jsonRolesPath);
                return new String[0];
            }
        }
        // try to get roles from claims, first as Object to avoid having to catch the ExpectedTypeException
        final Object rolesObject = claims.get(rolesKey, Object.class);
        if (rolesObject == null) {
            log.warn("Failed to get roles from JWT claims with roles_key '{}'. Check if this key is correct and available in the JWT payload.",
                    rolesKey);
            return new String[0];
        }

        return Roles.split(rolesObject);
    }

    private static PublicKey getPublicKey(final byte[] keyBytes, final String algo) throws NoSuchAlgorithmException, InvalidKeySpecException {
        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance(algo);
        return kf.generatePublic(spec);
    }

    private static Pattern getSubjectPattern(Settings settings) {
        String patternString = settings.get("subject_pattern");

        if (patternString == null) {
            return null;
        }

        try {
            return Pattern.compile(patternString);
        } catch (PatternSyntaxException e) {
            log.error("Invalid regular expression for subject_pattern: " + patternString, e);
            return null;
        }
    }

   
}
