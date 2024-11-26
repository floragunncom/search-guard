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

import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.util.FakeRestRequest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.SerializationException;
import io.jsonwebtoken.io.Serializer;
import io.jsonwebtoken.security.Keys;

public class HTTPJwtAuthenticatorTest {

    final static byte[] secretKey = new byte[1024];
    
    static {
        new SecureRandom().nextBytes(secretKey);
        LogConfigurator.configureESLogging();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testNoKey() throws Exception {
        Settings settings = Settings.builder().build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth =new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }

    @Test
    public void testEmptyKey() throws Exception {
        Settings settings = Settings.builder().put("signing_key", "").build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }
    
    @Test
    public void testBadKey() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(new byte[]{1,3,3,4,3,6,7,8,3,10})).build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }

    @Test
    public void testTokenMissing() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).build();
                
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
                
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }
    
    @Test
    public void testInvalid() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).build();
        
        String jwsToken = "123invalidtoken..";
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }
    
    @Test
    public void testBearer() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").audience().add("myaud").and().signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
        Assert.assertEquals(2, creds.getAttributes().size());
    }

    @Test
    public void testBearerWrongPosition() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken + "Bearer " + " 123");
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }
    
    @Test
    public void testNonBearer() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    }
    
    @Test
    public void testRoles() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("roles_key", "roles")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .claim("roles", "role1,role2")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(2, creds.getBackendRoles().size());
    }

    @Test
    public void testApi() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("roles_key", "roles")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .claim("roles", "role1,role2")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        
        AuthCredentials creds = jwtAuth.extractCredentials(ImmutableMap.of("jwt", jwsToken));
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(2, creds.getBackendRoles().size());
    }

    
    @Test
    public void testNullClaim() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("roles_key", "roles")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .claim("roles", null)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    } 

    @Test
    public void testNonStringClaim() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("roles_key", "roles")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .claim("roles", 123L)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(1, creds.getBackendRoles().size());
        Assert.assertTrue( creds.getBackendRoles().contains("123"));
    }
    
    @Test
    public void testRolesMissing() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("roles_key", "roles")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    }    
    
    @Test
    public void testWrongSubjectKey() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_key", "missing")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("roles", "role1,role2")
                .claim("asub", "Dr. Who")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }
    
    @Test
    public void testAlternativeSubject() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_key", "asub")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .claim("roles", "role1,role2")
                .claim("asub", "Dr. Who")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Dr. Who", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    }

    @Test
    public void testNonStringAlternativeSubject() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_key", "asub")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .claim("roles", "role1,role2")
                .claim("asub", false)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("false", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    }
    
    @Test
    public void testUrlParam() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("jwt_url_parameter", "abc")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Leonard McCoy")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        FakeRestRequest req = new FakeRestRequest(headers, new HashMap<>());
        req.params().put("abc", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(req, null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    }
    
    @Test
    public void testExp() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Expired")
                .expiration(new Date(100))
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }
    
    @Test
    public void testNbf() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .subject("Expired")
                .notBefore(new Date(System.currentTimeMillis()+(1000*36000)))
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }
    
    @Test
    public void testRS256() throws Exception {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair pair = keyGen.generateKeyPair();
        PrivateKey priv = pair.getPrivate();
        PublicKey pub = pair.getPublic();
    
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").signWith(priv, Jwts.SIG.RS256).compact();
        Settings settings = Settings.builder().put("signing_key", "-----BEGIN PUBLIC KEY-----\n"+BaseEncoding.base64().encode(pub.getEncoded())+"-----END PUBLIC KEY-----").build();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    }
    
    @Test
    public void testES512() throws Exception {
        KeyPair pair = Keys.keyPairFor(SignatureAlgorithm.ES512);
        PrivateKey priv = pair.getPrivate();
        PublicKey pub = pair.getPublic();
    
        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").signWith(priv, Jwts.SIG.ES512).compact();
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(pub.getEncoded())).build();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());
        Assert.assertEquals(0, creds.getBackendRoles().size());
    }
    
    @Test
    public void rolesArray() throws Exception {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("roles_key", "roles")
                .build();
        
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .content("{"+
                    "\"sub\": \"John Doe\","+
                    "\"roles\": [\"a\",\"b\",\"3rd\"]"+
                  "}")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        
        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer "+jwsToken);
        
        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("John Doe", creds.getUsername());
        Assert.assertEquals(3, creds.getBackendRoles().size());
        Assert.assertTrue(creds.getBackendRoles().contains("a"));
        Assert.assertTrue(creds.getBackendRoles().contains("b"));
        Assert.assertTrue(creds.getBackendRoles().contains("3rd"));
    }

    @Test
    public void testJsonPathRolesAndSubjectExpression() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("roles_path", "$['some_claim_name']['user']['roles']")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", "peter mueller");
        values.put("roles", "some role a, another role b");
        user.put("user", values);

        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("peter mueller", creds.getUsername());
        Assert.assertThat(creds.getBackendRoles(), CoreMatchers.hasItems("some role a", "another role b"));
    }

    @Test
    public void testJsonPathRolesAndSubjectExpressionWithSingleRole() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("roles_path", "$['some_claim_name']['user']['roles']")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", "peter mueller");
        values.put("roles", "some role a");
        user.put("user", values);

        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("peter mueller", creds.getUsername());
        Assert.assertThat(creds.getBackendRoles(), CoreMatchers.hasItem("some role a"));
    }

    @Test
    public void testJsonPathRolesAndSubjectExpressionWithCollection() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("roles_path", "$['some_claim_name']['user']['roles']")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", "peter mueller");
        values.put("roles", Arrays.asList("some role a, some role b", "some role c"));
        user.put("user", values);

        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("peter mueller", creds.getUsername());
        Assert.assertThat(creds.getBackendRoles(), CoreMatchers.hasItems("some role a", "some role b", "some role c"));
    }

    @Test
    public void testJsonPathRolesAndSubjectExpressionWithInvalidRolePath() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("roles_path", "$['some_claim_name']['asd']['roles']")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", "peter mueller");
        values.put("roles", "some role a");
        user.put("user", values);

        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("peter mueller", creds.getUsername());
        Assert.assertThat(creds.getBackendRoles(), Is.is(Collections.emptySet()));
    }

    @Test
    public void testInvalidJsonPathRolesAndSubjectExpression() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['123123asd']['asdf']['id']")
                .put("roles_path", "$['xyasd']['foo']['ss']")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", "peter mueller");
        values.put("roles", Arrays.asList("some role a", "another role b"));
        user.put("user", values);

        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    }

    @Test
    public void testIllegalJWTConfigurationDuplicateSubjects() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("subject_key", "foo")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", "peter mueller");
        values.put("roles", Arrays.asList("some role a", "another role b"));
        user.put("user", values);

        @SuppressWarnings("unused")
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        thrown.expect(IllegalStateException.class);
        new HTTPJwtAuthenticator(settings, null);
    }

    @Test
    public void testIllegalJWTConfigurationDuplicateRoleSpecification() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("roles_path", "$['xyasd']['foo']['ss']")
                .put("roles_key", "a, b, c")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        user.put("user", values);

        @SuppressWarnings("unused")
        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        thrown.expect(IllegalStateException.class);
        new HTTPJwtAuthenticator(settings, null);
    }

    @Test
    public void attributeAsArray() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).put("roles_key", "roles")
                .put("map_claims_to_user_attrs.attr_1", "claimsarray_string").put("map_claims_to_user_attrs.attr_2", "claimsarray_int")
                .put("map_claims_to_user_attrs.attr_3", "claimsarray_object").put("map_claims_to_user_attrs.attr_4", "claimsarray_mixed")
                .put("map_claims_to_user_attrs.attr_5", "claimsarray_empty").build();

        String jwsToken = Jwts.builder().json(jwtSerializer).content("{" + "\"sub\": \"John Doe\"," //
                + "\"claimsarray_string\": [\"a\",\"b\",\"c\"],"//
                + "\"claimsarray_int\": [1,2,3],"//
                + "\"claimsarray_object\": { \"objectarray\": []},"//
                + "\"claimsarray_mixed\": [\"a\",\"b\",1],"//
                + "\"claimsarray_empty\": []"//
                + "}").signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("John Doe", creds.getUsername());
        Assert.assertEquals(Arrays.asList("a", "b", "c"), creds.getStructuredAttributes().get("attr_1"));
        Assert.assertEquals(Arrays.asList(1, 2, 3), creds.getStructuredAttributes().get("attr_2"));
        Assert.assertEquals(ImmutableMap.of("objectarray", Collections.emptyList()), creds.getStructuredAttributes().get("attr_3"));
        Assert.assertEquals(Arrays.asList("a", "b", 1), creds.getStructuredAttributes().get("attr_4"));
        Assert.assertEquals(Arrays.asList(), creds.getStructuredAttributes().get("attr_5"));

    }

    @Test
    public void testSubjectPattern() throws Exception {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).put("subject_pattern", "^(.+)@(?:.+)$")
                .build();

        String jwsToken = Jwts.builder().json(jwtSerializer).subject("leonard@mccoy.com").audience().add("myaud").and()
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("leonard", creds.getUsername());
    }
    
    @Test
    public void testSubjectPathWithList() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("roles_path", "$['some_claim_name']['user']['roles']")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", Arrays.asList("peter mueller"));
        values.put("roles", "some role a, another role b");
        user.put("user", values);

        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("peter mueller", creds.getUsername());
    }
 
    
    @Test
    public void testSubjectPathWithListSize2() {
        Settings settings = Settings.builder()
                .put("signing_key", BaseEncoding.base64().encode(secretKey))
                .put("subject_path", "$['some_claim_name']['user']['id']")
                .put("roles_path", "$['some_claim_name']['user']['roles']")
                .build();

        Map<String, Map<String, Object>> user = new HashMap<>();

        HashMap<String, Object> values = new HashMap<>();
        values.put("id", Arrays.asList("peter mueller", "lieschen mueller"));
        values.put("roles", "some role a, another role b");
        user.put("user", values);

        String jwsToken = Jwts.builder().json(jwtSerializer)
                .claim("some_claim_name", user)
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNull(creds);
    } 

    @Test
    public void testRequiredAudience() {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).put("required_audience", "test_audience")
                .build();

        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").audience().add("test_audience").and()
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());

        jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").audience().add("wrong_audience").and()
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        headers.put("Authorization", jwsToken);

        creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);

        Assert.assertNull(creds);
    }

    @Test
    public void testRequiredIssuer() {
        Settings settings = Settings.builder().put("signing_key", BaseEncoding.base64().encode(secretKey)).put("required_issuer", "test_issuer")
                .build();

        String jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").issuer("test_issuer")
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();

        HTTPJwtAuthenticator jwtAuth = new HTTPJwtAuthenticator(settings, null);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", jwsToken);

        AuthCredentials creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);
        Assert.assertNotNull(creds);
        Assert.assertEquals("Leonard McCoy", creds.getUsername());

        jwsToken = Jwts.builder().json(jwtSerializer).subject("Leonard McCoy").audience().add("wrong_issuer").and()
                .signWith(Keys.hmacShaKeyFor(secretKey), Jwts.SIG.HS512).compact();
        headers.put("Authorization", jwsToken);

        creds = jwtAuth.extractCredentials(new FakeRestRequest(headers, new HashMap<>()), null);

        Assert.assertNull(creds);
    }
    
    private static final Serializer<Map<String, ?>> jwtSerializer = new Serializer<Map<String, ?>>() {

        @Override
        public byte[] serialize(Map<String, ?> t) throws SerializationException {
            return DocWriter.json().writeAsBytes(t);
        }

        @Override
        public void serialize(Map<String, ?> stringMap, OutputStream out) throws SerializationException {
            try {
                out.write(DocWriter.json().writeAsBytes(stringMap));
            } catch (IOException e) {
                throw new SerializationException(e.getMessage(), e);
            }
        }
    };
}
