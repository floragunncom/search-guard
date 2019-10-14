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

import java.util.HashMap;

import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.util.FakeRestRequest;
import com.google.common.collect.ImmutableMap;

public class SingleKeyHTTPJwtKeyByOpenIdConnectAuthenticatorTest {

	@Test
	public void basicTest() throws Exception {
		MockIpdServer mockIdpServer = new MockIpdServer(TestJwk.Jwks.RSA_1);
		try {
			Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri()).build();

			HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

			AuthCredentials creds = jwtAuth.extractCredentials(
					new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_RSA_1),
							new HashMap<String, String>()),
					null);

			Assert.assertNotNull(creds);
			Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
			Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
			Assert.assertEquals(0, creds.getBackendRoles().size());
			Assert.assertEquals(3, creds.getAttributes().size());

		} finally {
			try {
				mockIdpServer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void wrongSigTest() throws Exception {
		MockIpdServer mockIdpServer = new MockIpdServer(TestJwk.Jwks.RSA_1);
		try {
			Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri()).build();

			HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

			AuthCredentials creds = jwtAuth.extractCredentials(
					new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.NoKid.MC_COY_SIGNED_RSA_X),
							new HashMap<String, String>()),
					null);

			Assert.assertNull(creds);

		} finally {
			try {
				mockIdpServer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	

    @Test
    public void noAlgTest() throws Exception {
        MockIpdServer mockIdpServer = new MockIpdServer(TestJwk.Jwks.RSA_1_NO_ALG);
        try {
            Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri()).build();

            HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

            AuthCredentials creds = jwtAuth.extractCredentials(
                    new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.MC_COY_SIGNED_RSA_1),
                            new HashMap<String, String>()),
                    null);

            Assert.assertNotNull(creds);
            Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
            Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
            Assert.assertEquals(0, creds.getBackendRoles().size());
            Assert.assertEquals(3, creds.getAttributes().size());
        } finally {
            try {
                mockIdpServer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void mismatchedAlgTest() throws Exception {
        MockIpdServer mockIdpServer = new MockIpdServer(TestJwk.Jwks.RSA_1_WRONG_ALG);
        try {
            Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri()).build();

            HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

            AuthCredentials creds = jwtAuth.extractCredentials(
                    new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.NoKid.MC_COY_SIGNED_RSA_1),
                            new HashMap<String, String>()),
                    null);

            Assert.assertNull(creds);

        } finally {
            try {
                mockIdpServer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
	@Test
	public void keyExchangeTest() throws Exception {
		MockIpdServer mockIdpServer = new MockIpdServer(TestJwk.Jwks.RSA_1);

		Settings settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri()).build();

		HTTPJwtKeyByOpenIdConnectAuthenticator jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);

		try {
			AuthCredentials creds = jwtAuth.extractCredentials(
					new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.NoKid.MC_COY_SIGNED_RSA_1),
							new HashMap<String, String>()),
					null);

			Assert.assertNotNull(creds);
			Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
			Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
			Assert.assertEquals(0, creds.getBackendRoles().size());
			Assert.assertEquals(3, creds.getAttributes().size());

			creds = jwtAuth.extractCredentials(
					new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.NoKid.MC_COY_SIGNED_RSA_2),
							new HashMap<String, String>()),
					null);

			Assert.assertNull(creds);

			creds = jwtAuth.extractCredentials(
					new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.NoKid.MC_COY_SIGNED_RSA_X),
							new HashMap<String, String>()),
					null);

			Assert.assertNull(creds);
			
			creds = jwtAuth.extractCredentials(
					new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.NoKid.MC_COY_SIGNED_RSA_1),
							new HashMap<String, String>()),
					null);

			Assert.assertNotNull(creds);
			Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
			Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
			Assert.assertEquals(0, creds.getBackendRoles().size());
			Assert.assertEquals(3, creds.getAttributes().size());

		} finally {
			try {
				mockIdpServer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		mockIdpServer = new MockIpdServer(TestJwk.Jwks.RSA_2);
		settings = Settings.builder().put("openid_connect_url", mockIdpServer.getDiscoverUri()).build(); //port changed
		jwtAuth = new HTTPJwtKeyByOpenIdConnectAuthenticator(settings, null);
		
		try {
			AuthCredentials creds = jwtAuth.extractCredentials(
					new FakeRestRequest(ImmutableMap.of("Authorization", TestJwts.NoKid.MC_COY_SIGNED_RSA_2),
							new HashMap<String, String>()),
					null);

			Assert.assertNotNull(creds);
			Assert.assertEquals(TestJwts.MCCOY_SUBJECT, creds.getUsername());
			Assert.assertEquals(TestJwts.TEST_AUDIENCE, creds.getAttributes().get("attr.jwt.aud"));
			Assert.assertEquals(0, creds.getBackendRoles().size());
			Assert.assertEquals(3, creds.getAttributes().size());

		} finally {
			try {
				mockIdpServer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
