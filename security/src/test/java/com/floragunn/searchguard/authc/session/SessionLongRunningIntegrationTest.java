/*
 * Copyright 2022 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.authc.session;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.BearerAuthorization;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.google.common.io.BaseEncoding;

/**
 * These long-running integration tests are unsuitable for normal CI due to their run time. Thus, these are marked as @Ignore by default.
 */
public class SessionLongRunningIntegrationTest {
    private static final Logger log = LogManager.getLogger(SessionLongRunningIntegrationTest.class);

    static final Duration TIMEOUT = Duration.ofSeconds(60);

    static TestSgConfig.User BASIC_USER = new TestSgConfig.User("basic_user").roles("sg_all_access");
    static TestSgConfig.User NO_ROLES_USER = new TestSgConfig.User("no_roles_user");
    static TestSgConfig.Sessions SESSIONS = new TestSgConfig.Sessions().inactivityTimeout(TIMEOUT).refreshSessionActivityIndex(true);

    static TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));

    static TestSgConfig TEST_SG_CONFIG = new TestSgConfig().resources("session").authc(AUTHC)
            .frontendAuthc("default", new TestSgConfig.FrontendAuthc()
                    .authDomain(new TestSgConfig.FrontendAuthDomain("basic").label("Basic Login")))//
            .frontendAuthc("test_fe", new TestSgConfig.FrontendAuthc()
                    .authDomain(new TestSgConfig.FrontendAuthDomain(TestApiAuthenticationFrontend.class.getName()).label("Test Login")))
            .user(NO_ROLES_USER).user(BASIC_USER).sessions(SESSIONS);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().resources("session").sgConfig(TEST_SG_CONFIG).sslEnabled().build();

    @Ignore
    @Test
    public void singleUser() throws Exception {
        String token;

        try (GenericRestClient client = cluster.getRandomClientNode().getRestClient()) {
            HttpResponse response = client.postJson("/_searchguard/auth/session", basicAuthRequest(BASIC_USER));

            Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

            token = response.getBodyAsDocNode().getAsString("token");
         }

        for (int i = 0; i < 10; i++) {
            Thread.sleep(cluster.getRandom().nextInt((int) TIMEOUT.toMillis() - 2000) + 100);

            try (GenericRestClient restClient = cluster.getRandomClientNode().getRestClient(new BearerAuthorization(token))) {
                HttpResponse response = restClient.get("/_searchguard/authinfo");

                Assert.assertEquals(response.getBody(), 200, response.getStatusCode());
                Assert.assertEquals(response.getBody(), BASIC_USER.getName(), response.getBodyAsDocNode().getAsString("user_name"));
            }
        }

        Thread.sleep((int) TIMEOUT.toMillis() + 1000);

        try (GenericRestClient restClient = cluster.getRandomClientNode().getRestClient(new BearerAuthorization(token))) {
            HttpResponse response = restClient.get("/_searchguard/authinfo");

            Assert.assertEquals(response.getBody(), 401, response.getStatusCode());
        }

    }

    @Ignore("unsuitable for normal CI due to their run time")
    @Test
    public void multi() throws Exception {
        int sessions = 20;
        Duration testDuration = Duration.ofMinutes(30);
        long endTime = System.currentTimeMillis() + testDuration.toMillis();

        Map<String, Instant> sessionTokenToTimeoutMap = new HashMap<>();
        TreeMap<Long, String> scheduledAccess = new TreeMap<>();

        while (System.currentTimeMillis() < endTime) {

            while (sessionTokenToTimeoutMap.size() < sessions) {
                try (GenericRestClient client = cluster.getRandomClientNode().getRestClient()) {
                    HttpResponse response = client.postJson("/_searchguard/auth/session", basicAuthRequest(BASIC_USER));

                    Assert.assertEquals(response.getBody(), 201, response.getStatusCode());

                    String token = response.getBodyAsDocNode().getAsString("token");
                    Instant timeout = Instant.now().plus(TIMEOUT);
                    log.info("### Created new session " + getSessionId(token) + " :\n" + response.getBody());

                    sessionTokenToTimeoutMap.put(token, timeout);
                    scheduleRandomAccess(token, timeout, scheduledAccess);

                    Thread.sleep(cluster.getRandom().nextInt((int) 1000) + 10);
                }
            }

            Map.Entry<Long, String> nextAccess = scheduledAccess.firstEntry();
            scheduledAccess.remove(nextAccess.getKey());

            long sleepTime = nextAccess.getKey() - System.currentTimeMillis();

            log.debug("Sleeping " + sleepTime + " ms");

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }

            String sessionToken = nextAccess.getValue();
            Instant sessionTimeout = sessionTokenToTimeoutMap.get(sessionToken);

            try (GenericRestClient restClient = cluster.getRandomClientNode().getRestClient(new BearerAuthorization(sessionToken))) {
                HttpResponse response = restClient.get("/_searchguard/authinfo");

                if (response.getStatusCode() == 200) {
                    log.info("___ Session used: " + getSessionId(sessionToken) + " " + sessionTimeout);
                } else {
                    log.warn("@@@ Session expired: " + getSessionId(sessionToken) + " " + sessionTimeout);
                }

                if (sessionTimeout.isBefore(Instant.ofEpochMilli(System.currentTimeMillis() - 1000))) {
                    // This should be timed out

                    Assert.assertEquals(response.getBody(), 401, response.getStatusCode());

                    sessionTokenToTimeoutMap.remove(sessionToken);
                } else if (sessionTimeout.isAfter(Instant.ofEpochMilli(System.currentTimeMillis() + 1000))) {
                    // This should be active

                    Assert.assertEquals("Session was expired " + Instant.now() + " while it was expected to expire at " + sessionTimeout, 200,
                            response.getStatusCode());
                    Assert.assertEquals(response.getBody(), BASIC_USER.getName(), response.getBodyAsDocNode().getAsString("user_name"));
                    sessionTimeout = Instant.now().plus(TIMEOUT);
                    sessionTokenToTimeoutMap.put(sessionToken, sessionTimeout);
                    scheduleRandomAccess(sessionToken, sessionTimeout, scheduledAccess);
                } else {
                    // Grey area, be a bit tolerant here

                    if (sessionTimeout.isAfter(Instant.now())) {
                        log.info("Access briefly before timeout: " + response.getStatusCode());
                    } else {
                        log.info("Access briefly after timeout: " + response.getStatusCode());
                    }

                    sessionTokenToTimeoutMap.remove(sessionToken);
                }

            }

        }

    }

    private static Map<String, Object> basicAuthRequest(TestSgConfig.User user, Object... additionalAttrs) {
        Map<String, Object> result = new HashMap<>();

        result.put("mode", "basic");
        result.put("user", user.getName());
        result.put("password", user.getPassword());

        if (additionalAttrs != null && additionalAttrs.length > 0) {
            for (int i = 0; i < additionalAttrs.length; i += 2) {
                result.put(additionalAttrs[i].toString(), additionalAttrs[i + 1]);
            }
        }

        return result;
    }

    private void scheduleRandomAccess(String session, Instant timeout, SortedMap<Long, String> scheduledAccess) {
        if (cluster.getRandom().nextFloat() < 0.2) {
            // Let the session run into a timeout before doing the access

            long nextAccess = timeout.toEpochMilli() + cluster.getRandom().nextInt(5000) + 1000;

            while (scheduledAccess.containsKey(nextAccess)) {
                nextAccess++;
            }

            scheduledAccess.put(nextAccess, session);
        } else {
            // Do the access before the timeout

            long timeSpan = timeout.toEpochMilli() - 1000 - System.currentTimeMillis();

            if (timeSpan <= 0) {
                timeSpan = 100;
            }

            long nextAccess = Math.round(cluster.getRandom().nextDouble() * (double) timeSpan) + System.currentTimeMillis();

            while (scheduledAccess.containsKey(nextAccess)) {
                nextAccess++;
            }

            scheduledAccess.put(nextAccess, session);
        }
    }

    private String getSessionId(String sessionToken) throws DocumentParseException, UnexpectedDocumentStructureException {
        String[] parts = sessionToken.split("\\.");
        String payloadBase64 = parts[1];
        Map<String, Object> payload = DocReader.json().readObject(BaseEncoding.base64Url().decode(payloadBase64));

        return String.valueOf(payload.get("jti"));
    }
}
