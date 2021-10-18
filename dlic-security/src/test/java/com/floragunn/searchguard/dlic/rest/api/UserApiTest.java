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

package com.floragunn.searchguard.dlic.rest.api;

import java.net.URLEncoder;
import java.util.List;

import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;

public class UserApiTest {

    public static TestCertificates certificatesContext = TestCertificates.builder()
            .ca("CN=root.ca.example.com,OU=SearchGuard,O=SearchGuard")
            .addNodes("CN=node-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addClients("CN=client-0.example.com,OU=SearchGuard,O=SearchGuard")
            .addAdminClients("CN=admin-0.example.com,OU=SearchGuard,O=SearchGuard")
            .build();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin")
            .resources("restapi").sslEnabled(certificatesContext).build();

    @Test
    public void testSearchGuardRoles() throws Exception {

        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {

            // initial configuration, 5 users
            HttpResponse response = adminClient.get("_searchguard/api/" + CType.INTERNALUSERS.toLCString());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertTrue(response.getBody(), settings.size() >= 35);

            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/newuser\", \"value\": {\"password\": \"newuser\", \"search_guard_roles\": [\"sg_all_access\"] } }]");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

            response = adminClient.get("/_searchguard/api/internalusers/newuser");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("\"search_guard_roles\":[\"sg_all_access\"]"));

            checkGeneralAccess(HttpStatus.SC_OK, "newuser", "newuser");
        }
    }

    @Test
    public void testUserApi() throws Exception {

        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {

            // initial configuration, 5 users
            HttpResponse response = adminClient.get("_searchguard/api/" + CType.INTERNALUSERS.toLCString());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertTrue(response.getBody(), settings.size() >= 35);

            // --- GET

            // GET, user admin, exists
            response = adminClient.get("/_searchguard/api/internalusers/admin");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            System.out.println(response.getBody());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(7, settings.size());
            // hash must be filtered
            Assert.assertEquals("", settings.get("admin.hash"));

            // GET, user does not exist
            response = adminClient.get("/_searchguard/api/internalusers/nothinghthere");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // GET, new URL endpoint in SG6
            response = adminClient.get("/_searchguard/api/internalusers/");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // GET, new URL endpoint in SG6
            response = adminClient.get("/_searchguard/api/internalusers");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // -- PUT

            // no username given
            response = adminClient.putJson("/_searchguard/api/internalusers/", "{\"hash\": \"123\"}");
            Assert.assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, response.getStatusCode());

            // Faulty JSON payload
            response = adminClient.putJson("/_searchguard/api/internalusers/nagilum", "{some: \"thing\" asd  other: \"thing\"}");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(settings.get("reason"), AbstractConfigurationValidator.ErrorType.BODY_NOT_PARSEABLE.getMessage());

            // Missing quotes in JSON - parseable in 6.x, but wrong config keys
            response = adminClient.putJson("/_searchguard/api/internalusers/nagilum", "{some: \"thing\", other: \"thing\"}");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            //JK: this should be "Could not parse content of request." because JSON is truly invalid
            //Assert.assertEquals(settings.get("reason"), AbstractConfigurationValidator.ErrorType.INVALID_CONFIGURATION.getMessage());
            //Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("some"));
            //Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("other"));

            // Wrong config keys
            response = adminClient.putJson("/_searchguard/api/internalusers/nagilum", "{\"some\": \"thing\", \"other\": \"thing\"}");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(settings.get("reason"), AbstractConfigurationValidator.ErrorType.INVALID_CONFIGURATION.getMessage());
            Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("some"));
            Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("other"));

            // -- PATCH
            // PATCH on non-existing resource
            response = adminClient.patch("/_searchguard/api/internalusers/imnothere",
                    "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // PATCH read only resource, must be forbidden
            response = adminClient.patch("/_searchguard/api/internalusers/sarek",
                    "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // PATCH hidden resource, must be not found
            response = adminClient.patch("/_searchguard/api/internalusers/q",
                    "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // PATCH value of hidden flag, must fail with validation error
            response = adminClient.patch("/_searchguard/api/internalusers/test", "[{ \"op\": \"add\", \"path\": \"/hidden\", \"value\": true }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

            // PATCH password
            response = adminClient.patch("/_searchguard/api/internalusers/test",
                    "[{ \"op\": \"add\", \"path\": \"/password\", \"value\": \"neu\" }]");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            response = adminClient.get("/_searchguard/api/internalusers/test");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertFalse(settings.hasValue("test.password"));
            Assert.assertTrue(settings.hasValue("test.hash"));

            // -- PATCH on whole config resource
            // PATCH on non-existing resource
            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/imnothere/a\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            // PATCH read only resource, must be forbidden
            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/sarek/a\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // PATCH hidden resource, must be bad request
            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/q/a\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            // PATCH value of hidden flag, must fail with validation error
            response = adminClient.patch("/_searchguard/api/internalusers", "[{ \"op\": \"add\", \"path\": \"/test/hidden\", \"value\": true }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

            // PATCH
            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/bulknew1\", \"value\": {\"password\": \"bla\", \"backend_roles\": [\"vulcan\"] } }]");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            response = adminClient.get("/_searchguard/api/internalusers/bulknew1");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertFalse(settings.hasValue("bulknew1.password"));
            Assert.assertTrue(settings.hasValue("bulknew1.hash"));
            List<String> roles = settings.getAsList("bulknew1.backend_roles");
            Assert.assertEquals(1, roles.size());
            Assert.assertTrue(roles.contains("vulcan"));

            // add user with correct setting. User is in role "sg_all_access"

            // check access not allowed
            checkGeneralAccess(HttpStatus.SC_UNAUTHORIZED, "nagilum", "nagilum");

            // add/update user, user is read only, forbidden
            addUserWithHash(adminClient, "sarek", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_FORBIDDEN);

            // add/update user, user is hidden, forbidden
            addUserWithHash(adminClient, "q", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_FORBIDDEN);

            // add users
            addUserWithHash(adminClient, "nagilum", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_CREATED);

            // access must be allowed now
            checkGeneralAccess(HttpStatus.SC_OK, "nagilum", "nagilum");

            // try remove user, no username
            response = adminClient.delete("/_searchguard/api/internalusers");
            Assert.assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, response.getStatusCode());

            // try remove user, nonexisting user
            response = adminClient.delete("/_searchguard/api/internalusers/picard");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // try remove readonly user
            response = adminClient.delete("/_searchguard/api/internalusers/sarek");
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // try remove hidden user
            response = adminClient.delete("/_searchguard/api/internalusers/q");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // now really remove user
            adminClient.delete("/_searchguard/api/internalusers/nagilum");

            // Access must be forbidden now
            checkGeneralAccess(HttpStatus.SC_UNAUTHORIZED, "nagilum", "nagilum");

            // use password instead of hash
            addUserWithPassword(adminClient, "nagilum", "correctpassword", HttpStatus.SC_CREATED);

            checkGeneralAccess(HttpStatus.SC_UNAUTHORIZED, "nagilum", "wrongpassword");
            checkGeneralAccess(HttpStatus.SC_OK, "nagilum", "correctpassword");

            adminClient.delete("/_searchguard/api/internalusers/nagilum");

            // Check unchanged password functionality

            // new user, password or hash is mandatory
            addUserWithoutPasswordOrHash(adminClient, "nagilum", new String[] { "starfleet" }, HttpStatus.SC_BAD_REQUEST);
            // new user, add hash
            addUserWithHash(adminClient, "nagilum", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_CREATED);
            // update user, do not specify hash or password, hash must remain the same
            addUserWithoutPasswordOrHash(adminClient, "nagilum", new String[] { "starfleet" }, HttpStatus.SC_OK);
            // get user, check hash, must be untouched
            response = adminClient.get("/_searchguard/api/internalusers/nagilum");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertTrue(settings.get("nagilum.hash").equals(""));

            // ROLES
            // create index first
            setupStarfleetIndex();

            // wrong datatypes in roles file
            response = adminClient.putJson("/_searchguard/api/internalusers/picard", FileHelper.loadFile("restapi/users_wrong_datatypes.json"));
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
            Assert.assertTrue(settings.get("backend_roles").equals("Array expected"));

            response = adminClient.putJson("/_searchguard/api/internalusers/picard", FileHelper.loadFile("restapi/users_wrong_datatypes.json"));
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
            Assert.assertTrue(settings.get("backend_roles").equals("Array expected"));

            response = adminClient.putJson("/_searchguard/api/internalusers/picard", FileHelper.loadFile("restapi/users_wrong_datatypes2.json"));
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
            Assert.assertTrue(settings.get("password").equals("String expected"));
            Assert.assertTrue(settings.get("backend_roles") == null);

            response = adminClient.putJson("/_searchguard/api/internalusers/picard", FileHelper.loadFile("restapi/users_wrong_datatypes3.json"));
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
            Assert.assertTrue(settings.get("backend_roles").equals("Array expected"));

            // use backendroles when creating user. User picard does not exist in
            // the internal user DB
            // and is also not assigned to any role by username
            addUserWithPassword(adminClient, "picard", "picard", HttpStatus.SC_CREATED);
            // changed in ES5, you now need cluster:monitor/main which pucard does not have
            checkGeneralAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard");

            // check read access to starfleet index and ships type, must fail
            checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);

            // overwrite user picard, and give him role "starfleet". This role has READ access only
            addUserWithPassword(adminClient, "picard", "picard", new String[] { "starfleet" }, HttpStatus.SC_OK);
            checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
            checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 1);

            // overwrite user picard, and give him role "starfleet" plus "captains. Now
            // document can be created.
            addUserWithPassword(adminClient, "picard", "picard", new String[] { "starfleet", "captains" }, HttpStatus.SC_OK);
            checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
            checkWriteAccess(HttpStatus.SC_CREATED, "picard", "picard", "sf", "ships", 1);

            response = adminClient.get("/_searchguard/api/internalusers/picard");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals("", settings.get("picard.hash"));
            roles = settings.getAsList("picard.backend_roles");
            Assert.assertNotNull(roles);
            Assert.assertEquals(2, roles.size());
            Assert.assertTrue(roles.contains("starfleet"));
            Assert.assertTrue(roles.contains("captains"));

            addUserWithPassword(adminClient, "$1aAAAAAAAAC", "$1aAAAAAAAAC", HttpStatus.SC_CREATED);
            addUserWithPassword(adminClient, "abc", "abc", HttpStatus.SC_CREATED);

            // check tabs in json
            response = adminClient.putJson("/_searchguard/api/internalusers/userwithtabs", "\t{\"hash\": \t \"123\"\t}  ");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());
        }
    }

    @Test
    public void testPasswordRules() throws Exception {

        try (LocalCluster cluster = new LocalCluster.Builder()
                .nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin",
                        ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_ERROR_MESSAGE, "xxx",
                        ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_REGEX, "(?=.*[A-Z])(?=.*[^a-zA-Z\\\\d])(?=.*[0-9])(?=.*[a-z]).{8,}")
                .resources("restapi").singleNode().sslEnabled(certificatesContext).build();
                GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {

            // initial configuration, 5 users
            HttpResponse response = adminClient.get("_searchguard/api/" + CType.INTERNALUSERS.toLCString());
            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertEquals(response.getBody(), 35, settings.size());

            addUserWithPassword(adminClient, "tooshoort", "123", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, "tooshoort", "1234567", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, "tooshoort", "1Aa%", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, "no-nonnumeric", "123456789", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, "no-uppercase", "a123456789", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, "no-lowercase", "A123456789", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, "ok1", "a%A123456789", HttpStatus.SC_CREATED);
            addUserWithPassword(adminClient, "ok2", "$aA123456789", HttpStatus.SC_CREATED);
            addUserWithPassword(adminClient, "ok3", "$Aa123456789", HttpStatus.SC_CREATED);
            addUserWithPassword(adminClient, "ok4", "$1aAAAAAAAAA", HttpStatus.SC_CREATED);

            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/ok4\", \"value\": {\"password\": \"bla\", \"backend_roles\": [\"vulcan\"] } }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"replace\", \"path\": \"/ok4\", \"value\": {\"password\": \"bla\", \"backend_roles\": [\"vulcan\"] } }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            addUserWithPassword(adminClient, "ok4", "123", HttpStatus.SC_BAD_REQUEST);

            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/ok4\", \"value\": {\"password\": \"$1aAAAAAAAAB\", \"backend_roles\": [\"vulcan\"] } }]");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            addUserWithPassword(adminClient, "ok4", "$1aAAAAAAAAC", HttpStatus.SC_OK);

            //its not allowed to use the username as password (case insensitive)
            response = adminClient.patch("/_searchguard/api/internalusers",
                    "[{ \"op\": \"add\", \"path\": \"/$1aAAAAAAAAB\", \"value\": {\"password\": \"$1aAAAAAAAAB\", \"backend_roles\": [\"vulcan\"] } }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            addUserWithPassword(adminClient, "$1aAAAAAAAAC", "$1aAAAAAAAAC", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, "$1aAAAAAAAac", "$1aAAAAAAAAC", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, URLEncoder.encode("$1aAAAAAAAac%", "UTF-8"), "$1aAAAAAAAAC%", HttpStatus.SC_BAD_REQUEST);
            //https://github.com/elastic/elasticsearch/pull/44324
            addUserWithPassword(adminClient, URLEncoder.encode("$1aAAAAAAAac%!=\"/\\;:test&~@^", "UTF-8").replace("+", "%2B"),
                    "$1aAAAAAAAac%!=\\\"/\\\\;:test&~@^", HttpStatus.SC_BAD_REQUEST);
            addUserWithPassword(adminClient, URLEncoder.encode("$1aAAAAAAAac%!=\"/\\;: test&", "UTF-8"), "$1aAAAAAAAac%!=\\\"/\\\\;: test&123",
                    HttpStatus.SC_CREATED);

            response = adminClient.get("/_searchguard/api/internalusers/nothinghthere?pretty");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("NOT_FOUND"));

            String patchPayload = "[ "
                    + "{ \"op\": \"add\", \"path\": \"/testuser1\",  \"value\": { \"password\": \"$aA123456789\", \"backend_roles\": [\"testrole1\"] } },"
                    + "{ \"op\": \"add\", \"path\": \"/testuser2\",  \"value\": { \"password\": \"testpassword2\", \"backend_roles\": [\"testrole2\"] } }"
                    + "]";

            response = adminClient.patch("/_searchguard/api/internalusers", patchPayload);
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("error"));
            Assert.assertTrue(response.getBody().contains("xxx"));
        }
    }

    @Test
    public void testUserApiWithDots() throws Exception {

        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {
            // initial configuration, 5 users
            HttpResponse response = adminClient.get("_searchguard/api/" + CType.INTERNALUSERS.toLCString());
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
            Assert.assertTrue(response.getBody(), settings.size() >= 35);

            addUserWithPassword(adminClient, ".my.dotuser0", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_CREATED);

            addUserWithPassword(adminClient, ".my.dot.user0", "12345678", HttpStatus.SC_CREATED);

            addUserWithHash(adminClient, ".my.dotuser1", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_CREATED);

            addUserWithPassword(adminClient, ".my.dot.user2", "12345678", HttpStatus.SC_CREATED);
        }
    }

    @Test
    public void testUserApiNoPasswordChange() throws Exception {

        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {
            // initial configuration, 5 users
            HttpResponse response;

            addUserWithHash(adminClient, "user1", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_CREATED);

            response = adminClient.putJson("/_searchguard/api/internalusers/user1",
                    "{\"hash\":\"$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m\",\"password\":\"\",\"backend_roles\":[\"admin\",\"rolea\"]}");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            response = adminClient.get("/_searchguard/api/internalusers/user1");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            addUserWithHash(adminClient, "user2", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_CREATED);

            response = adminClient.putJson("/_searchguard/api/internalusers/user2", "{\"password\":\"\",\"backend_roles\":[\"admin\",\"rolex\"]}");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            response = adminClient.get("/_searchguard/api/internalusers/user2");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    @Test
    public void testDontReturnSensitiveDataUponInvalidRequests() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {
            HttpResponse response;

            addUserWithHash(adminClient, "user1", "$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m", HttpStatus.SC_CREATED);

            response = adminClient.putJson("/_searchguard/api/internalusers/user1",
                    "{\"12312\"---\"$2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m\",\"password\":\"secret\",\"xyz\":[\"admina\",\"rolea\"]}");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            Assert.assertThat(response.getBody(), CoreMatchers.not(CoreMatchers.containsString("secret")));
            Assert.assertThat(response.getBody(), CoreMatchers.not(CoreMatchers.containsString("password")));
        }
    }

    protected void checkGeneralAccess(int status, String username, String password) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(username, password)) {

            Assert.assertEquals(status, client.get("").getStatusCode());
        }
    }

    protected void addUserWithPassword(GenericRestClient adminClient, String username, String password, int status) throws Exception {
        HttpResponse response = adminClient.putJson("/_searchguard/api/internalusers/" + username, "{\"password\": \"" + password + "\"}",
                new Header[0]);
        Assert.assertEquals(status, response.getStatusCode());
    }

    protected void addUserWithPassword(GenericRestClient adminClient, String username, String password, String[] roles, int status) throws Exception {
        String payload = "{" + "\"password\": \"" + password + "\"," + "\"backend_roles\": [";
        for (int i = 0; i < roles.length; i++) {
            payload += "\"" + roles[i] + "\"";
            if (i + 1 < roles.length) {
                payload += ",";
            }
        }
        payload += "]}";
        HttpResponse response = adminClient.putJson("/_searchguard/api/internalusers/" + username, payload);
        Assert.assertEquals(status, response.getStatusCode());
    }

    protected void addUserWithHash(GenericRestClient adminClient, String username, String hash) throws Exception {
        addUserWithHash(adminClient, username, hash, HttpStatus.SC_OK);
    }

    protected void addUserWithHash(GenericRestClient adminClient, String username, String hash, int status) throws Exception {
        HttpResponse response = adminClient.putJson("/_searchguard/api/internalusers/" + username, "{\"hash\": \"" + hash + "\"}", new Header[0]);
        Assert.assertEquals(status, response.getStatusCode());
    }

    protected void addUserWithoutPasswordOrHash(GenericRestClient adminClient, String username, String[] roles, int status) throws Exception {
        String payload = "{ \"backend_roles\": [";
        for (int i = 0; i < roles.length; i++) {
            payload += "\" " + roles[i] + " \"";
            if (i + 1 < roles.length) {
                payload += ",";
            }
        }
        payload += "]}";
        HttpResponse response = adminClient.putJson("/_searchguard/api/internalusers/" + username, payload, new Header[0]);
        Assert.assertEquals(status, response.getStatusCode());
    }

    protected void setupStarfleetIndex() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {

            adminClient.put("sf");
            adminClient.putJson("sf/ships/0", "{\"number\" : \"NCC-1701-D\"}");
            adminClient.putJson("sf/public/0", "{\"some\" : \"value\"}");
        }
    }

    protected String checkReadAccess(int status, String username, String password, String indexName, String type, int id) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(username, password)) {
            String action = indexName + "/" + type + "/" + id;
            HttpResponse response = client.get(action);
            int returnedStatus = response.getStatusCode();
            Assert.assertEquals(status, returnedStatus);
            return response.getBody();
        }
    }

    protected String checkWriteAccess(int status, String username, String password, String indexName, String type, int id) throws Exception {

        try (GenericRestClient client = cluster.getRestClient(username, password)) {
            String action = indexName + "/" + type + "/" + id;
            String payload = "{\"value\" : \"true\"}";
            HttpResponse response = client.putJson(action, payload);
            int returnedStatus = response.getStatusCode();
            Assert.assertEquals(status, returnedStatus);
            return response.getBody();
        }
    }

}
