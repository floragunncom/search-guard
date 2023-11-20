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

import java.util.Collections;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator.ErrorType;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

public class RolesApiTest {

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin")
            .resources("restapi").sslEnabled().enterpriseModulesEnabled().build();

    @Test
    public void testPutRole() throws Exception {

        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {
            // check roles exists
            HttpResponse response = adminClient.putJson("_searchguard/api/roles/admin", FileHelper.loadFile("restapi/simple_role.json"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = adminClient.putJson("_searchguard/api/roles/lala", "{ \"cluster_permissions\": [\"*\"] }");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            response = adminClient.putJson("_searchguard/api/roles/empty", "{ \"cluster_permissions\": [] }");
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            //to test validation
            response = adminClient.putJson("_searchguard/api/roles/role_with_aliases_and_data_stream", FileHelper.loadFile("restapi/simple_role_with_empty_aliases_and_data_streams.json"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());
        }
    }

    @Test
    public void testAllRolesNotContainMetaHeader() throws Exception {

        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = adminClient.get("_searchguard/api/roles");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertFalse(response.getBody(), response.getBody().contains("_sg_meta"));
        }
    }

    @Test
    public void testPutDuplicateKeys() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = adminClient.putJson("_searchguard/api/roles/dup",
                    "{ \"cluster_permissions\": [\"*\"], \"cluster_permissions\": [\"*\"] }");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("is defined more than once"));
        }
    }

    @Test
    public void testPutUnknownKey() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = adminClient.putJson("_searchguard/api/roles/dup",
                    "{ \"unknownkey\": [\"*\"], \"cluster_permissions\": [\"*\"] }");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("invalid_keys"));
        }
    }

    @Test
    public void testPutInvalidJson() throws Exception {

        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = adminClient.putJson("_searchguard/api/roles/dup",
                    "{ \"invalid\"::{{ [\"*\"], \"cluster_permissions\": [\"*\"] }");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Invalid JSON document"));
        }
    }

    @Test
    public void testRolesApi() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {

            // check roles exists
            HttpResponse response = adminClient.get("_searchguard/api/roles");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // -- GET

            // GET sg_role_starfleet
            response = adminClient.get("/_searchguard/api/roles/sg_role_starfleet");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            DocNode settings = response.getBodyAsDocNode();
            Assert.assertEquals(1, settings.size());

            // GET, role does not exist
            response = adminClient.get("/_searchguard/api/roles/nothinghthere");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // GET, new URL endpoint in SG6
            response = adminClient.get("/_searchguard/api/roles/");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // GET, new URL endpoint in SG6
            response = adminClient.get("/_searchguard/api/roles");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertTrue(response.getBody().contains("\"cluster_permissions\":[\"*\"]"));
            Assert.assertFalse(response.getBody().contains("\"cluster_permissions\" : ["));

            // GET, new URL endpoint in SG6, pretty
            response = adminClient.get("/_searchguard/api/roles?pretty");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertFalse(response.getBody().contains("\"cluster_permissions\":[\"*\"]"));
            Assert.assertTrue(response.getBody().contains("\"cluster_permissions\" : ["));

            // hidden role
            response = adminClient.get("/_searchguard/api/roles/sg_internal");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // create index
            setupStarfleetIndex();

            // add user picard, role starfleet, maps to sg_role_starfleet
            addUserWithPassword("picard", "picard", new String[] { "starfleet", "captains" }, HttpStatus.SC_CREATED);
            checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);
            checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);

            // -- DELETE

            // Non-existing role
            response = adminClient.delete("/_searchguard/api/roles/idonotexist");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // read only role
            response = adminClient.delete("/_searchguard/api/roles/sg_transport_client");
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // hidden role
            response = adminClient.delete("/_searchguard/api/roles/sg_internal");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // remove complete role mapping for sg_role_starfleet_captains
            response = adminClient.delete("/_searchguard/api/roles/sg_role_starfleet_captains");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            // user has only role starfleet left, role has READ access only
            checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", 1);

            // ES7 only supports one doc type, but SG permission checks run first
            // So we also get a 403 FORBIDDEN when tring to add new document type
            checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", 0);

            // remove also starfleet role, nothing is allowed anymore
            response = adminClient.delete("/_searchguard/api/roles/sg_role_starfleet");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", 0);
            checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", 0);

            // -- PUT
            // put with empty roles, must fail
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet", "");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.PAYLOAD_MANDATORY.getMessage(), settings.getAsString("reason"));

            // put new configuration with invalid payload, must fail
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet", FileHelper.loadFile("restapi/roles_not_parseable.json"));
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.BODY_NOT_PARSEABLE.getMessage(), settings.getAsString("reason"));

            // put new configuration with invalid keys, must fail
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet", FileHelper.loadFile("restapi/roles_invalid_keys.json"));
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.INVALID_CONFIGURATION.getMessage(), settings.getAsString("reason"));
            Assert.assertTrue(settings.getAsNode(AbstractConfigurationValidator.INVALID_KEYS_KEY).getAsString("keys").contains("indexx_permissions"));
            Assert.assertTrue(settings.getAsNode(AbstractConfigurationValidator.INVALID_KEYS_KEY).getAsString("keys").contains("kluster_permissions"));

            // put new configuration with wrong datatypes, must fail
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet", FileHelper.loadFile("restapi/roles_wrong_datatype.json"));
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.getAsString("reason"));
            Assert.assertTrue(settings.getAsString("cluster_permissions").equals("Array expected"));

            // put read only role, must be forbidden
            response = adminClient.putJson("/_searchguard/api/roles/sg_transport_client", FileHelper.loadFile("restapi/roles_captains.json"));
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // put hidden role, must be forbidden
            response = adminClient.putJson("/_searchguard/api/roles/sg_internal", FileHelper.loadFile("restapi/roles_captains.json"));
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // restore starfleet role
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet", FileHelper.loadFile("restapi/roles_starfleet.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
            checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);

            // now picard is only in sg_role_starfleet, which has write access to
            // all indices. We collapse all document types in SG7 so this permission in the
            // starfleet role grants all permissions:
            //   public:  
            //       - 'indices:*'		
            checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);
            // restore captains role
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains", FileHelper.loadFile("restapi/roles_captains.json"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
            checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);
            checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", 0);

            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains",
                    FileHelper.loadFile("restapi/roles_complete_invalid.json"));
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            //		rh.sendHTTPClientCertificate = true;
            //		response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains",
            //				FileHelper.loadFile("restapi/roles_multiple.json"));
            //		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains",
                    FileHelper.loadFile("restapi/roles_multiple_2.json"));
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            // check tenants
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains",
                    FileHelper.loadFile("restapi/roles_captains_tenants.json"));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(2, settings.size());
            Assert.assertEquals(settings.get("status"), "OK");

            response = adminClient.get("/_searchguard/api/roles/sg_role_starfleet_captains");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(1, settings.size());
            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(1)
                    .getAsListOfNodes("tenant_patterns").get(0).toString(), "tenant1");
            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(1)
                    .getAsListOfNodes("allowed_actions").get(0).toString(), "SGS_KIBANA_ALL_READ");

            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(0)
                    .getAsListOfNodes("tenant_patterns").get(0).toString(), "tenant2");
            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(0)
                    .getAsListOfNodes("allowed_actions").get(0).toString(), "SGS_KIBANA_ALL_WRITE");

            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains",
                    FileHelper.loadFile("restapi/roles_captains_tenants2.json"));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(2, settings.size());
            Assert.assertEquals(settings.get("status"), "OK");

            response = adminClient.get("/_searchguard/api/roles/sg_role_starfleet_captains");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(1, settings.size());

            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(0)
                    .getAsListOfNodes("tenant_patterns").get(0).toString(), "tenant2");
            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(0)
                    .getAsListOfNodes("tenant_patterns").get(1).toString(), "tenant4");

            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(0)
                    .getAsListOfNodes("allowed_actions").get(0).toString(), "SGS_KIBANA_ALL_WRITE");

            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(1)
                    .getAsListOfNodes("tenant_patterns").get(0).toString(), "tenant1");
            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(1)
                    .getAsListOfNodes("tenant_patterns").get(1).toString(), "tenant3");
            Assert.assertEquals(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").get(1)
                    .getAsListOfNodes("allowed_actions").get(0).toString(), "SGS_KIBANA_ALL_READ");
            
            // remove tenants from role
            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains",
                    FileHelper.loadFile("restapi/roles_captains_no_tenants.json"));
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(2, settings.size());
            Assert.assertEquals(settings.get("status"), "OK");


            response = adminClient.get("/_searchguard/api/roles/sg_role_starfleet_captains");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(1, settings.size());
            Assert.assertFalse(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("cluster_permissions").get(0).isNull());
            Assert.assertTrue(settings.getAsNode("sg_role_starfleet_captains").getAsListOfNodes("tenant_permissions").isEmpty());

            response = adminClient.putJson("/_searchguard/api/roles/sg_role_starfleet_captains",
                    FileHelper.loadFile("restapi/roles_captains_tenants_malformed.json"));
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            Assert.assertEquals(settings.get("status"), "error");
            Assert.assertEquals(settings.get("reason"), ErrorType.INVALID_CONFIGURATION.getMessage());


            // -- PATCH
            // PATCH on non-existing resource
            response = adminClient.patch("/_searchguard/api/roles/imnothere",
                    "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // PATCH read only resource, must be forbidden
            response = adminClient.patch("/_searchguard/api/roles/sg_transport_client",
                    "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // PATCH hidden resource, must be not found
            response = adminClient.patch("/_searchguard/api/roles/sg_internal",
                    "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // PATCH value of hidden flag, must fail with validation error
            response = adminClient.patch("/_searchguard/api/roles/sg_role_starfleet",
                    "[{ \"op\": \"add\", \"path\": \"/hidden\", \"value\": true }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(),
                    response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

            List<String> permissions = null;

            // PATCH 
            /*
             * how to patch with new v7 config format?
             * rh.sendHTTPClientCertificate = true;
            response = adminClient.patch("/_searchguard/api/roles/sg_role_starfleet", "[{ \"op\": \"add\", \"path\": \"/index_permissions/sf/ships/-\", \"value\": \"SEARCH\" }]");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            response = adminClient.get("/_searchguard/api/roles/sg_role_starfleet");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = DefaultObjectMapper.readTree(response.getBody());       
            permissions = DefaultObjectMapper.objectMapper.convertValue(settings.get("sg_role_starfleet").get("indices").get("sf").get("ships"), List.class);
            Assert.assertNotNull(permissions);
            Assert.assertEquals(2, permissions.size());
            Assert.assertTrue(permissions.contains("READ"));
            Assert.assertTrue(permissions.contains("SEARCH")); */

            // -- PATCH on whole config resource
            // PATCH on non-existing resource
            response = adminClient.patch("/_searchguard/api/roles",
                    "[{ \"op\": \"add\", \"path\": \"/imnothere/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            // PATCH read only resource, must be forbidden
            response = adminClient.patch("/_searchguard/api/roles",
                    "[{ \"op\": \"add\", \"path\": \"/sg_transport_client/a\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // PATCH hidden resource, must be bad request
            response = adminClient.patch("/_searchguard/api/roles",
                    "[{ \"op\": \"add\", \"path\": \"/sg_internal/a\", \"value\": [ \"foo\", \"bar\" ] }]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            // PATCH delete read only resource, must be forbidden
            response = adminClient.patch("/_searchguard/api/roles", "[{ \"op\": \"remove\", \"path\": \"/sg_transport_client\" }]");
            Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

            // PATCH hidden resource, must be bad request
            response = adminClient.patch("/_searchguard/api/roles", "[{ \"op\": \"remove\", \"path\": \"/sg_internal\"}]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

            // PATCH value of hidden flag, must fail with validation error
            response = adminClient.patch("/_searchguard/api/roles",
                    "[{ \"op\": \"add\", \"path\": \"/newnewnew\", \"value\": {  \"hidden\": true, \"index_permissions\" : [ {\"index_patterns\" : [ \"sf\" ],\"allowed_actions\" : [ \"READ\" ]}] }}]");
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

            // PATCH 
            response = adminClient.patch("/_searchguard/api/roles",
                    "[{ \"op\": \"add\", \"path\": \"/bulknew1\", \"value\": {   \"index_permissions\" : [ {\"index_patterns\" : [ \"sf\" ],\"allowed_actions\" : [ \"READ\" ]}] }}]");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            response = adminClient.get("/_searchguard/api/roles/bulknew1");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            settings = response.getBodyAsDocNode();
            permissions = settings.getAsNode("bulknew1").getAsListOfNodes("index_permissions").get(0).getAsListOfStrings("allowed_actions");
            Assert.assertNotNull(permissions);
            Assert.assertEquals(1, permissions.size());
            Assert.assertTrue(permissions.contains("READ"));

            // delete resource
            response = adminClient.patch("/_searchguard/api/roles", "[{ \"op\": \"remove\", \"path\": \"/bulknew1\"}]");
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
            response = adminClient.get("/_searchguard/api/roles/bulknew1");
            Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

            // put valid field masks
            response = adminClient.putJson("/_searchguard/api/roles/sg_field_mask_valid",
                    FileHelper.loadFile("restapi/roles_field_masks_valid.json"));
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            // put invalid field masks
            response = adminClient.putJson("/_searchguard/api/roles/sg_field_mask_invalid",
                    FileHelper.loadFile("restapi/roles_field_masks_invalid.json"));
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        }

    }

    @Test
    public void putRole_roleWhichAssignsPermsToNoExistentTenantsShouldBeRejected() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {

            String tenantName = "missing1";
            String roleName = "put_role_with_pattern_matching_no_tenant";
            DocNode role = DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", Collections.singletonList(tenantName + "*"))));

            HttpResponse response = adminClient.putJson("_searchguard/api/roles/" + roleName, role);
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Tenant pattern: '" + tenantName + "*' does not match any tenant"));

            //add tenant
            response = adminClient.putJson("/_searchguard/api/tenants/" + tenantName, DocNode.of("description", "tenant"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

            response = adminClient.putJson("_searchguard/api/roles/" + roleName, role);
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        }
    }

    @Test
    public void patchRole_roleWhichAssignsPermsToNoExistentTenantsShouldBeRejected() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient().trackResources()) {

            String tenantName = "missing1";
            String roleName = "patch_role_with_pattern_matching_no_tenant";

            DocNode role = DocNode.of("cluster_permissions", Collections.singletonList("MONITOR"));
            HttpResponse response = adminClient.putJson("/_searchguard/api/roles/" + roleName, role);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            //single patch
            DocNode singlePatch = DocNode.array(
                    DocNode.of("op", "add", "path", "/tenant_permissions",
                            "value", DocNode.array(DocNode.of("tenant_patterns", Collections.singletonList(tenantName + "*")))
                    )
            );

            response = adminClient.patch("/_searchguard/api/roles/" + roleName, singlePatch.toJsonString());
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Tenant pattern: '" + tenantName + "*' does not match any tenant"));

            //patch
            DocNode patch = DocNode.array(
                    DocNode.of("op", "add", "path", "/" + roleName + "2",
                            "value", DocNode.of("tenant_permissions", DocNode.array(DocNode.of("tenant_patterns", Collections.singletonList(tenantName + "*"))))
                    )
            );

            response = adminClient.patch("/_searchguard/api/roles/", patch.toJsonString());
            Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
            Assert.assertTrue(response.getBody(), response.getBody().contains("Tenant pattern: '" + tenantName + "*' does not match any tenant"));

            //add tenant
            response = adminClient.putJson("/_searchguard/api/tenants/" + tenantName, DocNode.of("description", "tenant"));
            Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

            //single patch
            response = adminClient.patch("/_searchguard/api/roles/" + roleName, singlePatch.toJsonString());
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

            //patch
            response = adminClient.patch("/_searchguard/api/roles/", patch.toJsonString());
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        }
    }

    protected void setupStarfleetIndex() throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {

            adminClient.put("sf");
            adminClient.putJson("sf/_doc/0", "{\"number\" : \"NCC-1701-D\"}");
            adminClient.putJson("sf/_doc/0", "{\"some\" : \"value\"}");
        }
    }

    protected void addUserWithPassword(String username, String password, int status) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
            HttpResponse response = adminClient.putJson("/_searchguard/api/internalusers/" + username, "{\"password\": \"" + password + "\"}",
                    new Header[0]);
            Assert.assertEquals(status, response.getStatusCode());
        }
    }

    protected void addUserWithPassword(String username, String password, String[] roles, int status) throws Exception {
        try (GenericRestClient adminClient = cluster.getAdminCertRestClient()) {
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
    }

    protected String checkReadAccess(int status, String username, String password, String indexName, int id) throws Exception {
        try (GenericRestClient client = cluster.getRestClient(username, password)) {
            String action = indexName + "/_doc/" + id;
            HttpResponse response = client.get(action);
            int returnedStatus = response.getStatusCode();
            Assert.assertEquals(status, returnedStatus);
            return response.getBody();
        }
    }

    protected String checkWriteAccess(int status, String username, String password, String indexName, int id) throws Exception {

        try (GenericRestClient client = cluster.getRestClient(username, password)) {
            String action = indexName + "/_doc/" + id;
            String payload = "{\"value\" : \"true\"}";
            HttpResponse response = client.putJson(action, payload);
            int returnedStatus = response.getStatusCode();
            Assert.assertEquals(status, returnedStatus);
            return response.getBody();
        }
    }

}
