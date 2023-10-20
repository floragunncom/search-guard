/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.HttpStatus;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DFMFieldMaskedEnabledTest extends BaseDFMFieldMaskedTest {

    @ClassRule
    public static LocalCluster cluster = clusterWithDfmEmptyOverridesAll(true);

    @BeforeClass
    public static void beforeClass() {
        TEST_DATA_PRODUCER.accept(cluster);
    }

    @Test
    public void testMaskedSearch() throws Exception {
        try (GenericRestClient adminClient = cluster.getRestClient(ADMIN);
             GenericRestClient dfmUserClient = cluster.getRestClient(DFM_USER)){
            GenericRestClient.HttpResponse adminResponse = adminClient.get("/deals-*/_search?pretty");
            assertThat(adminResponse.getBody(), adminResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
            //admin user can see all fields
            assertThat(adminResponse.getBody(), containsString("123.123.4.2"));
            assertThat(adminResponse.getBody(), containsString("123.123.5.2"));
            assertThat(adminResponse.getBody(), containsString("123.123.6.2"));
            assertThat(adminResponse.getBody(), containsString("123.123.1.1"));
            assertThat(adminResponse.getBody(), containsString("123.123.2.2"));
            assertThat(adminResponse.getBody(), containsString("123.123.3.2"));


            // user has a restricted role where the ip_dest fields are masked on indices matching deals-*. In addition, user has
            // another unrestricted role where the index pattern matches deals-outdated-*. The second role should
            // remove the DMF restrictions only from indices matching deals-outdated-*. Otherwise, restrictions still apply.
            GenericRestClient.HttpResponse dfmUserResponse = dfmUserClient.get("/deals-outdated-*/_search?pretty");
            assertThat(dfmUserResponse.getBody(), dfmUserResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(dfmUserResponse.getBody(), containsString("123.123.4.2"));
            assertThat(dfmUserResponse.getBody(), containsString("123.123.5.2"));
            assertThat(dfmUserResponse.getBody(), containsString("123.123.6.2"));

            dfmUserResponse = dfmUserClient.get("/deals-*/_search?pretty");
            assertThat(dfmUserResponse.getBody(), dfmUserResponse.getStatusCode(), equalTo(HttpStatus.SC_OK));
            assertThat(dfmUserResponse.getBody(), containsString("123.123.4.2"));
            assertThat(dfmUserResponse.getBody(), containsString("123.123.5.2"));
            assertThat(dfmUserResponse.getBody(), containsString("123.123.6.2"));
            assertThat(dfmUserResponse.getBody(), not(containsString("123.123.1.1")));
            assertThat(dfmUserResponse.getBody(), not(containsString("123.123.2.2")));
            assertThat(dfmUserResponse.getBody(), not(containsString("123.123.3.2")));
        }
    }

    @Test
    public void testDFMUnrestrictedUser() throws Exception {
        // admin user sees all

        try (GenericRestClient adminClient = cluster.getRestClient(ADMIN)){
            GenericRestClient.HttpResponse response = adminClient.get("/index1-*/_search?pretty");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            // the only document in index1-1 is filtered by DLS query, so normally no hit in index-1-1
            assertThat(response.getBody(), response.getBody(), containsString("index1-1"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-2"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-3"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-4"));

            // field3 and field4 - normally filtered out by FLS
            assertThat(response.getBody(), response.getBody(), containsString("value-3-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-3-2"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-2"));
            assertThat(response.getBody(), response.getBody(), containsString("value-3-3"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-3"));
            assertThat(response.getBody(), response.getBody(), containsString("value-3-4"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-4"));

            // field2 - normally masked
            assertThat(response.getBody(), response.getBody(), containsString("value-2-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-2-2"));
            assertThat(response.getBody(), response.getBody(), containsString("value-2-3"));
            assertThat(response.getBody(), response.getBody(), containsString("value-2-4"));
        }
    }


    @Test
    public void testDFMRestrictedUser() throws Exception {
        // tests that the DFM settings are applied. User has only one role
        // with D/F/M all enabled, so restrictions must kick in

        try (GenericRestClient dfmRestrictedClient = cluster.getRestClient(DFM_RESTRICTED_ROLE)) {
            GenericRestClient.HttpResponse response = dfmRestrictedClient.get("/_searchguard/authinfo?pretty");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = dfmRestrictedClient.get("/index1-*/_search?pretty");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            // the only document in index1-1 is filtered by DLS query, so no hit in index-1-1
            assertThat(response.getBody(), response.getBody(), not(containsString("index1-1")));

            // contains hits from other indices
            assertThat(response.getBody(), response.getBody(), containsString("index1-2"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-3"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-4"));

            // field3 and field4 - filtered out by FLS
            assertThat(response.getBody(), response.getBody(), not(containsString("value-3-1")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-4-1")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-3-2")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-4-2")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-3-3")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-4-3")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-3-4")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-4-4")));

            // field2 - normally masked
            assertThat(response.getBody(), response.getBody(), not(containsString("value-2-1")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-2-2")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-2-3")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-2-4")));

            // field2 - check also masked values
            assertThat(response.getBody(), response.getBody(), containsString("20b780487cb66801b924502fa76fd3028221aea111b923136a3d65656a7c4b50"));
            assertThat(response.getBody(), response.getBody(), containsString("3cf4028185562c7fba0decfa7d917b37409e415fdc730dc5724962057ad7ad88"));
            assertThat(response.getBody(), response.getBody(), containsString("666c69a8ef129003c8b1c7f875d112833c792afb29d17931f272a366d3ea87fc"));
        }
    }

    @Test
    public void testDFMRestrictedAndUnrestrictedAllIndices() throws Exception {

        // user has the restricted role as in test testDFMRestrictedUser(). In addition, user has
        // another role with the same index pattern as the restricted role but no DFM settings. In that
        // case the unrestricted role should trump the restricted one, so basically user has
        // full access again.

        try (GenericRestClient client = cluster.getRestClient(DFM_RESTRICTED_AND_UNRESTRICTED_ALL_INDICES_ROLE)) {
            GenericRestClient.HttpResponse response = client.get("/index1-*/_search?pretty");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            // the only document in index1-1 is filtered by DLS query, so normally no hit in index-1-1
            assertThat(response.getBody(), response.getBody(), containsString("index1-1"));

            // contains hits from other indices
            assertThat(response.getBody(), response.getBody(), containsString("index1-2"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-3"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-4"));

            // field3 and field4 - normally filtered out by FLS
            assertThat(response.getBody(), response.getBody(), containsString("value-3-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-3-2"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-2"));
            assertThat(response.getBody(), response.getBody(), containsString("value-3-3"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-3"));
            assertThat(response.getBody(), response.getBody(), containsString("value-3-4"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-4"));

            // field2 - normally masked
            assertThat(response.getBody(), response.getBody(), containsString("value-2-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-2-2"));
            assertThat(response.getBody(), response.getBody(), containsString("value-2-3"));
            assertThat(response.getBody(), response.getBody(), containsString("value-2-4"));
        }
    }

    @Test
    public void testDFMRestrictedAndUnrestrictedOneIndex() throws Exception {

        // user has the restricted role as in test testDFMRestrictedUser(). In addition, user has
        // another role where the index pattern matches two specific indices ("index1-1", "index-1-4"), means this role has two indices
        // which are more specific than the index pattern in the restricted role ("index1-*"), So the second role should
        // remove the DMF restrictions from exactly two indices. Otherwise, restrictions still apply.

        try (GenericRestClient client = cluster.getRestClient(DFM_RESTRICTED_AND_UNRESTRICTED_TWO_INDICES_ROLE)) {
            GenericRestClient.HttpResponse response = client.get("/_searchguard/authinfo?pretty");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            response = client.get("/index1-*/_search?pretty");
            assertThat(response.getBody(), response.getStatusCode(), equalTo(HttpStatus.SC_OK));

            // we have a role that places no restrictions on index-1-1, lifting the DLS from the restricted role
            // So we expect one unrestricted hit in this index
            assertThat(response.getBody(), response.getBody(), containsString("index1-1"));

            // contains hits from other indices
            assertThat(response.getBody(), response.getBody(), containsString("index1-2"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-3"));
            assertThat(response.getBody(), response.getBody(), containsString("index1-4"));

            // field3 and field4 - normally filtered out by FLS. Secondary role
            // lifts restrictions for index1-1 and index1-4, so only those
            // values should be visible for index1-1 and index1-4
            assertThat(response.getBody(), response.getBody(), containsString("value-3-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-3-4"));
            assertThat(response.getBody(), response.getBody(), containsString("value-4-4"));

            // FLS restrictions still in place for index1-2 and index1-3, those
            // fields must not be present
            assertThat(response.getBody(), response.getBody(), not(containsString("value-3-2")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-4-2")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-3-3")));
            assertThat(response.getBody(), response.getBody(), not(containsString("value-4-3")));

            // field2 - normally masked, but for index1-1 and index1-4 restrictions are
            // lifted by secondary role, so we have cleartext in index1-1 and index1-4
            assertThat(response.getBody(), response.getBody(), containsString("value-2-1"));
            assertThat(response.getBody(), response.getBody(), containsString("value-2-4"));

            // but we still have masked values for index1-2 and index1-3, check
            // for actual masked values
            assertThat(response.getBody(), response.getBody(), containsString("20b780487cb66801b924502fa76fd3028221aea111b923136a3d65656a7c4b50"));
            assertThat(response.getBody(), response.getBody(), containsString("3cf4028185562c7fba0decfa7d917b37409e415fdc730dc5724962057ad7ad88"));
        }
    }
}
