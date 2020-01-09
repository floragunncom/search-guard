/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard;

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class SearchGuardRolesTests extends SingleClusterTest {

    @Test
    public void testSGRAnon() throws Exception {

        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml")
                .setSgConfig("sg_config_anon.yml"), Settings.EMPTY, true);

        RestHelper rh = nonSslRestHelper();

        HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo?pretty");
        Assert.assertTrue(resc.getBody().contains("sg_anonymous"));
        Assert.assertFalse(resc.getBody().contains("xyz_sgr"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());

        resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sgr_user", "nagilum"));
        Assert.assertTrue(resc.getBody().contains("sgr_user"));
        Assert.assertTrue(resc.getBody().contains("xyz_sgr"));
        Assert.assertTrue(resc.getBody().contains("backend_roles=[abc_ber]"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
    }
    
    @Test
    public void testSGR() throws Exception {

        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml"), Settings.EMPTY, true);

        RestHelper rh = nonSslRestHelper();

        HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sgr_user", "nagilum"));
        Assert.assertTrue(resc.getBody().contains("sgr_user"));
        Assert.assertTrue(resc.getBody().contains("xyz_sgr"));
        Assert.assertTrue(resc.getBody().contains("backend_roles=[abc_ber]"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
    }
    
    @Test
    public void testSGRImpersonation() throws Exception {

        Settings settings = Settings.builder()
                .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS+".sgr_user", "sgr_impuser")
                .build();
        
        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml"), settings, true);

        RestHelper rh = nonSslRestHelper();

        HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sgr_user", "nagilum"), new BasicHeader("sg_impersonate_as", "sgr_impuser"));
        Assert.assertFalse(resc.getBody().contains("sgr_user"));
        Assert.assertTrue(resc.getBody().contains("sgr_impuser"));
        Assert.assertFalse(resc.getBody().contains("xyz_sgr"));
        Assert.assertTrue(resc.getBody().contains("xyz_impsgr"));
        Assert.assertTrue(resc.getBody().contains("backend_roles=[ert_ber]"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        
        resc = rh.executeGetRequest("*/_search?pretty", encodeBasicHeader("sgr_user", "nagilum"), new BasicHeader("sg_impersonate_as", "sgr_impuser"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
    }
}
