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

package com.floragunn.searchguard.enterprise.femt;

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.opensearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

public class HttpIntegrationTests extends SingleClusterTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @Override
    protected String getResourceFolder() {
        return "multitenancy_legacy";
    }

    @Test
    public void testHTTPBasic() throws Exception {
        final Settings settings = Settings.builder()
                .putList(ConfigConstants.SEARCHGUARD_AUTHCZ_REST_IMPERSONATION_USERS + ".worf", "knuddel", "nonexists").build();
        setup(settings);
        RestHelper rh = nonSslRestHelper();

        HttpResponse res = rh.executeGetRequest("_searchguard/authinfo", new BasicHeader("sg_tenant", "unittesttenant"),
                encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("sg_tenants"));
        Assert.assertTrue(res.getBody().contains("unittesttenant"));
        Assert.assertTrue(res.getBody(), res.getBody().contains("\"kltentrw\":true"));
        Assert.assertTrue(res.getBody().contains("\"user_name\":\"worf\""));

        res = rh.executeGetRequest("_searchguard/authinfo", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("sg_tenants"));
        Assert.assertTrue(res.getBody().contains("\"user_requested_tenant\":null"));
        Assert.assertTrue(res.getBody().contains("\"kltentrw\":true"));
        Assert.assertTrue(res.getBody().contains("\"user_name\":\"worf\""));
        Assert.assertTrue(res.getBody().contains("\"custom_attribute_names\":[]"));
        Assert.assertFalse(res.getBody().contains("attributes="));

    }

}
