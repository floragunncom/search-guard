/*
  * Copyright 2015-2017 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

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
