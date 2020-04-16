package com.floragunn.searchguard;

import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

public class BackendRegistryTests extends SingleClusterTest {

    @Test
    public void when_user_is_skipped_then_authenticaton_should_fail() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml")
                .setSgConfig("sg_config_skip_users.yml"), Settings.EMPTY, true);

        RestHelper rh = nonSslRestHelper();

        // This request is answered by the first authc backend, namely the noop backend. Any non-skipped user is authenticated at this point
        RestHelper.HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());

        // The first backend (noop) skips over for this given user=peter, the second backend doesn't know this user, thus leading to a HTTP 401 response
        resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("peter", "apw"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
    }
}
