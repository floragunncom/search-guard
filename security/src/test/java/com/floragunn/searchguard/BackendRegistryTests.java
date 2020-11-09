package com.floragunn.searchguard;

import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.rest.RestHelper;

import java.net.InetAddress;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

public class BackendRegistryTests extends SingleClusterTest {

    @Test
    public void when_user_is_skipped_then_authentication_should_fail() throws Exception {
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

    @Test
    public void when_user_is_blocked_then_authentication_should_fail() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml")
                .setSgBlocks("sg_blocks_user_blocked.yml")
                .setSgConfig("sg_config_skip_users.yml"), Settings.EMPTY, true);

        RestHelper rh = nonSslRestHelper();

        // This request goes through since the user name is not blocked
        RestHelper.HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());

        // This request is blocked by the user name
        resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("Spock", "any_password"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());

        // This request is blocked by a wildcard configuration
        rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("John Doe", "any_password"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
    }

    @Test
    public void when_ip_is_blocked_then_authentication_should_fail() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml")
                .setSgBlocks("sg_blocks_ip_blocked.yml")
                .setSgConfig("sg_config_skip_users.yml"), Settings.EMPTY, true);

        RestHelper rh = nonSslRestHelper();

        // This request should fail due to a IP blocking
        RestHelper.HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
    }

    @Test
    public void when_ip_is_blocked_from_net_then_authentication_should_fail() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml")
                .setSgBlocks("sg_blocks_ip_net_blocked.yml")
                .setSgConfig("sg_config_skip_users.yml"), Settings.EMPTY, true);

        RestHelper rh = nonSslRestHelper();

        // This request should fail due to a IP blocking
        RestHelper.HttpResponse resc = rh.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
    }
    
    @Test
    public void testEnabledOnlyForHosts() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml")
                .setSgConfig("sg_config_auth_domains_with_disabled_ips.yml"), Settings.EMPTY, true);

        RestHelper rh002 = nonSslRestHelper(InetAddress.getByName("127.0.0.2"));
        RestHelper rh003 = nonSslRestHelper(InetAddress.getByName("127.0.0.3"));

        RestHelper.HttpResponse resc = rh002.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());

        resc = rh003.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
        
        resc = rh003.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sgr_user", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("backend_roles=[abc_ber]"));
    }
    
    @Test
    public void testEnabledOnlyForNetmask() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig()
                .setSgInternalUsers("sg_internal_users_sgr.yml")
                .setSgConfig("sg_config_auth_domains_with_disabled_ips2.yml"), Settings.EMPTY, true);

        RestHelper rh002 = nonSslRestHelper(InetAddress.getByName("127.0.0.2"));
        RestHelper rh003 = nonSslRestHelper(InetAddress.getByName("127.0.0.3"));
        RestHelper rh004 = nonSslRestHelper(InetAddress.getByName("127.0.0.4"));
        RestHelper rh005 = nonSslRestHelper(InetAddress.getByName("127.0.0.5"));

        RestHelper.HttpResponse resc = rh004.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(resc.getBody(), HttpStatus.SC_OK, resc.getStatusCode());

        resc = rh005.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        
        resc = rh002.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
        
        resc = rh003.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("any_name", "any_password"));
        Assert.assertEquals(HttpStatus.SC_UNAUTHORIZED, resc.getStatusCode());
        
        resc = rh003.executeGetRequest("_searchguard/authinfo?pretty", encodeBasicHeader("sgr_user", "nagilum"));
        Assert.assertEquals(HttpStatus.SC_OK, resc.getStatusCode());
        Assert.assertTrue(resc.getBody(), resc.getBody().contains("backend_roles=[abc_ber]"));
    }
}
