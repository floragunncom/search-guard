/*
 * Copyright 2015-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.cache;

import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;

public class CachingTest extends SingleClusterTest {

    @Override
    protected String getResourceFolder() {
        return "cache";
    }

    @Before
    public void reset() {
        DummyHTTPAuthenticator.reset();
        DummyAuthorizer.reset();
        DummyAuthenticationBackend.reset();

    }

    @Test
    public void testRestCaching() throws Exception {
        setup(Settings.EMPTY, new DynamicSgConfig(), Settings.EMPTY);
        final RestHelper rh = nonSslRestHelper();
        HttpResponse res = rh.executeGetRequest("_searchguard/authinfo?pretty");
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("_searchguard/authinfo?pretty");
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("_searchguard/authinfo?pretty");
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());

        Assert.assertEquals(3, DummyHTTPAuthenticator.getCount());
        Assert.assertEquals(1, DummyAuthorizer.getCount());
        Assert.assertEquals(3, DummyAuthenticationBackend.getAuthCount());
        Assert.assertEquals(0, DummyAuthenticationBackend.getExistsCount());
    }

    @Test
    public void testRestNoCaching() throws Exception {
        final Settings settings = Settings.builder().put("searchguard.cache.ttl_minutes", 0).build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings);
        final RestHelper rh = nonSslRestHelper();
        HttpResponse res = rh.executeGetRequest("_searchguard/authinfo?pretty");
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("_searchguard/authinfo?pretty");
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("_searchguard/authinfo?pretty");
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());

        Assert.assertEquals(3, DummyHTTPAuthenticator.getCount());
        Assert.assertEquals(3, DummyAuthorizer.getCount());
        Assert.assertEquals(3, DummyAuthenticationBackend.getAuthCount());
        Assert.assertEquals(0, DummyAuthenticationBackend.getExistsCount());
    }

    @Test
    public void testRestCachingWithImpersonation() throws Exception {
        final Settings settings = Settings.builder().putList("searchguard.authcz.rest_impersonation_user.dummy", "*").build();
        setup(Settings.EMPTY, new DynamicSgConfig(), settings);
        final RestHelper rh = nonSslRestHelper();
        HttpResponse res = rh.executeGetRequest("_searchguard/authinfo?pretty", new BasicHeader("sg_impersonate_as", "impuser"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("_searchguard/authinfo?pretty", new BasicHeader("sg_impersonate_as", "impuser"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("_searchguard/authinfo?pretty", new BasicHeader("sg_impersonate_as", "impuser"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        res = rh.executeGetRequest("_searchguard/authinfo?pretty", new BasicHeader("sg_impersonate_as", "impuser2"));
        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());

        Assert.assertEquals(4, DummyHTTPAuthenticator.getCount());
        Assert.assertEquals(3, DummyAuthorizer.getCount());
        Assert.assertEquals(4, DummyAuthenticationBackend.getAuthCount());
        Assert.assertEquals(2, DummyAuthenticationBackend.getExistsCount());

    }

}
