package com.floragunn.searchsupport.config.proxy;

import org.apache.http.HttpHost;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

public class ProxyConfigTest {

    @Test
    public void testComplexSettings() throws Exception {

        Settings proxySettings = Settings.builder().put("proxy.host", "127.0.0.8").put("proxy.port", 888).put("proxy.scheme", "http").build();

        ProxyConfig proxConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertEquals(HttpHost.create("http://127.0.0.8:888"), proxConfig.getHost());
    }

    @Test
    public void testComplexSettingWithSchemeDefault() throws Exception {

        Settings proxySettings = Settings.builder().put("proxy.host", "127.0.0.8").put("proxy.port", 888).build();

        ProxyConfig proxConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertEquals(HttpHost.create("https://127.0.0.8:888"), proxConfig.getHost());
    }

    @Test
    public void testSimpleSettings() throws Exception {

        Settings proxySettings = Settings.builder().put("proxy", "http://127.0.0.8:555").build();

        ProxyConfig proxConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertEquals(HttpHost.create("http://127.0.0.8:555"), proxConfig.getHost());
    }

    @Test
    public void testAbsentSettings() throws Exception {

        Settings proxySettings = Settings.builder().build();

        ProxyConfig proxConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertNull(proxConfig.getHost());
    }
}
