/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.codova.config.net;

import java.util.Collections;
import java.util.Map;

import org.apache.http.HttpHost;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ProxyConfigTest {

    @Test
    public void testComplexSettings() throws Exception {

        Map<String, Object> proxySettings = ImmutableMap.of("proxy.host", "127.0.0.8", "proxy.port", 888, "proxy.scheme", "http");

        ProxyConfig proxyConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertEquals(HttpHost.create("http://127.0.0.8:888"), proxyConfig.getHost());
    }

    @Test
    public void testComplexSettingWithSchemeDefault() throws Exception {

        Map<String, Object> proxySettings = ImmutableMap.of("proxy.host", "127.0.0.8", "proxy.port", 888);

        ProxyConfig proxConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertEquals(HttpHost.create("https://127.0.0.8:888"), proxConfig.getHost());
    }

    @Test
    public void testSimpleSettings() throws Exception {

        Map<String, Object> proxySettings = ImmutableMap.of("proxy", "http://127.0.0.8:555");

        ProxyConfig proxConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertEquals(HttpHost.create("http://127.0.0.8:555"), proxConfig.getHost());
    }

    @Test
    public void testAbsentSettings() throws Exception {

        Map<String, Object> proxySettings = Collections.emptyMap();

        ProxyConfig proxConfig = ProxyConfig.parse(proxySettings, "proxy");

        Assert.assertNull(proxConfig.getHost());
    }

}
