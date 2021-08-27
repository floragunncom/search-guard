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

import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

public class ProxyConfig {

    private final HttpHost host;

    public ProxyConfig(HttpHost host) {
        this.host = host;
    }

    public void apply(HttpClientBuilder httpClientBuilder) {
        if (host != null) {
            httpClientBuilder.setProxy(host);
        }
    }

    public static ProxyConfig parse(Map<String, Object> config, String property) throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors);

        if (config.get(property) instanceof Map && ((Map<?, ?>) config.get(property)).containsKey("host")) {
            ValidatingDocNode subNode = new ValidatingDocNode(vNode.getDocumentNode().getAsNode(property), validationErrors);

            String hostName = subNode.get("host").asString();
            int port = subNode.get("port").withDefault(80).asInt();
            String scheme = subNode.get("scheme").withDefault("https").asString();
            return new ProxyConfig(new HttpHost(hostName, port, scheme));
        } else if (config.get(property) instanceof String) {
            String simpleString = (String) config.get(property);

            try {
                return new ProxyConfig(HttpHost.create(simpleString));
            } catch (IllegalArgumentException e) {
                throw new ConfigValidationException(new InvalidAttributeValue(property, "HTTP host URL", simpleString).cause(e));
            }

        } else {
            return new ProxyConfig(null);
        }
    }
    
    @Deprecated
    public static ProxyConfig parse(Settings settings, String property) throws ConfigValidationException {

        Settings subSettings = settings.getByPrefix(property + ".");

        if (!subSettings.isEmpty() && subSettings.hasValue("host")) {
            String hostName = subSettings.get("host");
            int port = subSettings.getAsInt("port", 80);
            String scheme = subSettings.get("scheme", "https");
            return new ProxyConfig(new HttpHost(hostName, port, scheme));
        } else if (settings.hasValue(property)) {
            String simpleString = settings.get(property);

            try {
                return new ProxyConfig(HttpHost.create(simpleString));
            } catch (IllegalArgumentException e) {
                throw new ConfigValidationException(new InvalidAttributeValue(property, "HTTP host URL", simpleString).cause(e));
            }
        } else {
            return new ProxyConfig(null);
        }
    }

    public HttpHost getHost() {
        return host;
    }
}
