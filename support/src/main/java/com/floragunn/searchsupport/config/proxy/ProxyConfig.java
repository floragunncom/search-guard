package com.floragunn.searchsupport.config.proxy;

import org.apache.http.HttpHost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.InvalidAttributeValue;

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
