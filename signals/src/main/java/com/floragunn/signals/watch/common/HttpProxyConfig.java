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

package com.floragunn.signals.watch.common;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

public class HttpProxyConfig implements ToXContent {

    public static final HttpProxyConfig USE_DEFAULT = new HttpProxyConfig(Type.USE_DEFAULT_PROXY);
    public static final HttpProxyConfig USE_NONE = new HttpProxyConfig(Type.USE_NO_PROXY);

    private final HttpHost proxy;
    private final Type type;

    public HttpProxyConfig(HttpHost proxy) {
        this.proxy = proxy;
        this.type = Type.USE_SPECIFIC_PROXY;
    }

    public HttpProxyConfig(Type type) {
        this.type = type;
        this.proxy = null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (type == Type.USE_DEFAULT_PROXY) {
            builder.value("default");
        } else if (type == Type.USE_NO_PROXY) {
            builder.value("none");
        } else if (proxy != null) {
            builder.value(proxy.toURI());
        } else {
            // Should not happen
            builder.value("default");
        }

        return builder;
    }

    public HttpHost getProxy() {
        return proxy;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "HttpProxyConfig [proxy=" + proxy + ", type=" + type + "]";
    }

    static enum Type {
        USE_SPECIFIC_PROXY, USE_DEFAULT_PROXY, USE_NO_PROXY
    }

    public static HttpProxyConfig create(JsonNode jsonNode) throws ConfigValidationException {

        if (jsonNode == null || jsonNode instanceof NullNode) {
            return null;
        }

        if (!(jsonNode instanceof TextNode)) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, jsonNode.textValue(), "URI or default or none", jsonNode));
        }

        String value = jsonNode.textValue();

        return create(value);
    }

    public static HttpProxyConfig create(String value) throws ConfigValidationException {        
        if (value == null || "default".equalsIgnoreCase(value)) {
            return USE_DEFAULT;
        } else if ("none".equalsIgnoreCase(value)) {
            return USE_NONE;
        } else {
            try {
                return new HttpProxyConfig(HttpHost.create(value));
            } catch (IllegalArgumentException e) {
                throw new ConfigValidationException(
                        new InvalidAttributeValue(null, value, "URI or default or none").cause(e));
            }
        }
    }
}
