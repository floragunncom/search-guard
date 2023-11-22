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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.watch.common.ProxyTypeProvider.Type;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;

public class HttpProxyConfig implements ToXContent {

    private static final Logger log = LogManager.getLogger(HttpProxyConfig.class);

    static final String PROXY_FIELD = "proxy";

    private String proxy;

    private Supplier<HttpHost> hostSupplier;
    private Type type;

    private HttpProxyConfig() {}

    /**
     * Various actions use HttpProxyConfig class. Each watch can be composed of multiple actions. Even if some actions have incorrect
     * configuration other actions can still work correctly. The HttpProxyConfig is parsed in two cases. When a new Watch is created or updated
     * or when the watch is loaded during plugin start time. Validation in both described cases should work in various ways. When a watch is
     * created then all validation errors should be reported to the end user. Therefore, strict validation is performed in such case. Other
     * behaviour is needed when a watch is loaded during security plugin initialization phase. In such case lenient validation should be
     * applied. That means if one of the action which belongs to the watch contain an error (e.g. invalid proxy id) then only action
     * which contains incorrect configuration should be unavailable. Other actions should work correctly. Therefore, when the action
     * configuration is loaded from an index during start time then the lenient validation is applied. That means that the action with
     * buggy configuration will not use proxy at all, but exception is not thrown when HttpProxyConfig object is created.
     * Thus action object will be created correctly and other actions which belongs to the same watch can still work correctly.
     * @param httpProxyHostRegistry provides http proxy configs stored in an index.
     * @param validationLevel describe how strict validation should be applied during parsing configuration
     */
    public static HttpProxyConfig create(ValidatingDocNode doc, HttpProxyHostRegistry httpProxyHostRegistry, ValidationLevel validationLevel)
            throws ConfigValidationException {
        HttpProxyConfig result = new HttpProxyConfig();
        result.init(doc, httpProxyHostRegistry, validationLevel);
        return result;
    }

    public static HttpProxyConfig create(String value) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        HttpProxyConfig result = new HttpProxyConfig();
        result.proxy = value;
        result.initWithoutUsingStoredProxy(validationErrors);

        validationErrors.throwExceptionForPresentErrors();
        return result;
    }

    public HttpHost getProxy() {
        return Optional.ofNullable(hostSupplier).map(Supplier::get)
                .orElse(null);
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return String.format("HttpProxyConfig [proxy=%s, type=%s]", proxy, type);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (Type.USE_DEFAULT_PROXY == type) {
            builder.field(PROXY_FIELD, ProxyTypeProvider.DEFAULT_PROXY_KEYWORD);
        } else if (Type.USE_NO_PROXY == type) {
            builder.field(PROXY_FIELD, ProxyTypeProvider.NONE_PROXY_KEYWORD);
        } else if (proxy != null) {
            builder.field(PROXY_FIELD, proxy);
        } else {
            // Should not happen
            builder.field(PROXY_FIELD, "default");
        }

        return builder;
    }



    private void init(ValidatingDocNode doc, HttpProxyHostRegistry httpProxyHostRegistry, ValidationLevel validationLevel) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(doc, validationErrors);

        this.proxy = vJsonNode.get(PROXY_FIELD).asString();

        if (ProxyTypeProvider.determineTypeBasedOnValue(proxy) == Type.USE_STORED_PROXY) {
            initUsingStoredProxy(validationErrors, httpProxyHostRegistry, validationLevel);
        } else {
            initWithoutUsingStoredProxy(validationErrors);
        }

        validationErrors.throwExceptionForPresentErrors();
    }

    private void initUsingStoredProxy(ValidationErrors validationErrors, HttpProxyHostRegistry httpProxyHostRegistry, ValidationLevel validationLevel) {
        Objects.requireNonNull(httpProxyHostRegistry, "HttpProxyHostRegistry is required");

        validateProxyIdIfStrictValidationIsRequired(proxy, httpProxyHostRegistry, validationLevel, validationErrors);
        this.hostSupplier = () -> httpProxyHostRegistry.findHttpProxyHost(proxy)
                .orElseGet(() -> {
                    log.warn("Watch uses not existing proxy with id '{}', connections will not be routed through proxy.", proxy);
                    return null;
                });
        this.type = Type.USE_STORED_PROXY;
    }

    private void initWithoutUsingStoredProxy(ValidationErrors validationErrors) {
        type = ProxyTypeProvider.determineTypeBasedOnValue(proxy);
        if (Type.USE_DEFAULT_PROXY == type || Type.USE_NO_PROXY == type) {
            this.hostSupplier = () -> null;
        } else if (Type.USE_INLINE_PROXY == type){
            try {
                HttpHost httpHost = HttpHost.create(proxy);
                this.hostSupplier = () -> httpHost;
            } catch (IllegalArgumentException e) {
                validationErrors.add(new InvalidAttributeValue(PROXY_FIELD, proxy, "URI or default or none").cause(e));
            }
        } else {
            throw new IllegalArgumentException("Cannot initialize proxy of type: " + type + " without using stored proxy");
        }
    }

    private static void validateProxyIdIfStrictValidationIsRequired(
            String proxyId, HttpProxyHostRegistry httpProxyHostRegistry, ValidationLevel validationLevel, ValidationErrors validationErrors) {
        if(validationLevel.isStrictValidation()) {
            Optional<HttpHost> httpProxyHostOptional = httpProxyHostRegistry.findHttpProxyHost(proxyId);
            if (! httpProxyHostOptional.isPresent()) {
                validationErrors.add(PROXY_FIELD, new ValidationErrors(new ValidationError(null, "Http proxy '" + proxyId + "' not found.")));
            }
        }
    }
}
