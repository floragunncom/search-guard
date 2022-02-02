/*
 * Copyright 2022 floragunn GmbH
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

import java.time.Duration;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class CacheConfig implements Document<CacheConfig> {

    public static CacheConfig DEFAULT = new CacheConfig(true, Duration.ofMinutes(5), null, 1000);

    private final Duration expireAfterWrite;
    private final Duration expireAfterAccess;
    private final Integer maxSize;
    private final boolean enabled;
    private final DocNode source;

    public CacheConfig(boolean enabled, Duration expireAfterWrite, Duration expireAfterAccess, Integer maxSize) {
        this.enabled = enabled;
        this.expireAfterWrite = expireAfterWrite;
        this.expireAfterAccess = expireAfterAccess;
        this.maxSize = maxSize;
        this.source = null;
    }

    public CacheConfig(DocNode config, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors);

        this.enabled = vNode.get("enabled").withDefault(true).asBoolean();
        this.expireAfterWrite = vNode.get("expire_after_write").asDuration();
        this.expireAfterAccess = vNode.get("expire_after_access").asDuration();
        this.maxSize = vNode.get("max_size").asInteger();
        this.source = config;

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    public <K, V> Cache<K, V> build() {
        if (!enabled) {
            return null;
        }

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();

        if (expireAfterWrite != null) {
            cacheBuilder.expireAfterWrite(expireAfterWrite);
        }

        if (expireAfterAccess != null) {
            cacheBuilder.expireAfterAccess(expireAfterAccess);
        }

        if (maxSize != null) {
            cacheBuilder.maximumSize(maxSize);
        }

        return cacheBuilder.build();
    }

    @Override
    public String toString() {
        return "CacheConfig [expireAfterWrite=" + expireAfterWrite + ", expireAfterAccess=" + expireAfterAccess + ", maxSize=" + maxSize
                + ", enabled=" + enabled + "]";
    }
}
