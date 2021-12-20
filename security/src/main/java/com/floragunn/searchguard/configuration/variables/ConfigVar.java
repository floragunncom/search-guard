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

package com.floragunn.searchguard.configuration.variables;

import org.joda.time.Instant;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchsupport.indices.IndexMapping;
import com.floragunn.searchsupport.indices.IndexMapping.BinaryProperty;
import com.floragunn.searchsupport.indices.IndexMapping.DisabledIndexProperty;
import com.floragunn.searchsupport.indices.IndexMapping.DynamicIndexMapping;
import com.floragunn.searchsupport.indices.IndexMapping.KeywordProperty;
import com.floragunn.searchsupport.indices.IndexMapping.ObjectProperty;
import com.floragunn.searchsupport.util.ImmutableMap;

public class ConfigVar implements Document {

    static IndexMapping INDEX_MAPPING = new DynamicIndexMapping(//
            new DisabledIndexProperty("value"), //
            new KeywordProperty("scope"), //
            new KeywordProperty("updated"), //
            new ObjectProperty("encrypted", new BinaryProperty("value"), new KeywordProperty("key"), new KeywordProperty("iv")));

    private final Object value;
    private final String scope;
    private final String updated;

    private final String encValue;
    private final String encKey;
    private final String encIv;

    private ConfigVar(Object value, String scope, String updated, String encValue, String encKey, String encIv) {
        this.value = value;
        this.scope = scope;
        this.updated = updated;
        this.encValue = encValue;
        this.encKey = encKey;
        this.encIv = encIv;
    }

    public ConfigVar(DocNode docNode) throws ConfigValidationException {
        if (docNode.hasNonNull("value")) {
            this.value = docNode.get("value");
        } else if (docNode.hasNonNull("value_json")) {
            try {
                this.value = DocReader.json().read(docNode.getAsString("value_json"));
            } catch (ConfigValidationException e) {
                throw new ConfigValidationException(new ValidationErrors().add("value_json", e));
            }
        } else {
            this.value = null;
        }

        this.scope = docNode.getAsString("scope");
        this.updated = docNode.getAsString("updated");

        if (docNode.hasNonNull("encrypted")) {
            DocNode encrypted = docNode.getAsNode("encrypted");
            this.encValue = encrypted.getAsString("value");
            this.encKey = encrypted.getAsString("key");
            this.encIv = encrypted.getAsString("iv");

            if (this.encKey == null || this.encKey.length() == 0) {
                throw new ConfigValidationException(new MissingAttribute("encrypted.key"));
            }

            if (this.encValue == null || this.encValue.length() == 0) {
                throw new ConfigValidationException(new MissingAttribute("encrypted.value"));
            }
        } else {
            this.encValue = null;
            this.encKey = null;
            this.encIv = null;
        }

        if (this.value == null && this.encValue == null) {
            throw new ConfigValidationException(new MissingAttribute("value"));
        }
    }

    public ConfigVar updatedNow() {
        return new ConfigVar(this.value, this.scope, Instant.now().toString(), this.encValue, this.encKey, this.encIv);
    }

    @Override
    public Object toBasicObject() {
        if (encValue != null) {
            return ImmutableMap.ofNonNull("encrypted", ImmutableMap.ofNonNull("value", encValue, "key", encKey, "iv", encIv), "scope", scope,
                    "updated", updated);
        } else {
            return ImmutableMap.ofNonNull("value", value, "scope", scope, "updated", updated);
        }
    }

    public Object toBasicObjectForIndex() {
        if (encValue != null) {
            return ImmutableMap.ofNonNull("encrypted", ImmutableMap.ofNonNull("value", encValue, "key", encKey, "iv", encIv), "scope", scope,
                    "updated", updated);
        } else {
            return ImmutableMap.ofNonNull("value", DocWriter.json().writeAsString(this.value), "scope", scope, "updated", updated);
        }
    }

    public Object getValue() {
        return value;
    }

    public String getScope() {
        return scope;
    }

    public String getUpdated() {
        return updated;
    }

    public String getEncValue() {
        return encValue;
    }

    public String getEncKey() {
        return encKey;
    }

    public String getEncIv() {
        return encIv;
    }

}
