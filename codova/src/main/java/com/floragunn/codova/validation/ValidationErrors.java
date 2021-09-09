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

package com.floragunn.codova.validation;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public class ValidationErrors implements ToXContent {
    private static final Logger log = LogManager.getLogger(ValidationErrors.class);

    private Multimap<String, ValidationError> attributeToErrorMap;
    private ValidationErrors parent;
    private String attributeInParent;

    public ValidationErrors() {
        this.attributeToErrorMap = LinkedListMultimap.create();
    }

    public ValidationErrors(ValidationErrors parent, String attributeInParent) {
        this.attributeToErrorMap = LinkedListMultimap.create();
        this.parent = parent;
        this.attributeInParent = attributeInParent;
    }

    public ValidationErrors(Multimap<String, ValidationError> multimap) {
        this.attributeToErrorMap = multimap;
    }

    public ValidationErrors(ValidationError singleValidationError) {
        attributeToErrorMap = ImmutableListMultimap.of(singleValidationError.getAttribute(), singleValidationError);
    }

    public void throwExceptionForPresentErrors() throws ConfigValidationException {
        if (hasErrors()) {
            throw new ConfigValidationException(this);
        }
    }

    public boolean hasErrors() {
        return this.attributeToErrorMap.size() > 0;
    }

    public ValidationErrors add(ValidationError validationError) {
        this.attributeToErrorMap.put(validationError.getAttribute(), validationError);

        if (parent != null) {
            parent.add(attributeInParent, new ValidationErrors(validationError));
        }

        return this;
    }
    
    public ValidationErrors add(Collection<ValidationError> validationErrors) {
        for (ValidationError validationError : validationErrors) {
            add(validationError);
        }
        
        return this;
    }


    public ValidationErrors add(String attribute, ValidationErrors validationErrors) {
        for (Map.Entry<String, ValidationError> entry : validationErrors.attributeToErrorMap.entries()) {
            String subAttribute;

            if (attribute != null && !"_".equals(attribute)) {
                if (entry.getKey() != null && !"_".equals(entry.getKey())) {
                    if (entry.getKey().startsWith("[")) {
                        subAttribute = attribute + entry.getKey();
                    } else {
                        subAttribute = attribute + "." + entry.getKey();
                    }

                } else {
                    subAttribute = attribute;
                }
            } else {
                if (entry.getKey() != null && !"_".equals(entry.getKey())) {
                    subAttribute = entry.getKey();
                } else {
                    subAttribute = "_";
                }
            }

            this.attributeToErrorMap.put(subAttribute, entry.getValue());
        }

        if (parent != null) {
            parent.add(attribute == null ? attributeInParent : attribute + "." + attributeInParent, validationErrors);
        }

        return this;
    }

    public ValidationErrors add(String attribute, ConfigValidationException watchValidationException) {
        add(attribute, watchValidationException.getValidationErrors());
        return this;
    }

    public String toJson() {
        try {

            XContentBuilder builder = JsonXContent.contentBuilder();

            this.toXContent(builder, ToXContent.EMPTY_PARAMS);

            return Strings.toString(builder);

        } catch (Exception e) {
            log.error(e.toString(), e);
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, ValidationError> entry : this.attributeToErrorMap.entries()) {
            result.append(entry.getKey()).append(":\n\t").append(entry.getValue().toValidationErrorsOverviewString());
            result.append("\n");
        }

        return result.toString();
    }

    public String toDebugString() {
        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, ValidationError> entry : this.attributeToErrorMap.entries()) {
            result.append(entry.getKey()).append(":\n\t").append(entry.getValue().toValidationErrorsOverviewString());
            result.append("\n");

            if (entry.getValue().getCause() != null) {
                StringWriter stringWriter = new StringWriter();
                entry.getValue().getCause().printStackTrace(new PrintWriter(stringWriter));
                result.append(stringWriter.toString()).append("\n");
            }
        }

        return result.toString();
    }

    public int size() {
        return this.attributeToErrorMap.size();
    }

    public ValidationError getOnlyValidationError() {
        if (this.attributeToErrorMap.size() == 1) {
            ValidationError result = this.attributeToErrorMap.values().iterator().next();

            // FIXME keep attribute names of errors uptodate

            result.setAttribute(this.attributeToErrorMap.keys().iterator().next());

            return result;
        } else {
            return null;
        }
    }

    public Exception getCause() {
        for (ValidationError validationError : attributeToErrorMap.values()) {
            if (validationError.getCause() != null) {
                return validationError.getCause();
            }
        }

        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        for (Map.Entry<String, Collection<ValidationError>> entry : attributeToErrorMap.asMap().entrySet()) {
            builder.field(entry.getKey() != null ? entry.getKey() : "_");

            builder.startArray();

            for (ValidationError validationError : entry.getValue()) {
                builder.value(validationError);
            }

            builder.endArray();
        }

        builder.endObject();

        return builder;
    }
    
    public ValidationErrors mapKeys(Map<String, String> oldToNewKeyMap) {
        if (oldToNewKeyMap.size() == 0 || this.attributeToErrorMap.size() == 0) {
            return this;
        }
        
        Multimap<String, ValidationError> newErrorMap = LinkedListMultimap.create(this.attributeToErrorMap.size());
        
        for (Map.Entry<String, Collection<ValidationError>> entry : this.attributeToErrorMap.asMap().entrySet()) {            
            newErrorMap.putAll(mapKey(entry.getKey(), oldToNewKeyMap), entry.getValue());                    
        }
        
        return new ValidationErrors(newErrorMap);
    }
    
    public Map<String, ValidationErrors> groupByKeys(Map<String, String> oldToNewKeyMap) {
        if (this.attributeToErrorMap.size() == 0) {
            return Collections.emptyMap();
        }
        
        if (oldToNewKeyMap.size() == 0) {
            return Collections.singletonMap("", this);
        }
        
        Map<String, ValidationErrors> result = new LinkedHashMap<>(oldToNewKeyMap.size() + 5);
        
        for (Map.Entry<String, Collection<ValidationError>> entry : this.attributeToErrorMap.asMap().entrySet()) {
            SplitKey splitKey = splitAndMapKey(entry.getKey(), oldToNewKeyMap);
            
            ValidationErrors group = result.computeIfAbsent(splitKey.group, (k) -> new ValidationErrors());
            
            group.attributeToErrorMap.putAll(splitKey.key, entry.getValue()); 
        }
        
        return result;
    }
        
    private String mapKey(String key, Map<String, String> oldToNewKeyMap) {
        String newKey = oldToNewKeyMap.get(key);
        
        if (newKey != null) {
            return newKey;
        }
        
        for (Map.Entry<String, String> entry : oldToNewKeyMap.entrySet()) {
            if (key.startsWith(entry.getKey())) {
                return entry.getValue() + key.substring(entry.getKey().length()); 
            }
        }
        
        return key;
    }
    
    private SplitKey splitAndMapKey(String key, Map<String, String> oldToNewKeyMap) {
  String newKey = oldToNewKeyMap.get(key);
        
        if (newKey != null) {
            return new SplitKey(newKey, "");
        }
        
        for (Map.Entry<String, String> entry : oldToNewKeyMap.entrySet()) {
            if (key.startsWith(entry.getKey()) && key.charAt(entry.getKey().length()) == '.') {
                return new SplitKey(entry.getValue(), key.substring(entry.getKey().length() + 1));
            }
        }
        
        return new SplitKey("", key);
    }


    public static ValidationErrors parse(Map<String, Object> document) {
        ValidationErrors result = new ValidationErrors();

        for (Map.Entry<String, Object> entry : document.entrySet()) {
            result.add(ValidationError.parseArray(entry.getKey(), new DocNode.PlainJavaObjectAdapter(entry.getValue())));
        }

        return result;
    }
    
    private static class SplitKey {
        final String group;
        final String key;

        public SplitKey(String group, String key) {
            this.group = group;
            this.key = key;
        }
        
        
    }
    
}
