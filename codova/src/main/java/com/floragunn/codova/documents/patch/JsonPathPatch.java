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

package com.floragunn.codova.documents.patch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;

public class JsonPathPatch implements DocPatch {
    public static final String MEDIA_TYPE = "application/x-json-path-patch+json";

    private List<Operation> operations;

    public JsonPathPatch(Operation... operations) {
        this.operations = Arrays.asList(operations);
    }

    JsonPathPatch(DocNode source) throws ConfigValidationException {
        if (source.isList()) {
            ValidationErrors validationErrors = new ValidationErrors();

            int i = 0;

            ArrayList<Operation> operations = new ArrayList<>();

            for (DocNode element : source.toListOfNodes()) {
                try {
                    operations.add(new Operation(element));
                } catch (ConfigValidationException e) {
                    validationErrors.add(String.valueOf(i), e);
                }
                i++;
            }
            validationErrors.throwExceptionForPresentErrors();
            this.operations = operations;
        } else {
            this.operations = ImmutableList.of(new Operation(source));
        }
    }

    @Override
    public Object toBasicObject() {
        return operations;
    }

    @Override
    public DocNode apply(DocNode targetDocument) {
        Object document = createMutableCopy(targetDocument.toBasicObject());

        for (Operation operation : this.operations) {
            document = operation.apply(document);
        }

        return DocNode.wrap(document);
    }

    @Override
    public String getMediaType() {
        return MEDIA_TYPE;
    }

    private static Object createMutableCopy(Object object) {
        if (object instanceof Map) {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>(((Map<?, ?>) object).size());

            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                result.put(String.valueOf(entry.getKey()), createMutableCopy(entry.getValue()));
            }

            return result;
        } else if (object instanceof Collection) {
            ArrayList<Object> result = new ArrayList<>(((Collection<?>) object).size());

            for (Object element : ((Collection<?>) object)) {
                result.add(createMutableCopy(element));
            }

            return result;
        } else {
            return object;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((operations == null) ? 0 : operations.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JsonPathPatch)) {
            return false;
        }
        JsonPathPatch other = (JsonPathPatch) obj;
        if (operations == null) {
            if (other.operations != null) {
                return false;
            }
        } else if (!operations.equals(other.operations)) {
            return false;
        }
        return true;
    }

    public static class Operation implements Document<Operation> {
        private final JsonPath jsonPath;
        private final Object set;

        public Operation(JsonPath jsonPath, Object set) {
            this.jsonPath = jsonPath;
            this.set = set;
        }

        Operation(DocNode docNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);
            this.jsonPath = vNode.get("path").required().asJsonPath();
            this.set = vNode.get("set").required().asAnything();
            validationErrors.throwExceptionForPresentErrors();
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("path", jsonPath.getPath(), "set", set);
        }

        Object apply(Object document) {
            return this.jsonPath.set(document, set, BasicJsonPathDefaultConfiguration.defaultConfiguration());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((jsonPath == null) ? 0 : jsonPath.getPath().hashCode());
            result = prime * result + ((set == null) ? 0 : set.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Operation)) {
                return false;
            }
            Operation other = (Operation) obj;
            if (jsonPath == null) {
                if (other.jsonPath != null) {
                    return false;
                }
            } else if (!jsonPath.getPath().equals(other.jsonPath.getPath())) {
                return false;
            }
            if (set == null) {
                if (other.set != null) {
                    return false;
                }
            } else if (!set.equals(other.set)) {
                return false;
            }
            return true;
        }
    }

}
