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
import java.util.stream.Collectors;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SimplePathPatch implements DocPatch {
    public static final String MEDIA_TYPE = "application/x-simple-path-patch+json";

    private List<Operation> operations;

    public SimplePathPatch(Operation... operations) {
        this.operations = Arrays.asList(operations);
    }

    SimplePathPatch(DocNode source) throws ConfigValidationException {
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
        if (!(obj instanceof SimplePathPatch)) {
            return false;
        }
        SimplePathPatch other = (SimplePathPatch) obj;
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
        private final String pathString;
        private final List<String> path;
        private final Object set;

        public Operation(String path, Object set) {
            this.path = Splitter.on('.').splitToList(path);
            this.set = set;
            this.pathString = path;
        }

        public Operation(List<String> path, Object set) {
            this.path = path;
            this.set = set;
            this.pathString = path.stream().collect(Collectors.joining("."));
        }

        Operation(DocNode docNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);
            this.pathString = vNode.get("path").required().asString();
            this.path = this.pathString != null ? Splitter.on('.').splitToList(this.pathString) : null;
            this.set = vNode.get("set").required().asAnything();
            validationErrors.throwExceptionForPresentErrors();
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("path", pathString, "set", set);
        }

        Object apply(Object document) {
            Object targetObject = find(document, 0);

            if (targetObject instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> targetMap = (Map<String, Object>) targetObject;
                targetMap.put(path.get(path.size() - 1), set);
            }

            return document;
        }

        private Object find(Object object, int i) {
            if (i >= path.size() - 1) {
                return object;
            }

            String element = path.get(i);

            if (object instanceof Map) {
                Object subObject = ((Map<?, ?>) object).get(element);

                if (subObject != null) {
                    return find(subObject, i + 1);
                } else {
                    return null;
                }
            } else if (object instanceof Collection) {
                Object candidate = null;
                int candidateCount = 0;

                for (Object subObject : ((Collection<?>) object)) {
                    if (subObject instanceof Map) {
                        Map<?, ?> subMap = (Map<?, ?>) subObject;

                        if (element.equals(subMap.get("id")) || element.equals(subMap.get("type"))) {
                            candidate = subObject;
                            candidateCount++;
                        }
                    }
                }

                if (candidateCount == 1) {
                    return find(candidate, i + 1);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((path == null) ? 0 : path.hashCode());
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
            if (path == null) {
                if (other.path != null) {
                    return false;
                }
            } else if (!path.equals(other.path)) {
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

        @Override
        public String toString() {
            return "Operation [path=" + path + ", set=" + set + "]";
        }
    }

    @Override
    public String toString() {
        return "SimplePathPatch [operations=" + operations + "]";
    }

}