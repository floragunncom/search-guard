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

package com.floragunn.codova.documents;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;

public abstract class DocNode implements Map<String, Object> {
    private static final Logger log = LogManager.getLogger(DocNode.class);

    public static final DocNode EMPTY = new PlainJavaObjectAdapter(Collections.EMPTY_MAP);

    public static DocNode of(String key, Object value) {
        HashMap<String, Object> map = new LinkedHashMap<>(1);
        add(map, key, value);
        return new PlainJavaObjectAdapter(map);
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2) {
        HashMap<String, Object> map = new LinkedHashMap<>(2);
        add(map, k1, v1);
        add(map, k2, v2);
        return new PlainJavaObjectAdapter(map);
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        HashMap<String, Object> map = new LinkedHashMap<>(4);
        add(map, k1, v1);
        add(map, k2, v2);
        add(map, k3, v3);

        return new PlainJavaObjectAdapter(map);
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        HashMap<String, Object> map = new LinkedHashMap<>(8);
        add(map, k1, v1);
        add(map, k2, v2);
        add(map, k3, v3);
        add(map, k4, v4);

        return new PlainJavaObjectAdapter(map);
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, Object... more) {
        HashMap<String, Object> map = new LinkedHashMap<>();
        add(map, k1, v1);
        add(map, k2, v2);
        add(map, k3, v3);
        add(map, k4, v4);

        if (more != null) {
            for (int i = 0; i < more.length; i += 2) {
                add(map, String.valueOf(more[i]), more[i + 1]);
            }
        }

        return new PlainJavaObjectAdapter(map);
    }

    public static DocNode wrap(Object object) {
        if (object instanceof DocNode) {
            return (DocNode) object;
        } else {
            return new PlainJavaObjectAdapter(object);
        }
    }
    
    private static void add(Map<String, Object> map, String key, Object value) {
        int dot = key.indexOf('.');

        if (dot == -1) {
            map.put(key, value);
        } else {
            String base = key.substring(0, dot);
            String rest = key.substring(dot + 1);

            Object object = map.computeIfAbsent(base, (k) -> new LinkedHashMap<>());

            if (object instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> subMap = (Map<String, Object>) object;
                add(subMap, rest, value);
            }
        }
    }

    public static class PlainJavaObjectAdapter extends DocNode {
        private final Object object;

        public PlainJavaObjectAdapter(Object object) {
            this(object, null);
        }

        public PlainJavaObjectAdapter(Object object, String key) {
            if (object instanceof PlainJavaObjectAdapter) {
                log.warn("Got " + object + "; this should be not wrapped. Unwrapping", new Exception());
                object = ((PlainJavaObjectAdapter) object).object;
            }

            this.object = object;
            this.key = key;
        }

        @Override
        public Object get(String attribute) {
            if (attribute == null) {
                return this.object;
            } else if (this.object instanceof Map) {
                return toBaseType(((Map<?, ?>) this.object).get(attribute));
            } else {
                return null;
            }
        }

        @Override
        public DocNode getAsNode(String attribute) {
            Object object = get(attribute);

            if (object instanceof DocNode) {
                return (DocNode) object;
            } else if (object != null) {
                return new PlainJavaObjectAdapter(object);
            } else {
                return null;
            }
        }

        @Override
        public List<DocNode> getListOfNodes(String attribute) throws ConfigValidationException {
            Object object;

            if (attribute == null) {
                object = this.object;
            }
            if (this.object instanceof Map) {
                object = ((Map<?, ?>) this.object).get(attribute);
            } else {
                return null;
            }

            if (object == null) {
                return null;
            }

            if (!(object instanceof Collection)) {
                throw new ConfigValidationException(new InvalidAttributeValue(attribute, object, "A list of values"));
            }

            return toListOfDocumentNode((Collection<?>) object);
        }

        @Override
        public <R> List<R> getList(String attribute, ValidatingFunction<String, R> conversionFunction, Object expected)
                throws ConfigValidationException {
            Object object;

            if (attribute == null) {
                object = this.object;
            }
            if (this.object instanceof Map) {
                object = ((Map<?, ?>) this.object).get(attribute);
            } else {
                return null;
            }

            if (object == null) {
                return null;
            }

            if (!(object instanceof Collection)) {
                throw new ConfigValidationException(new InvalidAttributeValue(attribute, object, "A list of values"));
            }

            Collection<?> collection = (Collection<?>) object;

            List<R> result = new ArrayList<>(collection.size());
            ValidationErrors validationErrors = new ValidationErrors();

            int index = 0;

            for (Object subObject : collection) {
                try {
                    if (subObject == null) {
                        result.add(null);
                    } else {
                        result.add(conversionFunction.apply(subObject.toString()));
                    }

                } catch (Exception e) {
                    validationErrors.add(new InvalidAttributeValue(attribute + "." + index, subObject, expected).cause(e));
                }
                index++;
            }

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        private static Object toBaseType(Object object) {
            if (object == null) {
                return null;
            } else if (object instanceof Number || object instanceof String || object instanceof Boolean) {
                return object;
            } else if (object instanceof Collection) {
                return toListOfBaseType((Collection<?>) object);
            } else if (object instanceof DocNode) {
                return object;
            } else if (object instanceof Map) {
                return new PlainJavaObjectAdapter(object);
                //return toStringKeyedMap((Map<?, ?>) object);
            } else {
                throw new RuntimeException("Unexpected type: " + object);
            }
        }

        private static List<Object> toListOfBaseType(Collection<?> object) {
            List<Object> list = new ArrayList<>(object.size());

            for (Object subNode : object) {
                list.add(toBaseType(subNode));
            }

            return list;
        }

        private static List<DocNode> toListOfDocumentNode(Collection<?> object) {
            List<DocNode> list = new ArrayList<>(object.size());

            for (Object subNode : object) {
                list.add(new PlainJavaObjectAdapter(subNode));
            }

            return list;
        }

        @Override
        public Map<String, Object> toMap() throws ConfigValidationException {
            if (this.object instanceof Map) {
                return DocUtils.toStringKeyedMap((Map<?, ?>) this.object);
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, object, "An object"));
            }
        }

        @Override
        public boolean isMap() {
            return object instanceof Map;
        }

        @Override
        public boolean isList(String attribute) {
            if (attribute != null && this.object instanceof Map) {
                return ((Map<?, ?>) this.object).get(attribute) instanceof Collection;
            } else if (attribute == null) {
                return this.object instanceof Collection;
            } else {
                return false;
            }
        }

        @Override
        public boolean isList() {
            return this.object instanceof Collection;
        }

        @Override
        public int size() {
            if (object instanceof Map) {
                return ((Map<?, ?>) object).size();
            } else if (object instanceof Collection) {
                return ((Collection<?>) object).size();
            } else if (object != null) {
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        public boolean isEmpty() {
            return size() != 0;
        }

        @Override
        public boolean containsKey(Object key) {
            if (object instanceof Map) {
                return ((Map<?, ?>) object).containsKey(key);
            } else {
                return false;
            }
        }

        @Override
        public Set<String> keySet() {
            if (object instanceof Map) {
                return ensureSetOfStrings(((Map<?, ?>) object).keySet());
            } else {
                return Collections.emptySet();
            }
        }

        @Override
        public Collection<Object> values() {
            if (object instanceof Map) {
                return new ArrayList<Object>(((Map<?, ?>) object).values());
            } else if (object instanceof Collection) {
                return new ArrayList<Object>(((Collection<?>) object));
            } else if (object != null) {
                return Collections.singleton(object);
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            if (object instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) object;
                Set<Entry<String, Object>> result = new LinkedHashSet<>(map.size());

                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    result.add(Maps.immutableEntry(String.valueOf(entry.getKey()), entry.getValue()));
                }

                return result;
            } else {
                return Collections.emptySet();
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static Set<String> ensureSetOfStrings(Set<?> set) {
            if (containsOnlyStrings(set)) {
                return (Set<String>) (Set) set;
            }

            Set<String> result = new LinkedHashSet<String>(set.size());

            for (Object object : set) {
                if (object != null) {
                    result.add(object.toString());
                } else {
                    result.add(null);
                }
            }

            return result;
        }

        private static boolean containsOnlyStrings(Set<?> set) {
            for (Object object : set) {
                if (!(object instanceof String)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public List<Object> toList() {
            if (object instanceof Collection) {
                return toListOfBaseType((Collection<?>) object);
            } else {
                return Collections.singletonList(toBaseType(object));
            }
        }

        @Override
        public int hashCode() {
            if (object != null) {
                return object.hashCode();
            } else {
                return 0;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            PlainJavaObjectAdapter other = (PlainJavaObjectAdapter) obj;
            return Objects.equals(object, other.object);
        }
    }

    protected String key;

    public abstract Object get(String attribute);

    public abstract List<DocNode> getListOfNodes(String attribute) throws ConfigValidationException;

    public abstract DocNode getAsNode(String attribute);

    public abstract Map<String, Object> toMap() throws ConfigValidationException;

    public abstract boolean isMap();

    public abstract boolean isList();

    public abstract boolean isList(String attribute);

    public abstract List<Object> toList();

    public List<String> toListOfStrings() {
        List<Object> list = toList();

        List<String> result = new ArrayList<>(list.size());

        for (Object e : list) {
            result.add(e != null ? e.toString() : null);
        }

        return result;
    }

    public String getAsString(String attribute) {
        Object object = this.get(attribute);

        if (object != null) {
            return object.toString();
        } else {
            return null;
        }
    }

    public Number getNumber(String attribute) throws ConfigValidationException {
        Object object = this.get(attribute);

        if (object == null) {
            return null;
        } else if (object instanceof Number) {
            return (Number) object;
        } else {
            throw new ConfigValidationException(new InvalidAttributeValue(attribute, object, "A number value"));
        }
    }

    public BigDecimal getBigDecimal(String attribute) throws ConfigValidationException {
        Object object = this.get(attribute);

        if (object == null) {
            return null;
        } else if (object instanceof BigDecimal) {
            return (BigDecimal) object;
        } else if (object instanceof Double || object instanceof Float) {
            return new BigDecimal(((Number) object).doubleValue());
        } else if (object instanceof Long || object instanceof Integer || object instanceof Short) {
            return new BigDecimal(((Number) object).longValue());
        } else {
            try {
                return new BigDecimal(object.toString());
            } catch (NumberFormatException e) {
                throw new ConfigValidationException(new InvalidAttributeValue(attribute, object, "A number value").cause(e));
            }
        }
    }

    public Boolean getBoolean(String attribute) throws ConfigValidationException {
        Object object = this.get(attribute);

        if (object == null) {
            return null;
        } else if (object instanceof Boolean) {
            return (Boolean) object;
        } else {
            throw new ConfigValidationException(new InvalidAttributeValue(attribute, object, "Must be true or false"));
        }
    }

    public <R> R get(String attribute, ValidatingFunction<String, R> conversionFunction, Object expected) throws ConfigValidationException {

        String value = getAsString(attribute);

        if (value == null) {
            return null;
        } else {
            try {
                return conversionFunction.apply(value);
            } catch (ConfigValidationException e) {
                throw new ConfigValidationException(e.getValidationErrors());
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(attribute, value, expected).cause(e));
            }
        }
    }

    public <R> R getFromNode(String attribute, ValidatingFunction<DocNode, R> conversionFunction, Object expected) throws ConfigValidationException {

        DocNode value = getAsNode(attribute);

        if (value == null) {
            return null;
        } else {
            try {
                return conversionFunction.apply(value);
            } catch (ConfigValidationException e) {
                throw new ConfigValidationException(e.getValidationErrors());
            } catch (Exception e) {
                throw new ConfigValidationException(new InvalidAttributeValue(attribute, value, expected).cause(e));
            }
        }
    }

    public <R> List<R> getList(String attribute, ValidatingFunction<String, R> conversionFunction, Object expected) throws ConfigValidationException {

        List<DocNode> nodeList = getListOfNodes(attribute);
        List<R> result = new ArrayList<>(nodeList.size());
        ValidationErrors validationErrors = new ValidationErrors();

        int index = 0;

        for (DocNode node : nodeList) {
            try {
                result.add(node.get(null, conversionFunction, expected));
            } catch (ConfigValidationException e) {
                validationErrors.add(String.valueOf(index), e);
            }
            index++;
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public <R> List<R> getListFromNodes(String attribute, ValidatingFunction<DocNode, R> conversionFunction, Object expected)
            throws ConfigValidationException {

        List<DocNode> nodeList = getListOfNodes(attribute);
        List<R> result = new ArrayList<>(nodeList.size());
        ValidationErrors validationErrors = new ValidationErrors();

        int index = 0;

        for (DocNode node : nodeList) {
            try {
                result.add(node.getFromNode(null, conversionFunction, expected));
            } catch (ConfigValidationException e) {
                validationErrors.add(String.valueOf(index), e);
            }
            index++;
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public List<DocNode> getAsList(String attribute) throws ConfigValidationException {
        if (isList(attribute)) {
            return getListOfNodes(attribute);
        } else {
            return Collections.singletonList(getAsNode(attribute));
        }
    }

    public <R> List<R> getAsList(String attribute, ValidatingFunction<String, R> conversionFunction, Object expected)
            throws ConfigValidationException {
        if (!hasNonNull(attribute)) {
            return Collections.emptyList();
        } else if (isList(attribute)) {
            return getList(attribute, conversionFunction, expected);
        } else {
            return Collections.singletonList(get(attribute, conversionFunction, expected));
        }
    }

    public <R> List<R> getAsListFromNodes(String attribute, ValidatingFunction<DocNode, R> conversionFunction, Object expected)
            throws ConfigValidationException {
        if (!hasNonNull(attribute)) {
            return Collections.emptyList();
        } else if (isList(attribute)) {
            return getListFromNodes(attribute, conversionFunction, expected);
        } else {
            return Collections.singletonList(getFromNode(attribute, conversionFunction, expected));
        }
    }

    public List<String> getListOfStrings(String attribute) throws ConfigValidationException {
        return getList(attribute, (s) -> s, "List of strings");
    }

    public List<String> getAsListOfStrings(String attribute) {
        try {
            return getAsList(attribute, (s) -> s, "List of strings");
        } catch (ConfigValidationException e) {
            // should not happen
            throw new RuntimeException(e);
        }
    }

    public boolean hasNonNull(String attribute) {
        return get(attribute) != null;
    }

    public Settings toSettings() throws ConfigValidationException {
        return Settings.builder().loadFromMap(toMap()).build();
    }

    @Override
    public boolean containsValue(Object value) {
        if (!isMap()) {
            return false;
        }

        return values().contains(value);
    }

    @Override
    public Object get(Object key) {
        if (key instanceof String) {
            return get((String) key);
        } else {
            return null;
        }
    }

    public String getKey() {
        return this.key;
    }

    public List<?> findByJsonPath(String jsonPath) {
        return JsonPath.using(BasicJsonPathDefaultConfiguration.listDefaultConfiguration()).parse(this).read(jsonPath);
    }

    public List<DocNode> findNodesByJsonPath(String jsonPath) {
        Object object = JsonPath.using(BasicJsonPathDefaultConfiguration.listDefaultConfiguration()).parse(this).read(jsonPath);

        if (object instanceof List) {
            List<DocNode> result = new ArrayList<>(((List<?>) object).size());

            for (Object subObject : (List<?>) object) {
                if (subObject instanceof DocNode) {
                    result.add((DocNode) subObject);
                } else {
                    result.add(new PlainJavaObjectAdapter(subObject));
                }
            }

            return result;
        } else if (object instanceof DocNode) {
            return Collections.singletonList((DocNode) object);
        } else if (object != null) {
            return Collections.singletonList(new PlainJavaObjectAdapter(object));
        } else {
            return Collections.emptyList();
        }
    }

    public <T> T findSingleValueByJsonPath(String jsonPath, Class<T> type) {
        return JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration()).parse(this).read(jsonPath, type);
    }

    public DocNode findSingleNodeByJsonPath(String jsonPath) {
        Object object = JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration()).parse(this).read(jsonPath);

        if (object instanceof DocNode) {
            return (DocNode) object;
        } else if (object != null) {
            return new PlainJavaObjectAdapter(object);
        } else {
            return null;
        }
    }

    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException("DocumentNode instances cannot be modified");
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException("DocumentNode instances cannot be modified");
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        throw new UnsupportedOperationException("DocumentNode instances cannot be modified");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("DocumentNode instances cannot be modified");
    }
}
