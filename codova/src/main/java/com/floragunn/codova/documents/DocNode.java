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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;

public abstract class DocNode implements Map<String, Object> {
    private static final Logger log = LoggerFactory.getLogger(DocNode.class);

    public static final DocNode EMPTY = new PlainJavaObjectAdapter(Collections.EMPTY_MAP);

    public static DocNode of(String key, Object value) {
        return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(Collections.singletonMap(key, value)));
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2) {
        HashMap<String, Object> map = new LinkedHashMap<>(2);
        map.put(k1, v1);
        map.put(k2, v2);
        return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(map));
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        HashMap<String, Object> map = new LinkedHashMap<>(3);
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);

        return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(map));
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        HashMap<String, Object> map = new LinkedHashMap<>(4);
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);

        return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(map));
    }

    public static DocNode of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, Object... more) {
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);

        if (more != null) {
            for (int i = 0; i < more.length; i += 2) {
                map.put(String.valueOf(more[i]), more[i + 1]);
            }
        }

        return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(map));
    }

    public static DocNode wrap(Object object) {
        if (object instanceof DocNode) {
            return (DocNode) object;
        } else {
            return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(object));
        }
    }

    private static class PlainJavaObjectAdapter extends DocNode {
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

            if (object != null) {
                return DocNode.wrap(object);
            } else {
                return null;
            }
        }

        @Override
        public List<DocNode> getListOfNodes(String attribute) throws ConfigValidationException {
            Object object = null;

            if (attribute == null) {
                object = this.object;
            } else if (this.object instanceof Map) {
                object = ((Map<?, ?>) this.object).get(attribute);
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
                return (Map<?, ?>) object;
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

    static class SubTreeView extends DocNode {
        private final DocNode delegate;
        private final String viewAttributeWithDot;
        private DocNode simpleMap;
        private final int size;
        private Set<String> keySet;
        private Set<Entry<String, Object>> entrySet;

        static DocNode getSubTree(DocNode docNode, String viewAttribute) {
            String viewAttributeWithDot = viewAttribute + ".";
            int keysWithDot = 0;
            int actualObjectSize = 0;

            for (String key : docNode.keySet()) {
                if (key.startsWith(viewAttributeWithDot)) {
                    keysWithDot++;
                } else if (key.equals(viewAttribute)) {
                    Object o = docNode.get(key);

                    if (o instanceof Map) {
                        actualObjectSize += ((Map<?, ?>) o).size();
                    } else {
                        actualObjectSize++;
                    }
                }
            }

            if (keysWithDot != 0) {
                return new SubTreeView(docNode, viewAttribute, viewAttributeWithDot, actualObjectSize + keysWithDot);
            } else if (actualObjectSize != 0) {
                return docNode.getAsNode(viewAttribute);
            } else {
                return EMPTY;
            }
        }

        private SubTreeView(DocNode delegate, String viewAttribute, String viewAttributeWithDot, int size) {
            this.delegate = delegate;
            this.viewAttributeWithDot = viewAttributeWithDot;
            this.size = size;

            Object simpleInDelegate = delegate.get(viewAttribute);

            if (simpleInDelegate instanceof Map) {
                this.simpleMap = DocNode.wrap((Map<?, ?>) simpleInDelegate);
            }
        }

        @Override
        public int size() {
            return this.size;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsKey(Object key) {
            if (key == null) {
                return !isEmpty();
            } else if (simpleMap != null && simpleMap.containsKey(key)) {
                return true;
            } else {
                return delegate.containsKey(viewAttributeWithDot + key);
            }
        }

        @Override
        public Set<String> keySet() {
            if (keySet == null) {
                HashSet<String> temp = new HashSet<>();
                for (String key : delegate.keySet()) {
                    if (key.startsWith(viewAttributeWithDot)) {
                        temp.add(key.substring(viewAttributeWithDot.length()));
                    }
                }

                if (simpleMap != null) {
                    temp.addAll(simpleMap.keySet());
                }

                this.keySet = temp;
            }

            return keySet;
        }

        @Override
        public Collection<Object> values() {
            Collection<Object> result = new ArrayList<>();

            for (String key : delegate.keySet()) {
                if (key.startsWith(viewAttributeWithDot)) {
                    result.add(delegate.get(key));
                }
            }

            if (simpleMap != null) {
                result.addAll(simpleMap.values());
            }

            return result;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            if (entrySet == null) {
                HashSet<Entry<String, Object>> temp = new HashSet<>();
                for (Entry<String, Object> entry : delegate.entrySet()) {
                    String key = entry.getKey();

                    if (key.startsWith(viewAttributeWithDot)) {
                        temp.add(new AbstractMap.SimpleImmutableEntry<>(key.substring(viewAttributeWithDot.length()), entry.getValue()));
                    }
                }

                if (simpleMap != null) {
                    temp.addAll(simpleMap.entrySet());
                }

                this.entrySet = temp;
            }

            return entrySet;
        }

        @Override
        public Object get(String attribute) {
            if (simpleMap != null && simpleMap.containsKey(attribute)) {
                return simpleMap.get(attribute);
            } else {
                return delegate.get(viewAttributeWithDot + attribute);
            }
        }

        @Override
        public List<DocNode> getListOfNodes(String attribute) throws ConfigValidationException {
            if (simpleMap != null && simpleMap.containsKey(attribute)) {
                return simpleMap.getListOfNodes(attribute);
            } else {
                return delegate.getListOfNodes(viewAttributeWithDot + attribute);
            }
        }

        @Override
        public DocNode getAsNode(String attribute) {
            if (simpleMap != null && simpleMap.containsKey(attribute)) {
                return simpleMap.getAsNode(attribute);
            } else {
                return delegate.getAsNode(viewAttributeWithDot + attribute);
            }
        }

        @Override
        public Map<String, Object> toMap() throws ConfigValidationException {
            Map<String, Object> result = new LinkedHashMap<>(this.size);
            
            for (Map.Entry<String, Object> entry : entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
            
            return result;
        }

        @Override
        public boolean isMap() {
            return true;
        }

        @Override
        public boolean isList() {
            return false;
        }

        @Override
        public boolean isList(String attribute) {
            if (simpleMap != null && simpleMap.containsKey(attribute)) {
                return simpleMap.isList(attribute);
            } else {
                return delegate.isList(viewAttributeWithDot + attribute);
            }
        }

        @Override
        public List<Object> toList() {
            try {
                return Collections.singletonList(toMap());
            } catch (ConfigValidationException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class AttributeNormalizingAdapter extends DocNode {

        private final DocNode delegate;
        private Set<String> rootKeyNames;

        AttributeNormalizingAdapter(DocNode delegate) {
            this.delegate = delegate;
        }

        @Override
        public int size() {
            return rootKeyNames().size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean containsKey(Object keyObject) {
            if (delegate.containsKey(keyObject)) {
                return true;
            }

            if (!(keyObject instanceof String)) {
                return false;
            }

            String key = (String) keyObject;

            int dot = key.indexOf('.');

            if (dot == -1) {
                return rootKeyNames().contains(key);
            } else {
                String firstPart = key.substring(0, dot);

                if (rootKeyNames().contains(firstPart)) {
                    return getAsNode(firstPart).containsKey(key.substring(dot + 1));
                } else {
                    return false;
                }
            }
        }

        @Override
        public Set<String> keySet() {
            return delegate.keySet();
        }

        @Override
        public Collection<Object> values() {
            return delegate.values();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return delegate.entrySet();
        }

        @Override
        public Object get(String attribute) {
            if (attribute == null) {
                return delegate.get(null);
            }

            if (delegate.containsKey(attribute)) {
                return delegate.get(attribute);
            }

            int dot = attribute.indexOf('.');

            if (dot == -1) {
                return delegate.get(attribute);
            } else {
                String firstPart = attribute.substring(0, dot);

                if (rootKeyNames().contains(firstPart)) {
                    return getAsNode(firstPart).get(attribute.substring(dot + 1));
                } else {
                    return null;
                }
            }
        }

        @Override
        public List<DocNode> getListOfNodes(String attribute) throws ConfigValidationException {
            if (delegate.containsKey(attribute)) {
                return delegate.getListOfNodes(attribute);
            }

            int dot = attribute.indexOf('.');

            if (dot == -1) {
                if (rootKeyNames().contains(attribute)) {
                    return getSubTree(attribute).getListOfNodes(null);
                } else {
                    return null;
                }
            } else {
                String firstPart = attribute.substring(0, dot);

                if (rootKeyNames().contains(firstPart)) {
                    return getAsNode(firstPart).getListOfNodes(attribute.substring(dot + 1));
                } else {
                    return null;
                }
            }
        }

        @Override
        public DocNode getAsNode(String attribute) {
            if (delegate.containsKey(attribute)) {
                return delegate.getAsNode(attribute);
            }

            int dot = attribute.indexOf('.');
                        
            if (dot == -1) {
                if (rootKeyNames().contains(attribute)) {
                    return getSubTree(attribute);
                } else {
                    return null;
                }
            } else {
                String firstPart = attribute.substring(0, dot);

                if (rootKeyNames().contains(firstPart)) {
                    return getSubTree(attribute).getAsNode(attribute.substring(dot + 1));
                } else {
                    return null;
                }
            }
        }

        @Override
        public Map<String, Object> toMap() throws ConfigValidationException {
            return delegate.toMap();
        }

        @Override
        public boolean isMap() {
            return delegate.isMap();
        }

        @Override
        public boolean isList() {
            return delegate.isList();
        }

        @Override
        public boolean isList(String attribute) {
            return get(attribute) instanceof List;
        }

        @Override
        public List<Object> toList() {
            return delegate.toList();
        }

        private Set<String> rootKeyNames() {
            if (this.rootKeyNames == null) {
                Set<String> rootKeyNames = new HashSet<>();

                for (String key : keySet()) {
                    int dot = key.indexOf('.');
                    if (dot == -1) {
                        rootKeyNames.add(key);
                    } else {
                        rootKeyNames.add(key.substring(0, dot));
                    }
                }

                this.rootKeyNames = rootKeyNames;
            }

            return this.rootKeyNames;
        }

        @Override
        protected DocNode createSubTree(String attribute) {
            return new AttributeNormalizingAdapter(delegate.getSubTree(attribute));
        }

    }

    protected String key;
    private Map<String, DocNode> subTreeCache = new HashMap<>();

    public abstract Object get(String attribute);

    public abstract List<DocNode> getListOfNodes(String attribute) throws ConfigValidationException;

    public abstract DocNode getAsNode(String attribute);

    public abstract Map<String, Object> toMap() throws ConfigValidationException;

    public abstract boolean isMap();

    public abstract boolean isList();

    public abstract boolean isList(String attribute);

    public abstract List<Object> toList();

    public Object get() {
        return get(null);
    }

    public boolean hasAny(String... keys) {
        for (String key : keys) {
            if (containsKey(key)) {
                return true;
            }
        }

        return false;
    }

    public DocNode without(String... attrs) {
        Set<String> attrsSet = new HashSet<>(Arrays.asList(attrs));

        LinkedHashMap<String, Object> newMap = new LinkedHashMap<>(size());

        for (Map.Entry<String, Object> entry : entrySet()) {
            if (!attrsSet.contains(entry.getKey())) {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }

        return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(newMap));
    }

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
                result.add(DocNode.wrap(subObject));
            }

            return result;
        } else if (object != null) {
            return Collections.singletonList(DocNode.wrap(object));
        } else {
            return Collections.emptyList();
        }
    }

    public <T> T findSingleValueByJsonPath(String jsonPath, Class<T> type) {
        return JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration()).parse(this).read(jsonPath, type);
    }

    public DocNode findSingleNodeByJsonPath(String jsonPath) {
        Object object = JsonPath.using(BasicJsonPathDefaultConfiguration.defaultConfiguration()).parse(this).read(jsonPath);

        if (object != null) {
            return DocNode.wrap(object);
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

    protected DocNode getSubTree(String attribute) {
        DocNode result = subTreeCache.get(attribute);

        if (result != null) {
            return result;
        }

        result = createSubTree(attribute);

        subTreeCache.put(attribute, result);

        return result;
    }

    protected DocNode createSubTree(String attribute) {
        return SubTreeView.getSubTree(this, attribute);
    }
}
