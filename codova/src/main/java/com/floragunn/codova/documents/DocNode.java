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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.floragunn.codova.util.ValueRewritingMapWrapper;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;

public abstract class DocNode implements Map<String, Object>, Document<Object> {
    private static final Logger log = LoggerFactory.getLogger(DocNode.class);

    public static final DocNode EMPTY = new PlainJavaObjectAdapter(Collections.EMPTY_MAP);
    public static final DocNode NULL = new PlainJavaObjectAdapter(null);

    public static DocNodeParserBuilder parse(Format format) {
        return new DocNodeParserBuilder(format);
    }

    public static DocNodeParserBuilder parse(ContentType contentType) {
        return new DocNodeParserBuilder(contentType.getFormat());
    }

    public static DocNode parse(UnparsedDocument<?> unparsedDoc) throws DocumentParseException {
        return wrap(unparsedDoc.parse());
    }

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
        } else if (object instanceof Map) {
            return new AttributeNormalizingAdapter(new PlainJavaObjectAdapter(object));
        } else {
            return new PlainJavaObjectAdapter(object);
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
            return DocNode.wrap(get(attribute));
        }

        @Override
        public List<DocNode> getAsListOfNodes(String attribute) {
            Object object = null;

            if (attribute == null) {
                object = this.object;
            } else if (this.object instanceof Map) {
                object = ((Map<?, ?>) this.object).get(attribute);
            }

            if (object == null) {
                return Collections.emptyList();
            }

            if (object instanceof Collection) {
                return toListOfDocumentNode((Collection<?>) object);
            } else {
                return Collections.singletonList(wrap(object));
            }
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
        public Map<String, Object> toMap() {
            if (this.object == null) {
                return null;
            } else if (this.object instanceof Map) {
                return DocUtils.toStringKeyedMap((Map<?, ?>) this.object);
            } else {
                return Collections.singletonMap("_", this.object);
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
            return size() == 0;
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
            } else if (object != null) {
                return Collections.singletonList(toBaseType(object));
            } else {
                return Collections.emptyList();
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
            if (attribute == null) {
                return toMap();
            }

            if (simpleMap != null && simpleMap.containsKey(attribute)) {
                return simpleMap.get(attribute);
            } else {
                return delegate.get(viewAttributeWithDot + attribute);
            }
        }

        @Override
        public List<DocNode> getAsListOfNodes(String attribute) {
            if (simpleMap != null && simpleMap.containsKey(attribute)) {
                return simpleMap.getAsListOfNodes(attribute);
            } else {
                return delegate.getAsListOfNodes(viewAttributeWithDot + attribute);
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
        public Map<String, Object> toMap() {
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
            return Collections.singletonList(toMap());
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

            int dot = attribute.indexOf('.');

            if (dot == -1) {
                if (rootKeyNames().contains(attribute)) {
                    return getAsNode(attribute).toBasicObject();
                } else {
                    return null;
                }
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
        public List<DocNode> getAsListOfNodes(String attribute) {
            if (delegate.containsKey(attribute)) {
                return delegate.getAsListOfNodes(attribute);
            }

            int dot = attribute.indexOf('.');

            if (dot == -1) {
                if (rootKeyNames().contains(attribute)) {
                    return getSubTree(attribute).getAsListOfNodes(null);
                } else {
                    return Collections.emptyList();
                }
            } else {
                String firstPart = attribute.substring(0, dot);

                if (rootKeyNames().contains(firstPart)) {
                    return getAsNode(firstPart).getAsListOfNodes(attribute.substring(dot + 1));
                } else {
                    return Collections.emptyList();
                }
            }
        }

        @Override
        public DocNode getAsNode(String attribute) {
            int dot = attribute.indexOf('.');

            if (dot == -1) {
                if (rootKeyNames().contains(attribute)) {
                    return getSubTree(attribute);
                } else {
                    return DocNode.NULL;
                }
            } else {
                String firstPart = attribute.substring(0, dot);

                if (rootKeyNames().contains(firstPart)) {
                    return getSubTree(firstPart).getAsNode(attribute.substring(dot + 1));
                } else {
                    return DocNode.NULL;
                }
            }
        }

        @Override
        public Map<String, Object> toMap() {
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

        @Override
        public Map<String, Object> toNormalizedMap() {
            Map<String, Object> baseMap = toMap();

            if (baseMap == null) {
                return null;
            }

            boolean baseMapIsTree = baseMap.entrySet().stream().anyMatch(e -> e.getKey().indexOf('.') != -1 || e.getValue() instanceof Map);

            if (!baseMapIsTree) {
                return baseMap;
            }

            Set<String> rootKeyNames = rootKeyNames();

            Map<String, Object> result = new LinkedHashMap<>(rootKeyNames.size());

            for (String key : rootKeyNames) {
                DocNode subNode = getAsNode(key);

                if (subNode.isMap()) {
                    result.put(key, subNode.toNormalizedMap());
                } else {
                    result.put(key, subNode.toBasicObject());
                }
            }

            return result;
        }

        private Set<String> rootKeyNames() {
            if (this.rootKeyNames == null) {
                Set<String> rootKeyNames = new LinkedHashSet<>();

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
            DocNode subTree = delegate.getSubTree(attribute);

            if (subTree.isMap()) {
                return new AttributeNormalizingAdapter(subTree);
            } else {
                return subTree;
            }
        }

    }

    protected String key;
    private Map<String, DocNode> subTreeCache = new HashMap<>();

    public abstract Object get(String attribute);

    public abstract List<DocNode> getAsListOfNodes(String attribute);

    public abstract DocNode getAsNode(String attribute);

    public abstract Map<String, Object> toMap();

    public abstract boolean isMap();

    public abstract boolean isList();
    
    public abstract boolean isList(String attribute);

    public abstract List<Object> toList();
    
    public Object toBasicObject() {
        return get(null);
    }
    
    public Map<String, DocNode> toMapOfNodes() {
        return new ValueRewritingMapWrapper<>(toMap(), (o) -> DocNode.wrap(o));
    }

    public List<DocNode> toListOfNodes() {
        return getAsListOfNodes(null);
    }
    
    public boolean isNull() {
        return toBasicObject() == null;
    }
    
    public boolean isString() {
        return toBasicObject() instanceof String;
    }
    
    public boolean isNumber() {
        return toBasicObject() instanceof Number;
    }

    public boolean hasAny(String... keys) {
        for (String key : keys) {
            if (containsKey(key)) {
                return true;
            }
        }

        return false;
    }

    public DocNode with(Document<?> other) {
        DocNode otherDocNode;

        if (other instanceof DocNode) {
            otherDocNode = (DocNode) other;
        } else {
            otherDocNode = other.toDocNode();
        }

        if (otherDocNode.isEmpty()) {
            return this;
        }

        if (!otherDocNode.isMap() || !this.isMap()) {
            return otherDocNode;
        }

        Map<String, Object> newMap = new LinkedHashMap<>(this.toNormalizedMap());
        newMap.putAll(otherDocNode.toNormalizedMap());

        return new PlainJavaObjectAdapter(newMap);
    }

    public DocNode without(String... attrs) {
        Set<String> attrsSet = new HashSet<>(Arrays.asList(attrs));

        LinkedHashMap<String, Object> newMap = new LinkedHashMap<>(size());

        for (Map.Entry<String, Object> entry : entrySet()) {
            if (!attrsSet.contains(entry.getKey())) {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }

        return wrap(newMap);
    }

    public Object get(String attribute, String... attributePath) {
        DocNode docNode = getAsNode(attribute);

        for (int i = 0; i < attributePath.length - 1 && docNode != null; i++) {
            docNode = docNode.getAsNode(attributePath[i]);
        }

        if (docNode != null) {
            return docNode.get(attributePath[attributePath.length - 1]);
        } else {
            return null;
        }
    }

    public DocNode getAsNode(String attribute, String... moreAttributes) {
        DocNode docNode = getAsNode(attribute);

        for (int i = 0; i < moreAttributes.length && docNode != null && !docNode.isNull(); i++) {
            docNode = docNode.getAsNode(moreAttributes[i]);
        }

        return docNode;
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

        List<DocNode> nodeList = getAsListOfNodes(attribute);
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

        List<DocNode> nodeList = getAsListOfNodes(attribute);
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

    public <R> List<R> getAsListFromNodes(String attribute, ValidatingFunction<DocNode, R> conversionFunction) throws ConfigValidationException {
        return getAsListFromNodes(attribute, conversionFunction, null);
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

    public Map<String, Object> toNormalizedMap() {
        return toMap();
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

    public Number toNumber() throws ConfigValidationException {
        return getNumber(null);
    }

    @Override
    public String toString(Format format) {
        Object value = toBasicObject();

        if (value instanceof Map && format.equals(Format.JSON)) {
            return DocWriter.json().writeAsString(this.toNormalizedMap());
        } else {
            return DocWriter.format(format).writeAsString(value);
        }
    }

    @Override
    public String toJsonString() {
        return toString(Format.JSON);
    }

    @Override
    public String toYamlString() {
        return toString(Format.YAML);
    }

    @Override
    public String toString() {
        Object value = toBasicObject();

        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Number || value instanceof Boolean || value instanceof Character) {
            return value.toString();
        } else {
            return toString(60);
        }
    }

    protected String toString(int maxChars) {
        Object value = toBasicObject();

        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            StringBuilder result = new StringBuilder("{");
            int count = 0;

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (count != 0) {
                    result.append(", ");
                }

                if (result.length() >= maxChars) {
                    result.append(map.size() - count).append(" more ...");
                    break;
                }

                result.append(DocWriter.json().writeAsString(entry.getKey()));
                result.append(": ");
                result.append(DocNode.wrap(entry.getValue()).toString(maxChars / 2));
                count++;
            }

            result.append("}");
            return result.toString();
        } else if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;
            StringBuilder result = new StringBuilder("[");
            int count = 0;

            for (Object element : collection) {
                if (count != 0) {
                    result.append(", ");
                }

                if (result.length() >= maxChars) {
                    result.append(collection.size() - count).append(" more ...");
                    break;
                }

                result.append(DocNode.wrap(element).toString(maxChars / 2));
                count++;
            }

            result.append("]");
            return result.toString();
        } else {
            return DocWriter.json().writeAsString(value);
        }
    }

    public static class DocNodeParserBuilder {
        private final Format format;
        private final JsonFactory jsonFactory;

        private DocNodeParserBuilder(Format format) {
            this.format = format;
            this.jsonFactory = format.getJsonFactory();
        }

        public DocNode from(Reader in) throws DocumentParseException, IOException {
            try (JsonParser parser = jsonFactory.createParser(in)) {
                return wrap(new DocReader(format, parser).read());
            }
        }

        public DocNode from(String string) throws DocumentParseException {
            if (string == null || string.length() == 0) {
                throw new DocumentParseException(new ValidationError(null, "The document is empty").expected(format.getName() + " document"));
            }

            try (JsonParser parser = jsonFactory.createParser(string)) {
                return wrap(new DocReader(format, parser).read());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public DocNode from(byte[] bytes) throws DocumentParseException {
            if (bytes == null || bytes.length == 0) {
                throw new DocumentParseException(new ValidationError(null, "The document is empty").expected(format.getName() + " document"));
            }

            try {
                return from(new ByteArrayInputStream(bytes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public DocNode from(InputStream in) throws DocumentParseException, IOException {
            try (JsonParser parser = jsonFactory.createParser(in)) {
                return wrap(new DocReader(format, parser).read());
            }
        }

        public DocNode from(File file) throws DocumentParseException, FileNotFoundException, IOException {
            return from(new FileInputStream(file));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DocNode)) {
            return false;
        }

        DocNode other = (DocNode) obj;

        Object thisObject = this.toBasicObject();
        Object otherObject = other.toBasicObject();

        return Objects.equals(thisObject, otherObject);
    }
}
