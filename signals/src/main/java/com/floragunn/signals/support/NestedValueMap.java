package com.floragunn.signals.support;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.google.common.collect.MapMaker;

public class NestedValueMap extends HashMap<String, Object> {

    private static final long serialVersionUID = 2953312818482932741L;

    private Map<Object, Object> originalToCloneMap;
    private final boolean cloneWhilePut;
    private boolean writable = true;

    public NestedValueMap() {
        originalToCloneMap = new MapMaker().weakKeys().makeMap();
        cloneWhilePut = true;
    }

    public NestedValueMap(int initialCapacity) {
        super(initialCapacity);
        originalToCloneMap = new MapMaker().weakKeys().makeMap();
        cloneWhilePut = true;
    }

    NestedValueMap(Map<Object, Object> originalToCloneMap, boolean cloneWhilePut) {
        this.originalToCloneMap = originalToCloneMap;
        this.cloneWhilePut = cloneWhilePut;
    }

    NestedValueMap(int initialCapacity, Map<Object, Object> originalToCloneMap, boolean cloneWhilePut) {
        super(initialCapacity);
        this.originalToCloneMap = originalToCloneMap;
        this.cloneWhilePut = cloneWhilePut;
    }

    @Override
    public NestedValueMap clone() {
        NestedValueMap result = new NestedValueMap(Math.max(this.size(), 10), cloneWhilePut ? new MapMaker().weakKeys().makeMap() : null,
                this.cloneWhilePut);

        result.putAll(this);

        return result;
    }

    public NestedValueMap without(String... keys) {
        NestedValueMap result = new NestedValueMap(Math.max(this.size(), 10), cloneWhilePut ? new MapMaker().weakKeys().makeMap() : null,
                this.cloneWhilePut);

        Set<String> withoutKeySet = new HashSet<>(Arrays.asList(keys));

        for (Map.Entry<String, Object> entry : this.entrySet()) {
            if (!withoutKeySet.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }

    public static NestedValueMap copy(Map<?, ?> data) {
        NestedValueMap result = new NestedValueMap(data.size());

        result.putAllFromAnyMap(data);

        return result;
    }

    public static NestedValueMap copy(Object data) {
        if (data instanceof Map) {
            return copy((Map<?, ?>) data);
        } else {
            NestedValueMap result = new NestedValueMap();
            result.put("_value", data);
            return result;
        }
    }

    public static NestedValueMap createNonCloningMap() {
        return new NestedValueMap(null, false);
    }

    public static NestedValueMap createUnmodifieableMap(Map<?, ?> data) {
        NestedValueMap result = new NestedValueMap(data.size());

        result.putAllFromAnyMap(data);
        result.seal();

        return result;
    }

    public static NestedValueMap fromJsonString(String jsonString) throws IOException {
        return NestedValueMap.copy(DefaultObjectMapper.readValue(jsonString, Map.class));
    }

    public static NestedValueMap fromJsonArrayString(String jsonString) throws IOException {
        return NestedValueMap.copy(DefaultObjectMapper.readValue(jsonString, List.class));
    }

    public Object put(String key, Map<?, ?> data) {
        checkWritable();

        Object result = this.get(key);
        NestedValueMap subMap = this.getOrCreateSubMapAt(key, data.size());

        subMap.putAllFromAnyMap(data);
        return result;
    }

    public void putAll(Map<? extends String, ? extends Object> map) {
        checkWritable();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = String.valueOf(entry.getKey());
            put(key, entry.getValue());
        }
    }

    public void putAllFromAnyMap(Map<?, ?> map) {
        checkWritable();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = String.valueOf(entry.getKey());
            put(key, entry.getValue());
        }
    }

    public Object put(String key, Object object) {
        checkWritable();

        if (object instanceof Map) {
            return put(key, (Map<?, ?>) object);
        }

        return super.put(key, deepCloneObject(object));
    }

    public void put(Path path, Object object) {
        checkWritable();

        if (path.isEmpty()) {
            if (path instanceof Map) {
                putAllFromAnyMap((Map<?, ?>) object);
            } else {
                throw new IllegalArgumentException("put([], " + object + "): If an empty path is given, the object must be of type map");
            }

        } else {
            NestedValueMap subMap = getOrCreateSubMapAtPath(path.withoutLast());
            subMap.put(path.getLast(), object);
        }
    }

    public Object get(Path path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.length() == 1) {
            return this.get(path.getFirst());
        } else {
            Object subObject = this.get(path.getFirst());

            if (subObject instanceof NestedValueMap) {
                return ((NestedValueMap) subObject).get(path.withoutFirst());
            } else {
                return null;
            }
        }
    }

    public void seal() {
        if (!this.writable) {
            return;
        }

        this.writable = false;
        this.originalToCloneMap = null;

        for (Object value : this.values()) {
            if (value instanceof NestedValueMap) {
                NestedValueMap subMap = (NestedValueMap) value;
                subMap.seal();
            } else if (value instanceof Iterable) {
                for (Object subValue : ((Iterable<?>) value)) {
                    if (subValue instanceof NestedValueMap) {
                        NestedValueMap subMap = (NestedValueMap) subValue;
                        subMap.seal();
                    }
                }
            }
        }
    }

    public String toJsonString() {
        return DocWriter.writeAsString(this);
    }

    private Object deepCloneObject(Object object) {
        if (!cloneWhilePut || object == null || isImmutable(object)) {
            return object;
        }

        Object clone = this.originalToCloneMap.get(object);

        if (clone != null) {
            return clone;
        }

        if (object instanceof Set) {
            Set<?> set = (Set<?>) object;
            Set<Object> copy = new HashSet<>(set.size());
            this.originalToCloneMap.put(object, copy);

            for (Object element : set) {
                copy.add(deepCloneObject(element));
            }

            return copy;
        } else if (object instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) object;
            NestedValueMap copy = new NestedValueMap(map.size(), this.originalToCloneMap, this.cloneWhilePut);
            this.originalToCloneMap.put(object, copy);

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                copy.put((String) deepCloneObject(String.valueOf(entry.getKey())), deepCloneObject(entry.getValue()));
            }

            return copy;
        } else if (object instanceof Collection) {
            Collection<?> collection = (Collection<?>) object;
            ArrayList<Object> copy = new ArrayList<>(collection.size());
            this.originalToCloneMap.put(object, copy);

            for (Object element : collection) {
                copy.add(deepCloneObject(element));
            }

            return copy;
        } else if (object.getClass().isArray()) {
            int length = Array.getLength(object);
            Object copy = Array.newInstance(object.getClass().getComponentType(), length);
            this.originalToCloneMap.put(object, copy);

            for (int i = 0; i < length; i++) {
                Array.set(copy, i, deepCloneObject(Array.get(object, i)));
            }

            return copy;
        } else {
            // Hope the best

            return object;
        }
    }

    private boolean isImmutable(Object object) {
        return object instanceof String || object instanceof Number || object instanceof Boolean || object instanceof Void || object instanceof Class
                || object instanceof Character || object instanceof Enum || object instanceof File || object instanceof UUID || object instanceof URL
                || object instanceof URI;
    }

    private NestedValueMap getOrCreateSubMapAt(String key, int capacity) {
        Object value = this.get(key);

        if (value instanceof NestedValueMap) {
            return (NestedValueMap) value;
        } else {
            if (value instanceof Map) {
                capacity = Math.max(capacity, ((Map<?, ?>) value).size());
            }

            NestedValueMap mapValue = new NestedValueMap(capacity, this.originalToCloneMap, this.cloneWhilePut);

            if (value instanceof Map) {
                mapValue.putAllFromAnyMap((Map<?, ?>) value);
            }

            super.put(key, mapValue);
            return mapValue;
        }

    }

    private NestedValueMap getOrCreateSubMapAtPath(Path path) {
        if (path.isEmpty()) {
            return this;
        }

        String pathElement = path.getFirst();
        Path remainingPath = path.withoutFirst();

        Object value = this.get(pathElement);

        if (value instanceof NestedValueMap) {
            NestedValueMap mapValue = (NestedValueMap) value;
            if (remainingPath.isEmpty()) {
                return mapValue;
            } else {
                return mapValue.getOrCreateSubMapAtPath(remainingPath);
            }
        } else {
            NestedValueMap mapValue = new NestedValueMap(this.originalToCloneMap, this.cloneWhilePut);
            super.put(pathElement, mapValue);

            if (remainingPath.isEmpty()) {
                return mapValue;
            } else {
                return mapValue.getOrCreateSubMapAtPath(remainingPath);
            }
        }
    }

    private void checkWritable() {
        if (!writable) {
            throw new UnsupportedOperationException("Map is not writable");
        }
    }

    public static class Path {
        private String[] elements;
        private int start;
        private int end;

        public Path(String... elements) {
            this.elements = elements;
            this.start = 0;
            this.end = elements.length;
        }

        private Path(String[] elements, int start, int end) {
            this.elements = elements;
            this.start = start;
            this.end = end;
        }

        public String getFirst() {
            if (this.start >= this.end) {
                return null;
            }

            return this.elements[start];
        }

        public String getLast() {
            if (this.start >= this.end) {
                return null;
            }

            return this.elements[end - 1];
        }

        public Path withoutFirst() {
            if (this.start >= this.end - 1) {
                return new Path(null, 0, 0);
            }

            return new Path(elements, start + 1, end);
        }

        public Path withoutLast() {
            if (this.start >= this.end - 1) {
                return new Path(null, 0, 0);
            }

            return new Path(elements, start, end - 1);
        }

        public int length() {
            return this.end - this.start;
        }

        public boolean isEmpty() {
            return this.start == this.end;
        }

        public static Path parse(String path) {
            return new Path(path.split("\\."));
        }
    }

}
