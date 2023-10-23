package com.floragunn.searchguard.test.helper.cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.common.bytes.BytesReference;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;

public class ResourceConfig<K> {

    private Map<K, Resource> map = new HashMap<>();
    private String baseDir;

    public ResourceConfig() {
        this("");
    }

    public ResourceConfig(String baseDir) {
        if (baseDir.length() == 0) {
            this.baseDir = "";
        } else if (baseDir.endsWith("/")) {
            this.baseDir = baseDir;
        } else {
            this.baseDir = baseDir + "/";
        }
    }

    public ResourceConfig(String baseDir, ResourceConfig<K> copy) {
        this(baseDir);

        for (Map.Entry<K, Resource> entry : copy.entrySet()) {
            if (entry.getValue().file == null) {
                this.map.put(entry.getKey(), entry.getValue());
            } else if (entry.getValue() instanceof Doc) {
                try {
                    this.map.put(entry.getKey(), new Doc(DocNode.parse(Format.YAML).from(entry.getValue().file), entry.getValue().file));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                this.map.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public Set<Entry<K, Resource>> entrySet() {
        return map.entrySet();

    }

    public BytesReference getAsBytesReference(K key) {
        Resource entry = map.get(key);

        if (entry == null) {
            throw new RuntimeException("No such resource with key " + key + "\nResources: " + map);
        }

        return entry.asBytesReference();

    }

    public ResourceConfig<K> setYamlDoc(K key, String yaml) {
        try {
            this.map.put(key, new Doc(DocNode.parse(Format.YAML).from(yaml), null));
            return this;
        } catch (DocumentParseException e) {
            throw new RuntimeException(e);
        }
    }

    public ResourceConfig<K> setYamlDoc(K key, File file) {
        try {
            this.map.put(key, new Doc(DocNode.parse(Format.YAML).from(file), file));
            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static abstract class Resource {
        final File file;

        Resource(File file) {
            this.file = file;
        }

        public abstract BytesReference asBytesReference();
    }

    public static class Doc extends Resource {
        private DocNode doc;
        
        Doc(DocNode doc, File file) {
            super(file);
            this.doc = doc;
        }

        public String asJsonString() {
            return doc.toJsonString();
        }

        public BytesReference asJsonBytesReference() {
            return BytesReference.fromByteBuffer(ByteBuffer.wrap(asJsonString().getBytes()));
        }

        public String asYamlString() {
            return doc.toYamlString();
        }

        public BytesReference asYamlBytesReference() {
            return BytesReference.fromByteBuffer(ByteBuffer.wrap(asJsonString().getBytes()));
        }

        @Override
        public BytesReference asBytesReference() {
            return asJsonBytesReference();
        }

    }

    private InputStream getStream(File file) throws IOException {

        InputStream inputStream = ResourceConfig.class.getResourceAsStream("/" + baseDir + file.getPath());

        if (inputStream == null) {
            throw new FileNotFoundException("Could not find resource in class path: " + file);
        }
        return inputStream;
    }

}
