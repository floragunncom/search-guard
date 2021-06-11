package com.floragunn.searchguard.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

public class TestData {
    private static final Logger log = LogManager.getLogger(TestData.class);

    public static int DEFAULT_SEED = 1234;
    public static int DEFAULT_DOCUMENT_COUNT = 300;

    public static TestData get() {
        return DEFAULT;
    }

    public static TestData.Builder documentCount(int documentCount) {
        return new Builder().documentCount(documentCount);
    }

    public static final TestData DEFAULT;

    private static final Cache<Key, TestData> cache;

    static {
        cache = CacheBuilder.newBuilder().softValues().initialCapacity(3).build();
        DEFAULT = documentCount(DEFAULT_DOCUMENT_COUNT).get();
    }

    private String[] ipAddresses;
    private String[] locationNames;
    private int size;
    private int deletedDocumentCount;
    private int refreshAfter;
    private Map<String, Map<String, ?>> allDocuments;
    private Map<String, Map<String, ?>> retainedDocuments;
    private Set<String> deletedDocuments;
    private long subRandomSeed;

    public TestData(int seed, int size, int deletedDocumentCount, int refreshAfter) {
        Random random = new Random(seed);
        this.ipAddresses = createRandomIpAddresses(random);
        this.locationNames = createRandomLocationNames(random);
        this.size = size;
        this.deletedDocumentCount = deletedDocumentCount;
        this.refreshAfter = refreshAfter;
        createTestDocuments(random);
        this.subRandomSeed = random.nextLong();
    }

    public void createIndex(Client client, String name, Settings settings) {
        log.info("creating test index " + name + "; size: " + size + "; deletedDocumentCount: " + deletedDocumentCount + "; refreshAfter: "
                + refreshAfter);
        
        Random random = new Random(subRandomSeed);
        long start = System.currentTimeMillis();

        client.admin().indices().create(new CreateIndexRequest(name).settings(settings)).actionGet();
        int nextRefresh = (int) Math.floor((random.nextGaussian() * 0.5 + 0.5) * refreshAfter);
        int i = 0;

        for (Map.Entry<String, Map<String, ?>> entry : allDocuments.entrySet()) {
            String id = entry.getKey();
            Map<String, ?> document = entry.getValue();

            client.index(new IndexRequest(name).source(document, XContentType.JSON).id(id)).actionGet();

            if (i > nextRefresh) {
                client.admin().indices().refresh(new RefreshRequest(name)).actionGet();
                double g = random.nextGaussian();

                nextRefresh = (int) Math.floor((g * 0.5 + 1) * refreshAfter) + i + 1;
                log.debug("refresh at " + i + " " + g + " " + (g * 0.5 + 1));
            }

            i++;
        }

        client.admin().indices().refresh(new RefreshRequest(name)).actionGet();

        for (String id : deletedDocuments) {
            client.delete(new DeleteRequest(name, id)).actionGet();
        }

        client.admin().indices().refresh(new RefreshRequest(name)).actionGet();
        log.info("Test index creation finished after " + (System.currentTimeMillis() - start) + " ms");
    }

    private void createTestDocuments(Random random) {

        Map<String, Map<String, ?>> allDocuments = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            Map<String, ?> document = ImmutableMap.of("source_ip", randomIpAddress(random), "dest_ip", randomIpAddress(random), "source_loc",
                    randomLocationName(random), "dest_loc", randomLocationName(random));

            String id = randomId(random);

            allDocuments.put(id, document);

        }

        List<String> createdDocIds = new ArrayList<>(allDocuments.keySet());

        Collections.shuffle(createdDocIds, random);

        Set<String> deletedDocuments = new HashSet<>(deletedDocumentCount);
        Map<String, Map<String, ?>> retainedDocuments = new HashMap<>(allDocuments);

        for (int i = 0; i < deletedDocumentCount; i++) {
            String id = createdDocIds.get(i);
            deletedDocuments.add(id);
            retainedDocuments.remove(id);
        }

        this.allDocuments = Collections.unmodifiableMap(allDocuments);
        this.retainedDocuments = Collections.unmodifiableMap(retainedDocuments);
        this.deletedDocuments = Collections.unmodifiableSet(deletedDocuments);
    }

    private String[] createRandomIpAddresses(Random random) {
        String[] result = new String[2000];

        for (int i = 0; i < result.length; i++) {
            result[i] = (random.nextInt(10) + 100) + "." + (random.nextInt(5) + 100) + "." + random.nextInt(255) + "." + random.nextInt(255);
        }

        return result;
    }

    private String[] createRandomLocationNames(Random random) {
        String[] p1 = new String[] { "Schön", "Schöner", "Tempel", "Friedens", "Friedrichs", "Blanken", "Rosen", "Charlotten", "Malch", "Lichten",
                "Lichter", "Hasel", "Kreuz", "Pank", "Marien", "Adlers", "Zehlen", "Haken", "Witten", "Jungfern", "Hellers", "Finster", "Birken",
                "Falken", "Freders", "Karls", "Grün", "Wilmers", "Heiners", "Lieben", "Marien", "Wiesen", "Biesen", "Schmachten", "Rahns", "Rangs",
                "Herms", "Rüders", "Wuster", "Hoppe" };
        String[] p2 = new String[] { "au", "ow", "berg", "feld", "felde", "tal", "thal", "höhe", "burg", "horst", "hausen", "dorf", "hof", "heide",
                "weide", "hain", "walde", "linde", "hagen", "eiche", "witz", "rade", "werder", "see", "fließ", "krug", "mark" };

        ArrayList<String> result = new ArrayList<>(p1.length * p2.length);

        for (int i = 0; i < p1.length; i++) {
            for (int k = 0; k < p2.length; k++) {
                result.add(p1[i] + p2[k]);
            }
        }

        Collections.shuffle(result, random);

        return result.toArray(new String[result.size()]);
    }

    private String randomIpAddress(Random random) {
        return ipAddresses[random.nextInt(ipAddresses.length)];
    }

    private String randomLocationName(Random random) {
        int i = (int) Math.floor(random.nextGaussian() * locationNames.length * 0.333 + locationNames.length);

        if (i < 0 || i >= locationNames.length) {
            i = random.nextInt(locationNames.length);
        }

        return locationNames[i];
    }

    private static String randomId(Random random) {
        UUID uuid = new UUID(random.nextLong(), random.nextLong());
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().encodeToString(byteBuffer.array()).replace("=", "");
    }

    public int getSize() {
        return size;
    }

    public int getDeletedDocumentCount() {
        return deletedDocumentCount;
    }

    public Map<String, Map<String, ?>> getAllDocuments() {
        return allDocuments;
    }

    public Map<String, Map<String, ?>> getRetainedDocuments() {
        return retainedDocuments;
    }

    public Set<String> getDeletedDocuments() {
        return deletedDocuments;
    }

    private static class Key {

        private final int seed;
        private final int size;
        private final int deletedDocumentCount;
        private final int refreshAfter;

        public Key(int seed, int size, int deletedDocumentCount, int refreshAfter) {
            super();
            this.seed = seed;
            this.size = size;
            this.deletedDocumentCount = deletedDocumentCount;
            this.refreshAfter = refreshAfter;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + deletedDocumentCount;
            result = prime * result + refreshAfter;
            result = prime * result + seed;
            result = prime * result + size;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Key other = (Key) obj;
            if (deletedDocumentCount != other.deletedDocumentCount) {
                return false;
            }
            if (refreshAfter != other.refreshAfter) {
                return false;
            }
            if (seed != other.seed) {
                return false;
            }
            if (size != other.size) {
                return false;
            }
            return true;
        }

    }

    public static class Builder {

        private int seed = DEFAULT_SEED;
        private int size = DEFAULT_DOCUMENT_COUNT;
        private int deletedDocumentCount = -1;
        private double deletedDocumentFraction = 0.06;
        private int refreshAfter = -1;
        private int segmentCount = 17;

        public Builder() {
            super();
        }

        public Builder seed(int seed) {
            this.seed = seed;
            return this;
        }

        public Builder documentCount(int size) {
            this.size = size;
            return this;
        }

        public Builder deletedDocumentCount(int deletedDocumentCount) {
            this.deletedDocumentCount = deletedDocumentCount;
            return this;
        }

        public Builder refreshAfter(int refreshAfter) {
            this.refreshAfter = refreshAfter;
            return this;
        }

        public Builder deletedDocumentFraction(double deletedDocumentFraction) {
            this.deletedDocumentFraction = deletedDocumentFraction;
            return this;
        }

        public Builder segmentCount(int segmentCount) {
            this.segmentCount = segmentCount;
            return this;
        }

        public Key toKey() {
            if (deletedDocumentCount == -1) {
                this.deletedDocumentCount = (int) (this.size * deletedDocumentFraction);
            }

            if (refreshAfter == -1) {
                this.refreshAfter = this.size / this.segmentCount;
            }

            return new Key(seed, size, deletedDocumentCount, refreshAfter);
        }

        public TestData get() {
            Key key = toKey();

            try {
                return cache.get(key, () -> new TestData(seed, size, deletedDocumentCount, refreshAfter));
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
