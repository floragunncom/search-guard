/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableMap;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class TestData {
    private static final Logger log = LogManager.getLogger(TestData.class);
    public static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";

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
    private String[] departments = new String[] { "dept_a_1", "dept_a_2", "dept_a_3", "dept_b_1", "dept_b_2", "dept_c", "dept_d" };
    private int size;
    private int deletedDocumentCount;
    private int refreshAfter;
    private int rolloverAfter;
    private Map<String, Map<String, ?>> allDocuments;
    private Map<String, Map<String, ?>> retainedDocuments;
    private Map<String, Map<String, Map<String, ?>>> documentsByDepartment;
    private ImmutableMap<String, Object> additionalAttributes;
    private Set<String> deletedDocuments;
    private long subRandomSeed;
    private final String timestampColumn;

    public TestData(int seed, int size, int deletedDocumentCount, int refreshAfter, int rolloverAfter,
                    ImmutableMap<String, Object> additionalAttributes, String timestampColumn, Map<String, Map<String, Object>> customDocuments) {
        Random random = new Random(seed);
        this.ipAddresses = createRandomIpAddresses(random);
        this.locationNames = createRandomLocationNames(random);
        this.size = size;
        this.deletedDocumentCount = deletedDocumentCount;
        this.refreshAfter = refreshAfter;
        this.rolloverAfter = rolloverAfter;
        this.additionalAttributes = additionalAttributes;
        this.timestampColumn = Objects.requireNonNull(timestampColumn, "Timestamp column name must not be null");
        createTestDocuments(random, customDocuments);
        this.subRandomSeed = random.nextLong();
    }

    private TestData(String[] ipAddresses, String[] locationNames, String[] departments, int size, int deletedDocumentCount, int refreshAfter,
            int rolloverAfter, Map<String, Map<String, ?>> allDocuments, Map<String, Map<String, ?>> retainedDocuments,
            Map<String, Map<String, Map<String, ?>>> documentsByDepartment, ImmutableMap<String, Object> additionalAttributes,
            Set<String> deletedDocuments, long subRandomSeed, String timestampColumn) {
        super();
        this.ipAddresses = ipAddresses;
        this.locationNames = locationNames;
        this.departments = departments;
        this.size = size;
        this.deletedDocumentCount = deletedDocumentCount;
        this.refreshAfter = refreshAfter;
        this.rolloverAfter = rolloverAfter;
        this.allDocuments = allDocuments;
        this.retainedDocuments = retainedDocuments;
        this.documentsByDepartment = documentsByDepartment;
        this.additionalAttributes = additionalAttributes;
        this.deletedDocuments = deletedDocuments;
        this.subRandomSeed = subRandomSeed;
        this.timestampColumn = timestampColumn;
    }

    public TestData withAdditionalDocument(String id, Map<String, Object> content) {
        return new TestData(ipAddresses, locationNames, departments, size, deletedDocumentCount, refreshAfter, rolloverAfter,
                ImmutableMap.of(allDocuments).with(id, content), ImmutableMap.of(retainedDocuments).with(id, content), documentsByDepartment,
                additionalAttributes, deletedDocuments, subRandomSeed, timestampColumn);
    }

    public String createIndex(Client client, String name, Settings settings) {
        log.info("creating test index " + name + "; size: " + size + "; deletedDocumentCount: " + deletedDocumentCount + "; refreshAfter: "
                + refreshAfter);

        Random random = new Random(subRandomSeed);
        long start = System.currentTimeMillis();

        client.admin().indices()
                .create(new CreateIndexRequest(name).settings(settings).simpleMapping(timestampColumn, "type=date,format=date_optional_time"))
                .actionGet();
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
        return getIndexMode(client, name);
    }

    public String createIndex(GenericRestClient client, String indexName, Settings settings) {
        try {
            log.info("creating test index " + indexName + "; size: " + size + "; deletedDocumentCount: " + deletedDocumentCount + "; refreshAfter: "
                    + refreshAfter);

            DocNode settingsDocNode = DocNode.EMPTY;

            for (String key : settings.keySet()) {
                settingsDocNode = settingsDocNode.with(key, settings.get(key));
            }

            GenericRestClient.HttpResponse response = client.putJson(indexName, DocNode.of("mappings.properties.timestamp",
                    DocNode.of("type", "date", "format", "date_optional_time"), "settings", settingsDocNode));

            if (response.getStatusCode() != 200) {
                throw new RuntimeException("Error while creating index " + indexName + "\n" + response);
            }

            putDocuments(client, indexName);
            return getIndexMode(client, indexName);

        } catch (Exception e) {
            throw new RuntimeException("Error while creating test index " + indexName, e);
        }
    }

    public void putDocuments(GenericRestClient client, String indexName) {
        try {
            Random random = new Random(subRandomSeed);
            long start = System.currentTimeMillis();

            int nextRefresh = (int) Math.floor((random.nextGaussian() * 0.5 + 0.5) * refreshAfter);
            int nextRollover = rolloverAfter != -1 ? rolloverAfter : Integer.MAX_VALUE;
            int i = 0;

            for (Map.Entry<String, Map<String, ?>> entry : allDocuments.entrySet()) {
                String id = entry.getKey();
                Map<String, ?> document = entry.getValue();

                GenericRestClient.HttpResponse response = client.putJson(indexName + "/_create/" + id, DocNode.wrap(document));

                if (response.getStatusCode() != 201) {
                    throw new RuntimeException("Error while creating document " + id + " in " + indexName + "\n" + response);
                }

                if (i > nextRefresh) {
                    client.post(indexName + "/_refresh");
                    double g = random.nextGaussian();

                    nextRefresh = (int) Math.floor((g * 0.5 + 1) * refreshAfter) + i + 1;
                    log.debug("refresh at " + i + " " + g + " " + (g * 0.5 + 1));
                }

                if (i > nextRollover) {
                    response = client.post(indexName + "/_rollover/");
                    if (response.getStatusCode() != 200) {
                        throw new RuntimeException("Error while performing rollover of " + indexName + "\n" + response);
                    }

                    nextRollover += rolloverAfter;
                }

                i++;
            }

            client.post(indexName + "/_refresh");

            for (String id : deletedDocuments) {
                client.delete(indexName + "/_doc/" + id);
            }

            client.post(indexName + "/_refresh");
            log.info("Test index creation finished after " + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
            throw new RuntimeException("Error while wring test documents to index " + indexName, e);
        }
    }

    public static String getIndexMode(Client client, String indexName) {
        return client.admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, "index.mode");
    }

    public static String getIndexMode(GenericRestClient client, String indexName) {
        try {
            GenericRestClient.HttpResponse response = client.get(indexName + "/_settings");
            return response.getBodyAsDocNode().getAsNode(indexName).getAsNode("settings").getAsNode("index").getAsString("mode");
        } catch (Exception e) {
            throw new RuntimeException("Error while getting index mode for " + indexName, e);
        }
    }

    private void createTestDocuments(Random random, Map<String, Map<String, Object>> customDocuments) {

        Map<String, Map<String, Object>> customDocumentsWithTestAttributes = addTestDocAttributesToCustomDocuments(random, customDocuments);

        Map<String, Map<String, ?>> allDocuments = new HashMap<>(size + customDocumentsWithTestAttributes.size());

        for (int i = 0; i < size; i++) {
            ImmutableMap<String, Object> document = randomDocument(random);

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

        allDocuments.putAll(customDocumentsWithTestAttributes);
        retainedDocuments.putAll(customDocumentsWithTestAttributes);

        Map<String, Map<String, Map<String, ?>>> documentsByDepartment = new HashMap<>();

        for (Map.Entry<String, Map<String, ?>> entry : retainedDocuments.entrySet()) {
            String dept = (String) entry.getValue().get("dept");
            documentsByDepartment.computeIfAbsent(dept, (k) -> new HashMap<>()).put(entry.getKey(), entry.getValue());
        }

        this.allDocuments = Collections.unmodifiableMap(allDocuments);
        this.retainedDocuments = Collections.unmodifiableMap(retainedDocuments);
        this.deletedDocuments = Collections.unmodifiableSet(deletedDocuments);
        this.documentsByDepartment = documentsByDepartment;
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

    private ImmutableMap<String, Object> randomDocument(Random random) {
        return ImmutableMap
                .<String, Object>of("source_ip", randomIpAddress(random), "dest_ip", randomIpAddress(random), "source_loc",
                        randomLocationName(random), "dest_loc", randomLocationName(random), "dept", randomDepartmentName(random))
                .with(timestampColumn, randomTimestamp(random)).with(Optional.ofNullable(additionalAttributes).orElse(ImmutableMap.empty()));
    }

    private Map<String, Map<String, Object>> addTestDocAttributesToCustomDocuments(Random random, Map<String, Map<String, Object>> customDocuments) {
        HashMap<String, Map<String, Object>> customDocsWithTestAttributes = new HashMap<>(customDocuments.size());
        customDocuments.forEach((customDocId, customDocSource) -> {
            ImmutableMap<String, Object> customDocWithTestAttributes = randomDocument(random).with(ImmutableMap.of(customDocSource));
            customDocsWithTestAttributes.put(customDocId, customDocWithTestAttributes);

        });
        return customDocsWithTestAttributes;
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

    private String randomDepartmentName(Random random) {
        return departments[random.nextInt(departments.length)];
    }

    private String randomTimestamp(Random random) {
        long epochMillis = random.longs(1, -2857691960709L, 2857691960709L).findFirst().getAsLong();
        return Instant.ofEpochMilli(epochMillis).toString();
    }

    private static String randomId(Random random) {
        UUID uuid = new UUID(random.nextLong(), random.nextLong());
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().encodeToString(byteBuffer.array()).replace("=", "");
    }

    public int getSize() {
        return size - deletedDocumentCount;
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

    public TestDocument anyDocument() {
        Map.Entry<String, Map<String, ?>> entry = retainedDocuments.entrySet().iterator().next();

        return new TestDocument(entry.getKey(), entry.getValue());
    }

    public TestDocument anyDocumentForDepartment(String dept) {
        Map<String, Map<String, ?>> docs = this.documentsByDepartment.get(dept);

        if (docs == null) {
            return null;
        }

        Map.Entry<String, Map<String, ?>> entry = docs.entrySet().iterator().next();

        return new TestDocument(entry.getKey(), entry.getValue());
    }

    private static class Key {

        private final int seed;
        private final int size;
        private final int deletedDocumentCount;
        private final int refreshAfter;
        private final ImmutableMap<String, Object> additionalAttributes;
        private final String timestampColumnName;
        private final ImmutableMap<String, Map<String, Object>> customDocuments;

        public Key(int seed, int size, int deletedDocumentCount, int refreshAfter, ImmutableMap<String, Object> additionalAttributes, String timestampColumnName, ImmutableMap<String, Map<String, Object>> customDocuments) {
            super();
            this.seed = seed;
            this.size = size;
            this.deletedDocumentCount = deletedDocumentCount;
            this.refreshAfter = refreshAfter;
            this.additionalAttributes = additionalAttributes;
            this.timestampColumnName = Objects.requireNonNull(timestampColumnName);
            this.customDocuments = customDocuments;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + deletedDocumentCount;
            result = prime * result + refreshAfter;
            result = prime * result + seed;
            result = prime * result + size;
            result = prime * result + additionalAttributes.hashCode();
            result = prime * result + timestampColumnName.hashCode();
            result = prime * result + customDocuments.hashCode();
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
            if (!additionalAttributes.equals(other.additionalAttributes)) {
                return false;
            }

            if (!timestampColumnName.equals(other.timestampColumnName)) {
                return false;
            }

            if (!customDocuments.equals(other.customDocuments)) {
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
        private int rolloverAfter = -1;
        private int segmentCount = 17;
        private Map<String, Object> additionalAttributes = new HashMap<>();
        private String timestampColumnName = DEFAULT_TIMESTAMP_COLUMN;
        private Map<String, Map<String, Object>> customDocuments = new HashMap<>();

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

        public Builder rolloverAfter(int rolloverAfter) {
            this.rolloverAfter = rolloverAfter;
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

        public Builder attr(String name, Object value) {
            additionalAttributes.put(name, value);
            return this;
        }

        public Builder timestampColumnName(String timestampColumnName) {
            this.timestampColumnName = timestampColumnName;
            return this;
        }

        public Builder customDocument(String id, Map<String, Object> source) {
            this.customDocuments.put(id, source);
            return this;
        }

        public Key toKey() {
            if (deletedDocumentCount == -1) {
                this.deletedDocumentCount = (int) (this.size * deletedDocumentFraction);
            }

            if (refreshAfter == -1) {
                this.refreshAfter = this.size / this.segmentCount;
            }

            return new Key(seed, size, deletedDocumentCount, refreshAfter, ImmutableMap.of(additionalAttributes), timestampColumnName, ImmutableMap.of(customDocuments));
        }

        public TestData get() {
            Key key = toKey();

            try {
                return cache.get(key, () -> new TestData(seed, size, deletedDocumentCount, refreshAfter, rolloverAfter, ImmutableMap.of(additionalAttributes), key.timestampColumnName, ImmutableMap.of(customDocuments)));
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class TestDocument {
        private final String id;
        private final Map<String, ?> content;

        TestDocument(String id, Map<String, ?> content) {
            this.id = id;
            this.content = content;
        }

        public String getId() {
            return id;
        }

        public Map<String, ?> getContent() {
            return content;
        }

        public String getUri(String index) {
            return "/" + index + "/_doc/" + id;
        }
    }

}
