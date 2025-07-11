/*
 * Copyright 2021-2024 floragunn GmbH
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

import com.floragunn.codova.documents.DocNode;

import java.util.Map;
import java.util.Set;

public class TestDataStream implements TestIndexLike {

    private final String name;
    private final TestData testData;

    public TestDataStream(String name, TestData testData) {
        this.name = name;
        this.testData = testData;
    }

    @Override
    public DocNode getFieldsMappings() {
        return testData.getFieldsMappings();
    }

    public void create(GenericRestClient client) throws Exception {
        GenericRestClient.HttpResponse response = client.head(name);
        if (response.getStatusCode() == 200) {
            return;
        }

        response = client.put("/_data_stream/" + name);
        if (response.getStatusCode() != 200 && response.getStatusCode() != 201) {
            throw new RuntimeException("Error while creating data stream " + name + "\n" + response);
        }
        DocNode mappings = testData.getFieldMappingsWithoutTimestamp();
        if (! mappings.isEmpty()) {
            DocNode mappingRequestBody = DocNode.of("properties", mappings);
            GenericRestClient.HttpResponse mappingsResponse = client.putJson("/" + name + "/_mapping", mappingRequestBody);
            if (mappingsResponse.getStatusCode() != 200) {
                throw new RuntimeException(
                        "Cannot update mappings for data stream '" + name + "' response code '" + mappingsResponse.getStatusCode() + "' and body '" + mappingsResponse.getBody() + "'");
            }
        }

        testData.putDocuments(client, name);
    }

    public String getName() {
        return name;
    }

    public TestData getTestData() {
        return testData;
    }

    public static Builder name(String name) {
        return new Builder().name(name);
    }

    @Override
    public String toString() {
        return "Test data stream '" + name + '\'';
    }

    public static class Builder {
        private String name;
        private TestData.Builder testDataBuilder = new TestData.Builder().timestampColumnName("@timestamp").deletedDocumentFraction(0);
        private TestData testData;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder data(TestData data) {
            this.testData = data;
            return this;
        }

        public Builder seed(int seed) {
            testDataBuilder.seed(seed);
            return this;
        }

        public Builder documentCount(int size) {
            testDataBuilder.documentCount(size);
            return this;
        }

        public Builder refreshAfter(int refreshAfter) {
            testDataBuilder.refreshAfter(refreshAfter);
            return this;
        }

        public Builder rolloverAfter(int rolloverAfter) {
            testDataBuilder.rolloverAfter(rolloverAfter);
            return this;
        }

        public Builder segmentCount(int segmentCount) {
            testDataBuilder.segmentCount(segmentCount);
            return this;
        }

        public Builder attr(String name, Object value) {
            testDataBuilder.attr(name, value);
            return this;
        }

        public TestDataStream build() {
            if (testData == null) {
                testData = testDataBuilder.get();
            }

            return new TestDataStream(name, testData);
        }
    }

    @Override
    public Set<String> getDocumentIds() {
        return getTestData().getRetainedDocuments().keySet();
    }

    @Override
    public Map<String, Map<String, ?>> getDocuments() {
        return getTestData().getRetainedDocuments();
    }

}
