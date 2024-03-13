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

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;

public class TestIndex {

    private final String name;
    private final Settings settings;
    private final TestData testData;

    public TestIndex(String name, Settings settings, TestData testData) {
        this.name = name;
        this.settings = settings;
        this.testData = testData;
    }

    public void create(Client client) {
        if (!client.admin().indices().exists(new IndicesExistsRequest(name)).actionGet().isExists()) {
            testData.createIndex(client, name, settings);
        }
    }
    
    public void create(GenericRestClient client) throws Exception {
        GenericRestClient.HttpResponse response = client.head(name);
        
        if (response.getStatusCode() == 200) {
            return;
        }
        
        testData.createIndex(client, name, settings);        
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

    public static class Builder {
        private String name;
        private Settings.Builder settings = Settings.builder();
        private TestData.Builder testDataBuilder = new TestData.Builder();
        private TestData testData;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder setting(String name, int value) {
            settings.put(name, value);
            return this;
        }

        public Builder shards(int value) {
            settings.put("index.number_of_shards", 5);
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

        public Builder deletedDocumentCount(int deletedDocumentCount) {
            testDataBuilder.deletedDocumentCount(deletedDocumentCount);
            return this;
        }

        public Builder refreshAfter(int refreshAfter) {
            testDataBuilder.refreshAfter(refreshAfter);
            return this;
        }

        public Builder deletedDocumentFraction(double deletedDocumentFraction) {
            testDataBuilder.deletedDocumentFraction(deletedDocumentFraction);
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

        public TestIndex build() {
            if (testData == null) {
                testData = testDataBuilder.get();
            }

            return new TestIndex(name, settings.build(), testData);
        }
    }

}
