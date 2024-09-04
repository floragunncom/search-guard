/*
 * Copyright 2024 floragunn GmbH
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
package com.floragunn.searchguard.test.helper;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.Objects;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;

public class PitHolder implements AutoCloseable {

    private final GenericRestClient genericRestClient;
    private final HttpResponse response;

    public static HolderClient of(GenericRestClient genericRestClient) {
        return new HolderClient(genericRestClient);
    }

    private PitHolder(GenericRestClient genericRestClient, HttpResponse response) {
        this.genericRestClient = genericRestClient;
        this.response = response;
    }

    public String getPitId() {
        assertThat(response, isOk());
        try {
            return response.getBodyAsDocNode().getAsString("id");
        } catch (DocumentParseException | Format.UnknownDocTypeException e) {
            throw new RuntimeException("Cannot retrieve PIT id", e);
        }
    }

    public DocNode asSearchBody() {
        assertThat(response, isOk());
        try {
            return DocNode.of("pit.id", response.getBodyAsDocNode().getAsString("id"));
        } catch (DocumentParseException | Format.UnknownDocTypeException e) {
            throw new RuntimeException("Cannot retrieve PIT id", e);
        }
    }

    public String[] extractIndicesFromPit(NamedWriteableRegistry namedWriteableRegistry) {
        Objects.requireNonNull(namedWriteableRegistry, "Name writeable registry cannot be null");
        SearchContextId searchContextId = SearchContextId.decode(namedWriteableRegistry, getPitId());
        return searchContextId.getActualIndices();
    }

    @Override
    public void close() throws Exception {
        if (response.getStatusCode() == SC_OK) {
            HttpResponse response = genericRestClient.deleteJson("/_pit/", DocNode.of("id", getPitId()));
            assertThat(response, isOk());
        }
    }

    public HttpResponse getResponse() {
        return response;
    }

    public static class HolderClient {

        private final GenericRestClient genericRestClient;

        public HolderClient(GenericRestClient genericRestClient) {
            this.genericRestClient = Objects.requireNonNull(genericRestClient, "Rest client is required");
        }

        public PitHolder post(String path) throws Exception {
            HttpResponse response = genericRestClient.post(path);
            return new PitHolder(genericRestClient, response);
        }
    }
}
