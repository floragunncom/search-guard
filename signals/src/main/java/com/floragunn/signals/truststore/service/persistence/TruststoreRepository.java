/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.truststore.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.client.SearchScroller;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.IndexNames;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class TruststoreRepository {

    private final PrivilegedConfigClient client;
    private final SignalsSettings signalsSettings;

    public TruststoreRepository(SignalsSettings signalsSettings, PrivilegedConfigClient client) {
        this.signalsSettings = Objects.requireNonNull(signalsSettings, "Signals settings is required");
        this.client = Objects.requireNonNull(client, "Node client is required");
    }

    public DocWriteResponse createOrReplace(String truststoreId, TruststoreData truststoreData) {
        Objects.requireNonNull(truststoreId, "Truststore id is required");
        Objects.requireNonNull(truststoreData, "Truststore data are required");
        String json = truststoreData.toJsonString();
        IndexRequest storeRequest = new IndexRequest(IndexNames.TRUSTSTORES) //
            .setRefreshPolicy(IMMEDIATE) //
            .id(truststoreId) //
            .source(json, XContentType.JSON);
        return client.index(storeRequest).actionGet();
    }

    public List<TruststoreData> findAll() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.sort(TruststoreData.FIELD_STORE_TIME);
        TimeValue scrollTimeout = new TimeValue(1000);
        SearchRequest searchRequest = new SearchRequest(IndexNames.TRUSTSTORES).source(sourceBuilder);
        SearchScroller searchScroller = new SearchScroller(client);
        List<TruststoreData> results = new ArrayList<>();
        searchScroller.scroll(searchRequest, scrollTimeout, (searchHit) -> {
            try {
                return searchHitToTruststoreData(searchHit);
            } catch (ConfigValidationException e) {
                throw new RuntimeException("Cannot parse trust store content stored in an index.", e);
            }
        }, results::addAll);
        return results;
    }

    public Optional<TruststoreData> findOneById(String truststoreId)  {
        Objects.requireNonNull(truststoreId, "Trust store id is required");
        GetResponse getResponse = client.get(new GetRequest(IndexNames.TRUSTSTORES).id(truststoreId)).actionGet();
        try {
            return getResponseToTruststoreData(getResponse);
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Cannot parse trust store '" + truststoreId + "' content stored in an index.", e);
        }
    }

    private Optional<TruststoreData> getResponseToTruststoreData(GetResponse response) throws ConfigValidationException {
        return response.isExists() ? Optional.of(jsonToTruststoreData(response.getId(), response.getSourceAsString())) : Optional.empty();
    }

    private TruststoreData searchHitToTruststoreData(SearchHit searchHit) throws ConfigValidationException {
        return jsonToTruststoreData(searchHit.getId(), searchHit.getSourceAsString());
    }

    private TruststoreData  jsonToTruststoreData(String truststoreId, String json) throws ConfigValidationException {
        DocNode docNode = parseJsonAsDocNode(json);
        return new TruststoreData(truststoreId, docNode);

    }

    private static DocNode parseJsonAsDocNode(String json) throws DocumentParseException {
        return DocNode.parse(Format.JSON).from(json);
    }

    public Boolean deleteById(String truststoreId) {
        Objects.requireNonNull(truststoreId, "Truststore id is required");
        DeleteRequest deleteRequest = new DeleteRequest(IndexNames.TRUSTSTORES).id(truststoreId).setRefreshPolicy(IMMEDIATE);
        DeleteResponse deleteResponse = client.delete(deleteRequest).actionGet();
        return RestStatus.OK.equals(deleteResponse.status());
    }

    public boolean isTruststoreUsedByAnyWatch(String truststoreId) {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(
                QueryBuilders.boolQuery().must(
                        QueryBuilders.boolQuery()
                                .should(QueryBuilders.termQuery("actions.tls.truststore_id.keyword", truststoreId))
                                .should(QueryBuilders.termQuery("checks.tls.truststore_id.keyword", truststoreId))
                                .should(QueryBuilders.queryStringQuery(truststoreId).field("actions.attachments.*.tls.truststore_id.keyword"))
                )
        );
        SearchRequest searchWatchesRequest = new SearchRequest(signalsSettings.getStaticSettings().getIndexNames().getWatches())
                .source(searchSourceBuilder)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchResponse searchResponse = client.search(searchWatchesRequest).actionGet();
        try {
            return searchResponse.getHits().getTotalHits().value() > 0;
        } finally {
            searchResponse.decRef();
        }

    }
}
