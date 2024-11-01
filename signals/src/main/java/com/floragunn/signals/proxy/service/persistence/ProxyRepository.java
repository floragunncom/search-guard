/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.signals.proxy.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class ProxyRepository {

    private final SignalsSettings signalsSettings;
    private final PrivilegedConfigClient client;

    public ProxyRepository(SignalsSettings signalsSettings, PrivilegedConfigClient client) {
        this.signalsSettings = Objects.requireNonNull(signalsSettings, "signals settings is required");
        this.client = Objects.requireNonNull(client, "Node client is required");
    }

    public boolean isProxyUsedByAnyWatch(String proxyId) {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(
                QueryBuilders.boolQuery().must(
                        QueryBuilders.boolQuery()
                                .should(QueryBuilders.termQuery("actions.proxy.keyword", proxyId))
                                .should(QueryBuilders.termQuery("checks.proxy.keyword", proxyId))
                                .should(QueryBuilders.queryStringQuery(proxyId).field("actions.attachments.*.proxy.keyword"))
                )
        );
        SearchRequest searchWatchesRequest = new SearchRequest(signalsSettings.getStaticSettings().getIndexNames().getWatches())
                .source(searchSourceBuilder)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchResponse searchResponse = client.search(searchWatchesRequest).actionGet();
        return searchResponse.getHits().getTotalHits().value > 0;

    }

    public DocWriteResponse createOrReplace(ProxyData proxyData) {
        Objects.requireNonNull(proxyData, "Proxy data is required");
        String proxyId = proxyData.getId();
        String json = proxyData.toJsonString();
        IndexRequest storeRequest = new IndexRequest(IndexNames.PROXIES) //
                .setRefreshPolicy(IMMEDIATE) //
                .id(proxyId) //
                .source(json, XContentType.JSON);
        return client.index(storeRequest).actionGet();
    }

    public Optional<ProxyData> findOneById(String proxyId) {
        Objects.requireNonNull(proxyId, "Proxy id is required");
        GetResponse getResponse = client.get(new GetRequest(IndexNames.PROXIES).id(proxyId)).actionGet();
        try {
            return getResponseToProxyData(getResponse);
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Cannot parse proxy '" + proxyId + "' content stored in an index.", e);
        }
    }

    public Boolean deleteById(String proxyId) {
        Objects.requireNonNull(proxyId, "Proxy id is required");
        DeleteRequest deleteRequest = new DeleteRequest(IndexNames.PROXIES).id(proxyId).setRefreshPolicy(IMMEDIATE);
        DeleteResponse deleteResponse = client.delete(deleteRequest).actionGet();
        return deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
    }

    public List<ProxyData> findAll() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .sort(ProxyData.FIELD_STORE_TIME, SortOrder.DESC);
        TimeValue scrollTimeout = new TimeValue(1000);
        SearchRequest searchRequest = new SearchRequest(IndexNames.PROXIES).source(sourceBuilder);
        SearchScroller searchScroller = new SearchScroller(client);
        List<ProxyData> results = new ArrayList<>();
        searchScroller.scroll(searchRequest, scrollTimeout, (searchHit) -> {
            try {
                return searchHitToProxyData(searchHit);
            } catch (ConfigValidationException e) {
                throw new RuntimeException("Cannot parse proxy '" + searchHit.getId() + "' content stored in an index.", e);
            }
        }, results::addAll);
        return results;
    }

    private ProxyData searchHitToProxyData(SearchHit searchHit) throws ConfigValidationException {
        return jsonToProxyData(searchHit.getId(), searchHit.getSourceAsString());
    }

    private Optional<ProxyData> getResponseToProxyData(GetResponse response) throws ConfigValidationException {
        return response.isExists() ? Optional.of(jsonToProxyData(response.getId(), response.getSourceAsString())) : Optional.empty();
    }

    private ProxyData  jsonToProxyData(String proxyId, String json) throws ConfigValidationException {
        DocNode docNode = DocNode.parse(Format.JSON).from(json);
        return new ProxyData(proxyId, docNode);
    }

}
