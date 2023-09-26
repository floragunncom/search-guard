package com.floragunn.searchsupport.client;

import com.floragunn.fluent.collections.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.A;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Implementation based on com.floragunn.searchsupport.jobs.core.IndexJobStateStore#loadTriggerStates(java.util.Map)
 */
public class SearchScroller {

    private static final Logger log = LogManager.getLogger(SearchScroller.class);

    private final Client client;

    public SearchScroller(Client client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    /**
     * Load all search result via scrolling
     * @param request search request, please set correct page size
     * @param resultMapper maps json string loaded from document to java object
     * @return all search results
     * @param <T> type of search result
     */
    public  <T> ImmutableList<T> scrollAndLoadAll(SearchRequest request, Function<SearchHit, T> resultMapper) {
        Objects.requireNonNull(request, "Search request is required to scroll");
        Objects.requireNonNull(resultMapper, "Scroll result mapper is required");
        request.scroll(new TimeValue(10000));
        List<T> mutableResultList = new ArrayList<>();
        try {
            SearchResponse searchResponse = client.search(request).actionGet();
            try {
                do {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    String scrollId = searchResponse.getScrollId();
                    log.debug("'{}' elements were gained due to scrolling with id '{}'.", hits.length, scrollId);
                    for (SearchHit searchHit : hits) {
                        try {
                            mutableResultList.add(resultMapper.apply(searchHit));
                        } catch (Exception e) {
                            log.error("Error while loading scroll result " + searchHit, e);
                        }
                    }
                    searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()) //
                        .setScroll(new TimeValue(10000)) //
                        .execute() //
                        .actionGet();
                } while (searchResponse.getHits().getHits().length != 0);
            } finally {
                log.debug("Async clear scroll '{}'.", searchResponse.getScrollId());
                Actions.clearScrollAsync(client, searchResponse);
            }
            return ImmutableList.of(mutableResultList);
        } catch (ElasticsearchException e) {
            log.error("Error during scrolling to load all search results", e);
            throw e;
        }
    }

    public <T>  void scroll(SearchRequest request, TimeValue scrollTime,
        Function<SearchHit, T> resultMapper,
        Consumer<ImmutableList<T>> resultConsumer) {
        Objects.requireNonNull(request, "Search request is required to scroll");
        Objects.requireNonNull(resultMapper, "Scroll result mapper is required");
        request.scroll(scrollTime);
        try {
            SearchResponse searchResponse = client.search(request).actionGet();
            try {
                do {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    String scrollId = searchResponse.getScrollId();
                    if(searchResponse.getFailedShards() > 0) {
                        throw new IllegalStateException("Unexpected error during scrolling via search results.");
                    }
                    log.debug("'{}' elements were gained due to scrolling with id '{}'.", hits.length, scrollId);
                    ArrayList<T> mutableResultList = new ArrayList<>();
                    for (SearchHit searchHit : hits) {
                        mutableResultList.add(resultMapper.apply(searchHit));
                    }
                    resultConsumer.accept(ImmutableList.of(mutableResultList));
                    searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()) //
                        .setScroll(new TimeValue(10000)) // TODO add parameter
                        .execute() //
                        .actionGet();
                } while (searchResponse.getHits().getHits().length != 0);
            } finally {
                log.debug("Async clear scroll '{}'.", searchResponse.getScrollId());
                Actions.clearScrollAsync(client, searchResponse);
            }
        } catch (ElasticsearchException e) {
            log.error("Error during scrolling to load all search results", e);
            throw e;
        }
    }

}
