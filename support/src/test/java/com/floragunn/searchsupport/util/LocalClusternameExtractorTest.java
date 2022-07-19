package com.floragunn.searchsupport.util;

import org.elasticsearch.action.search.LocalClusternameAwareSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.junit.Assert;
import org.junit.Test;

public class LocalClusternameExtractorTest {

    @Test
    public void testExtractionNoLocalClusterName() {
        Assert.assertNull(LocalClusternameExtractor.getLocalClusterAliasFromSearchRequest(new SearchRequest()));
        Assert.assertNull(LocalClusternameExtractor.getLocalClusterAliasFromSearchRequest(new SearchRequest(new String[]{"xxx"})));

    }

    @Test
    public void testExtractionLocalClusterName() {
        SearchRequest searchRequest = LocalClusternameAwareSearchRequest.create(new SearchRequest(), "myalias", "a", "b");
        Assert.assertEquals("myalias", LocalClusternameExtractor.getLocalClusterAliasFromSearchRequest(searchRequest));
    }

    @Test
    public void testExtractionLocalClusterName2() {
        SearchRequest searchRequest = LocalClusternameAwareSearchRequest.create(new SearchRequest(new String[]{"xxx"}), "myalias1", "a", "b");
        Assert.assertEquals("myalias1", LocalClusternameExtractor.getLocalClusterAliasFromSearchRequest(searchRequest));
    }

    @Test
    public void testExtractionLocalClusterName3() {
        SearchRequest searchRequest = LocalClusternameAwareSearchRequest.create(new SearchRequest(new String[]{"xxx"}), "", "a", "b");
        Assert.assertEquals("", LocalClusternameExtractor.getLocalClusterAliasFromSearchRequest(searchRequest));
    }

}
