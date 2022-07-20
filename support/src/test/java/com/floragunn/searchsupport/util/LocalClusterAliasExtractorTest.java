package com.floragunn.searchsupport.util;

import org.elasticsearch.action.search.LocalClusterAliasAwareSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.junit.Assert;
import org.junit.Test;

public class LocalClusterAliasExtractorTest {

    @Test
    public void testExtractionNoLocalClusterAlias() {
        Assert.assertNull(LocalClusterAliasExtractor.getLocalClusterAliasFromSearchRequest(new SearchRequest()));
        Assert.assertNull(LocalClusterAliasExtractor.getLocalClusterAliasFromSearchRequest(new SearchRequest(new String[]{"xxx"})));

    }

    @Test
    public void testExtractionLocalClusterAlias() {
        SearchRequest searchRequest = LocalClusterAliasAwareSearchRequest.createSearchRequestWithClusterAlias(new SearchRequest(), "myalias", "a", "b");
        Assert.assertEquals("myalias", LocalClusterAliasExtractor.getLocalClusterAliasFromSearchRequest(searchRequest));
    }

    @Test
    public void testExtractionLocalClusterAlias2() {
        SearchRequest searchRequest = LocalClusterAliasAwareSearchRequest.createSearchRequestWithClusterAlias(new SearchRequest(new String[]{"xxx"}), "myalias1", "a", "b");
        Assert.assertEquals("myalias1", LocalClusterAliasExtractor.getLocalClusterAliasFromSearchRequest(searchRequest));
    }

    @Test
    public void testExtractionLocalClusterAlias3() {
        SearchRequest searchRequest = LocalClusterAliasAwareSearchRequest.createSearchRequestWithClusterAlias(new SearchRequest(new String[]{"xxx"}), "", "a", "b");
        Assert.assertEquals("", LocalClusterAliasExtractor.getLocalClusterAliasFromSearchRequest(searchRequest));
    }

}
