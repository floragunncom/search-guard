/*
 * Copyright 2022 floragunn GmbH
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
