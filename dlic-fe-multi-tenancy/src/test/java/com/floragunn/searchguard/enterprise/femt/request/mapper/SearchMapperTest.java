/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;

public class SearchMapperTest {

    @Test
    public void unscopeResponse_preservesHitDataAndSearchMetadata() {
        SortField sortField = new SortField("sort", SortField.Type.STRING);
        SearchHit originalHit = SearchHit.unpooled(7, RequestResponseTenantData.scopedId("document", "tenant"));
        originalHit.sourceRef(new BytesArray("{\"field\":\"value\"}"));
        SearchHits originalHits = new SearchHits(new SearchHit[] { originalHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f,
                new SortField[] { sortField }, "group", new Object[] { "group-value" });
        SearchResponse originalResponse = new SearchResponse(originalHits, null, null, false, false, null, 1, null, 1, 1, 0, 10,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
        originalHits.decRef();

        SearchResponse rewrittenResponse = new SearchMapper().unscopeResponse(originalResponse);
        originalResponse.decRef();

        try {
            SearchHit rewrittenHit = rewrittenResponse.getHits().getAt(0);
            assertThat(rewrittenHit.getId(), equalTo("document"));
            assertThat(rewrittenHit.getSourceAsString(), equalTo("{\"field\":\"value\"}"));
            assertThat(rewrittenResponse.getHits().getSortFields(), arrayContaining(sortField));
            assertThat(rewrittenResponse.getHits().getCollapseField(), equalTo("group"));
            assertThat(rewrittenResponse.getHits().getCollapseValues(), arrayContaining("group-value"));
        } finally {
            rewrittenResponse.decRef();
        }
    }
}
