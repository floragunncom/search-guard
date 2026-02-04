package org.elasticsearch.action.search;

import org.elasticsearch.tasks.TaskId;

public class LocalClusterAliasAwareSearchRequest extends SearchRequest {

    public static SearchRequest createSearchRequestWithClusterAlias(SearchRequest searchRequest, String clusterAlias, String... indices) {
        return subSearchRequest(new TaskId("dummy:1"), searchRequest, indices, searchRequest.indicesOptions(), clusterAlias, 0, false);
    }

}
