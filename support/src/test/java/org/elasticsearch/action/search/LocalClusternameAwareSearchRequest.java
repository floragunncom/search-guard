package org.elasticsearch.action.search;

import org.elasticsearch.tasks.TaskId;

public class LocalClusternameAwareSearchRequest extends SearchRequest {

    public static SearchRequest create(SearchRequest searchRequest, String clusteralias, String... indices) {
        return subSearchRequest(new TaskId("dummy:1"), searchRequest, indices, clusteralias, 0, false);
    }

}
