/*
 * Copyright 2022-2026 floragunn GmbH
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

package org.elasticsearch.action.search;

import org.elasticsearch.tasks.TaskId;

public class LocalClusterAliasAwareSearchRequest extends SearchRequest {

    public static SearchRequest createSearchRequestWithClusterAlias(SearchRequest searchRequest, String clusterAlias, String... indices) {
        return subSearchRequest(new TaskId("dummy:1"), searchRequest, indices, searchRequest.indicesOptions(), clusterAlias, 0, false);
    }

}
