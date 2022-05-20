/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchsupport.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;

public class Actions {
    private static final Logger log = LogManager.getLogger(Actions.class);

    public static void clearScrollAsync(Client client, String scrollId) {
        if (scrollId == null) {
            return;
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {

            @Override
            public void onResponse(ClearScrollResponse response) {

            }

            @Override
            public void onFailure(Exception e) {
                log.warn(e);
            }
        });
    }

    public static void clearScrollAsync(Client client, SearchResponse searchResponse) {
        if (searchResponse == null) {
            return;
        }

        clearScrollAsync(client, searchResponse.getScrollId());
    }
}
