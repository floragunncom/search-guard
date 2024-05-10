/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

import java.util.List;
import java.util.Objects;

public class UpdateByQueryMapper implements Unscoper<BulkByScrollResponse> {

    private final BulkMapper bulkMapper;

    private final static Logger log = LogManager.getLogger(UpdateByQueryMapper.class);

    public UpdateByQueryMapper(BulkMapper bulkMapper) {
        this.bulkMapper = Objects.requireNonNull(bulkMapper, "bulkMapper is required");
    }

    public UpdateByQueryRequest toScopedUpdateByQueryRequest(UpdateByQueryRequest request, String tenant) {
        log.debug("Rewriting update by query request - adding tenant scope");

        BoolQueryBuilder queryBuilder = RequestResponseTenantData.sgTenantFieldQuery(tenant);

        SearchRequest searchRequest = request.getSearchRequest();
        if (searchRequest.source().query() != null) {
            queryBuilder.must(searchRequest.source().query());
        }

        request.setQuery(queryBuilder);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Query to indices '{}' was intercepted to limit access only to tenant '{}', extended query version '{}'",
                    String.join(", ", request.indices()),
                    tenant,
                    queryBuilder
            );
        }

        return request;
    }

    @Override
    public BulkByScrollResponse unscopeResponse(BulkByScrollResponse response) {
        log.debug("Rewriting bulk by scroll response - removing tenant scope");

        List<BulkItemResponse.Failure> unscopedFailures = response.getBulkFailures()
                .stream()
                .map(this.bulkMapper::toUnscopedFailure)
                .toList();
          return new BulkByScrollResponse(
                  response.getTook(), response.getStatus(), unscopedFailures,
                  response.getSearchFailures(), response.isTimedOut()
          );
    }

}
