/*
 * Copyright 2024 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData.SG_TENANT_FIELD;

public class TenantRepository {

    private static final Logger log = LogManager.getLogger(TenantRepository.class);
    public static final String AGGREGATION_NAME = "documents_per_tenant";
    public static final String MAIN_FRONTEND_INDEX_ALIAS = ".kibana";
    public static final String[] FRONTEND_MULTI_TENANCY_ALIASES =
        { MAIN_FRONTEND_INDEX_ALIAS, ".kibana_analytics", ".kibana_ingest", ".kibana_security_solution", ".kibana_alerting_cases" };

    private final PrivilegedConfigClient client;

    public TenantRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Config client is required");
    }

    public ImmutableSet<String> exists(String... names) {
        Objects.requireNonNull(names, "Tenant names are required");
        if(names.length == 0) {
            return ImmutableSet.empty();
        }
        Set<String> existingTenants = findExistingNonGlobalTenants(names);
        if (ImmutableSet.ofArray(names).contains(Tenant.GLOBAL_TENANT_ID)) {
            if(checkIfGlobalTenantExists()) {
                existingTenants.add(Tenant.GLOBAL_TENANT_ID);
            }
        }
        return ImmutableSet.of(existingTenants);
    }

    private boolean checkIfGlobalTenantExists() {
        try {
            GetRequest request = new GetRequest(MAIN_FRONTEND_INDEX_ALIAS, "space:default");
            GetResponse globalTenantDefaultSpaceResponse = client.get(request).actionGet();
            return globalTenantDefaultSpaceResponse.isExists();
        } catch (IndexNotFoundException ex) {
            log.debug("Main front-end index does not exist", ex);
            return false;
        }
    }

    private Set<String> findExistingNonGlobalTenants(String[] names) {
        Map<String, String> internalNameToNameMap = new HashMap<>();
        for (String currentTenant : names) {
            internalNameToNameMap.put(TenantManager.toInternalTenantName(currentTenant), currentTenant);
        }
        SearchRequest searchRequest = buildTenantsExistQuery(internalNameToNameMap);
        SearchResponse response = client.search(searchRequest).actionGet();
        if (!RestStatus.OK.equals(response.status())) {
            log.error("Unexpected error occurred during loading information of available tenant, search response '{}'", response);
            throw new RuntimeException("Cannot retrieve information about existing frontend tenants");
        }
        StringTerms aggregation = Optional.ofNullable(response.getAggregations()) //
            .map(aggregations -> aggregations.get(AGGREGATION_NAME)) //
            .filter(StringTerms.class::isInstance) //
            .map(StringTerms.class::cast) //
            .orElse(null);
        Set<String> existingTenants = new HashSet<>();
        if (Objects.nonNull(aggregation)) {
            for (Bucket bucket : aggregation.getBuckets()) {
                String internalTenantName = bucket.getKeyAsString();
                long docCount = bucket.getDocCount();
                if (!internalNameToNameMap.containsKey(internalTenantName)) {
                    throw new RuntimeException("Unexpected internal tenant name '" + internalTenantName + "'");
                }
                if (docCount > 0) {
                    existingTenants.add(internalNameToNameMap.get(internalTenantName));
                }
            }
        }
        return existingTenants;
    }

    void extendTenantsIndexMappings(DocNode mappings) {
        mappings = mappings.hasNonNull("properties")? mappings : DocNode.of("properties", mappings);
        PutMappingRequest putMappingRequest = new PutMappingRequest(FRONTEND_MULTI_TENANCY_ALIASES)
                .source(mappings);

        client.admin().indices().putMapping(putMappingRequest)
                .actionGet();
    }

    private static SearchRequest buildTenantsExistQuery(Map<String, String> internalNameToNameMap) {
        SearchRequest searchRequest = new SearchRequest(FRONTEND_MULTI_TENANCY_ALIASES);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        SearchSourceBuilder sources = SearchSourceBuilder.searchSource() //
            .size(0) //
            .query(QueryBuilders.termsQuery("sg_tenant", internalNameToNameMap.keySet().toArray(String[]::new))) //
            .aggregation(AggregationBuilders.terms(AGGREGATION_NAME).size(10_000).field(SG_TENANT_FIELD));
        searchRequest.source(sources);
        return searchRequest;
    }
}
