package com.floragunn.searchguard.enterprise.femt.request;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class RequestTenantData {

    protected static final String SG_TENANT_FIELD = "sg_tenant";
    private static final String TENAND_SEPARATOR_IN_ID = "__sg_ten__";

    public static String scopedId(String id, String tenant) {
        return id + TENAND_SEPARATOR_IN_ID + tenant;
    }

    public static String unscopedId(String id) {
        int i = id.indexOf(TENAND_SEPARATOR_IN_ID);

        if (i != -1) {
            return id.substring(0, i);
        } else {
            return id;
        }
    }

    public static String scopeIdIfNeeded(String id, String tenant) {
        if(id.contains(TENAND_SEPARATOR_IN_ID)) {
            return scopedId(unscopedId(id), tenant);
        }
        return scopedId(id, tenant);
    }

    public static void appendSgTenantFieldToSource(Map<String, Object> source, String tenant) {
        source.put(SG_TENANT_FIELD, tenant);
    }

    public static BoolQueryBuilder sgTenantIdsQuery(String tenant, String... ids) {
        List<String> scopedIds = Stream.of(ids).map(id -> scopeIdIfNeeded(id, tenant)).toList();
        return QueryBuilders.boolQuery().must(QueryBuilders.idsQuery().addIds(scopedIds.toArray(new String[] {})));
    }

    public static BoolQueryBuilder sgTenantFieldQuery(String tenant) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);
        // TODO better tenant id
        return queryBuilder.should(QueryBuilders.termQuery(SG_TENANT_FIELD, tenant));
    }

}
