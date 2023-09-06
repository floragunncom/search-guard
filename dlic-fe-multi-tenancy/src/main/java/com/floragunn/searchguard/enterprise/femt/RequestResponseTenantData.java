package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class RequestResponseTenantData {

    private static final String SG_TENANT_FIELD = "sg_tenant";
    private static final String TENAND_SEPARATOR_IN_ID = "__sg_ten__";

    private RequestResponseTenantData() {}

    public static String getSgTenantField() {
        return SG_TENANT_FIELD;
    }
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

    public static boolean isScopedId(String id) {
        if(id == null) {
            return false;
        }
        return id.contains(TENAND_SEPARATOR_IN_ID) && (!id.endsWith(TENAND_SEPARATOR_IN_ID));
    }

    public static String extractTenantFromId(String id) {
        if(isScopedId(id)) {
            return id.substring(id.indexOf(TENAND_SEPARATOR_IN_ID) + TENAND_SEPARATOR_IN_ID.length());
        } else {
            return null;
        }
    }

    public static boolean containsSgTenantField(Map<String, Object> map) {
        return map.containsKey(SG_TENANT_FIELD);
    }

    public static boolean containsSgTenantField(DocNode docNode) {
        return docNode.hasNonNull(SG_TENANT_FIELD);
    }

    public static void appendSgTenantFieldTo(Map<String, Object> map, String tenant) {
        map.put(SG_TENANT_FIELD, tenant);
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
