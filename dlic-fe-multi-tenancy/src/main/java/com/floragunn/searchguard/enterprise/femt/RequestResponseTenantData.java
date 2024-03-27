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
package com.floragunn.searchguard.enterprise.femt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.authz.config.Tenant;
import com.google.common.base.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RequestResponseTenantData {

    private final static Pattern INDEX_NAME_TENANT_PART = Pattern.compile("_(?<tenantName>-?\\d+_[^_]+)_.+");

    private final static String STORAGE_GLOBAL_TENANT_ID = TenantManager.toInternalTenantName(Tenant.GLOBAL_TENANT_ID);
    public static final String SG_TENANT_FIELD = "sg_tenant";
    private static final String TENANT_SEPARATOR_IN_ID = "__sg_ten__";
    private static final String TENANT_NAME_GROUP = "tenantName";

    private RequestResponseTenantData() {}

    public static String getSgTenantField() {
        return SG_TENANT_FIELD;
    }

    public static String scopedId(String id, String tenant) {
        return isStorageGlobal(tenant) ? id : id + TENANT_SEPARATOR_IN_ID + tenant;
    }

    private static boolean isStorageGlobal(String tenant) {
        return STORAGE_GLOBAL_TENANT_ID.equals(tenant);
    }

    public static String unscopedId(String id) {
        int i = id.indexOf(TENANT_SEPARATOR_IN_ID);

        if (i != -1) {
            return id.substring(0, i);
        } else {
            return id;
        }
    }

    public static String scopeIdIfNeeded(String id, String tenant) {
        if(id.contains(TENANT_SEPARATOR_IN_ID)) {
            return scopedId(unscopedId(id), tenant);
        }
        return scopedId(id, tenant);
    }

    public static boolean isScopedId(String id) {
        if(id == null) {
            return false;
        }
        return id.contains(TENANT_SEPARATOR_IN_ID) && (!id.endsWith(TENANT_SEPARATOR_IN_ID));
    }

    public static String extractTenantFromId(String id) {
        if(isScopedId(id)) {
            return id.substring(id.indexOf(TENANT_SEPARATOR_IN_ID) + TENANT_SEPARATOR_IN_ID.length());
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
        if((!Strings.isNullOrEmpty(tenant)) && (!isStorageGlobal(tenant))) {
            map.put(SG_TENANT_FIELD, tenant);
        }
    }

    public static BoolQueryBuilder sgTenantFieldQuery(String tenantStorageId) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if(isStorageGlobal(tenantStorageId)) {
            return queryBuilder.mustNot(QueryBuilders.existsQuery(SG_TENANT_FIELD));
        } else {
            return queryBuilder.minimumShouldMatch(1).should(QueryBuilders.termQuery(SG_TENANT_FIELD, tenantStorageId));
        }
    }

    public static Optional<String> scopedIdForPrivateTenantIndexName(String id, String indexName, String indexNamePrefix) {
        String indexNameWithoutPrefix = indexName.substring(indexNamePrefix.length());
        Matcher matcher = INDEX_NAME_TENANT_PART.matcher(indexNameWithoutPrefix);
        if(matcher.matches()) {
            String tenantNameExtractedFromIndexName = matcher.group(TENANT_NAME_GROUP);
            return Optional.of(scopedId(id, tenantNameExtractedFromIndexName));
        }
        return Optional.empty();
    }
}
