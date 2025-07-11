package com.floragunn.signals.actions.summary;

import static com.floragunn.signals.actions.summary.LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX;
import static java.util.Objects.requireNonNull;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.signals.actions.summary.SortParser.SortByField;
import com.floragunn.signals.actions.summary.WatchFilter.Range;

import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

class WatchStateRepository {
    private final static Logger log = LogManager.getLogger(WatchStateRepository.class);
    private static final String PROP = "properties";
    private static final String FIELDS = "fields";
    private final String stateIndexName;
    private final PrivilegedConfigClient privilegedConfigClient;

    public WatchStateRepository(String stateIndexName, PrivilegedConfigClient privilegedConfigClient) {
        this.stateIndexName = requireNonNull(stateIndexName);
        this.privilegedConfigClient = requireNonNull(privilegedConfigClient);
    }

    public SearchResponse search(WatchFilter watchFilter,  List<SortByField> sorting, int size, Collection<String> watchIdFilter) {
        requireNonNull(watchFilter, "Watch filter is required");
        requireNonNull(sorting, "Sorting is required");
        GetMappingsResponse mappings = getMappings();
        DocNode docNode = extractFieldNames(mappings);
        return searchWithFilteringOutMissingSortingFields(watchFilter, sorting, size, docNode, watchIdFilter);
    }

    public SearchResponse findNeverExecutedWatchesWithSeverity(String tenant, Collection<String> watchIds, int size) {
        requireNonNull(tenant, "Tenant is required");
        requireNonNull(watchIds, "Watch ids are required");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(size);
        sourceBuilder.query(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("_tenant", tenant))
                .filter(QueryBuilders.idsQuery().addIds(watchIds.toArray(new String[0])))
                .mustNot(QueryBuilders.existsQuery("last_execution")));

        SearchRequest request = new SearchRequest(stateIndexName).source(sourceBuilder);

        return privilegedConfigClient.search(request).actionGet();
    }

    private SearchResponse searchWithFilteringOutMissingSortingFields(WatchFilter watchFilter, List<SortByField> sorting,
        int size, DocNode fieldsDefinedInMappings, Collection<String> watchIdFilter) {

        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        for(SortByField field : sorting) {
            // filter out fields non-existing in fields mapping
            // https://git.floragunn.com/search-guard/search-guard-suite-enterprise/-/issues/241
            if (!checkPropertyExists(fieldsDefinedInMappings, field.getDocumentFieldName())){
                continue;
            }
            SortOrder order = field.isAscending() ? SortOrder.ASC : SortOrder.DESC;
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort(field.getDocumentFieldName()).order(order);
            sourceBuilder.sort(sortBuilder);
        }
        sourceBuilder.size(size);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        buildWatchIdsQuery(watchIdFilter, boolQueryBuilder);
        buildStatusCodeQuery(watchFilter, boolQueryBuilder);
        buildSeverityQuery(watchFilter, boolQueryBuilder);
        buildActionsNamesQuery(watchFilter, boolQueryBuilder);
        buildActionsPropertiesQuery(watchFilter, boolQueryBuilder);
        buildRangeQueries(watchFilter, boolQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        SearchRequest request = new SearchRequest(stateIndexName).source(sourceBuilder);
        log.debug("Request used by operator view to load watch state '{}'", request);
        return privilegedConfigClient.search(request).actionGet();
    }

    private static void buildWatchIdsQuery(Collection<String> watchIdFilter, BoolQueryBuilder boolQueryBuilder) {
        if(watchIdFilter != null) {
            boolQueryBuilder.filter(QueryBuilders.idsQuery().addIds(watchIdFilter.toArray(new String[0])));
        }
    }

    private static void buildRangeQueries(WatchFilter watchFilter, BoolQueryBuilder boolQueryBuilder) {
        RangesFilters ranges = watchFilter.getRangesFilters();
        if (ranges.getLevelNumericRange() != null) {
            buildRangeOrTermQuery(boolQueryBuilder, ranges.getLevelNumericRange());
        }
        if (ranges.getActionsCheckedRange() != null) {
            buildRangeOrTermQuery(boolQueryBuilder, ranges.getActionsCheckedRange());
        }
        if (ranges.getActionsTriggeredRange() != null) {
            buildRangeOrTermQuery(boolQueryBuilder, ranges.getActionsTriggeredRange());
        }
        if (ranges.getActionsExecutionRange() != null) {
            buildRangeOrTermQuery(boolQueryBuilder, ranges.getActionsExecutionRange());
        }
    }

    private static void buildActionsPropertiesQuery(WatchFilter watchFilter, BoolQueryBuilder boolQueryBuilder) {
        ActionProperties prop = watchFilter.getActionProperties();
        if (prop.getCheckResultValue() != null) {
            boolQueryBuilder.must(QueryBuilders.termQuery(prop.getCheckResultName(), prop.getCheckResultValue()));
        }
        if (prop.getErrorValue() != null) {
            boolQueryBuilder.must(QueryBuilders.matchQuery(prop.getErrorName(), prop.getErrorValue()));
        }
        if (prop.getStatusCodeValue() != null) {
            boolQueryBuilder.must(QueryBuilders.matchQuery(prop.getStatusCodeName(), prop.getStatusCodeValue()));
        }
        if (prop.getStatusDetailsValue() != null) {
            boolQueryBuilder.must(QueryBuilders.matchQuery(prop.getStatusDetailsName(), prop.getStatusDetailsValue()));
        }
    }


    private static void buildActionsNamesQuery(WatchFilter watchFilter, BoolQueryBuilder boolQueryBuilder) {
        if(watchFilter.containsActions()) {
            BoolQueryBuilder orQueryBuilder = QueryBuilders.boolQuery();
            for(String action : watchFilter.getActionNames()) {
                ExistsQueryBuilder existsQuery = QueryBuilders.existsQuery(ACTIONS_PREFIX + action);
                orQueryBuilder.should(existsQuery);
            }
            boolQueryBuilder.filter(orQueryBuilder);
        }
    }

    private static void buildRangeOrTermQuery(BoolQueryBuilder boolQueryBuilder, Range<?> range) {
        QueryBuilder rangeQuery;
        String field = range.getFieldName();
        if(range.containsEqualTo()) {
            rangeQuery = QueryBuilders.termQuery(field, range.getEqualTo());
        } else {
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);
            if(range.containsLessThan()) {
                rangeQueryBuilder = rangeQueryBuilder.lt(range.getLessThan());
            }
            if(range.containsGreaterThan()) {
                rangeQueryBuilder = rangeQueryBuilder.gt(range.getGreaterThan());
            }
            rangeQuery = rangeQueryBuilder;
        }
        boolQueryBuilder.must(rangeQuery);
    }

    private static void buildSeverityQuery(WatchFilter watchFilter, BoolQueryBuilder boolQueryBuilder) {
        if(watchFilter.containsSeverities()) {
            BoolQueryBuilder orQueryBuilder = QueryBuilders.boolQuery();
            for(String severity : watchFilter.getSeverities()) {
                TermQueryBuilder query = QueryBuilders.termQuery("last_status.severity", severity);
                orQueryBuilder.should(query);
            }
            boolQueryBuilder.must(orQueryBuilder);
        }
    }

    private static void buildStatusCodeQuery(WatchFilter watchFilter, BoolQueryBuilder boolQueryBuilder) {
        if(watchFilter.containsWatchStatusFilter()) {
            BoolQueryBuilder orQueryBuilder = QueryBuilders.boolQuery();
            for(String status : watchFilter.getWatchStatusCodes()) {
                TermQueryBuilder query = QueryBuilders.termQuery(SummaryToWatchFieldMapper.getSearchFieldName("status_code"), status);
                orQueryBuilder.should(query);
            }
            boolQueryBuilder.must(orQueryBuilder);
        }
    }

    private GetMappingsResponse getMappings() {
        ActionFuture<GetMappingsResponse> mappings =
            privilegedConfigClient.admin().indices().getMappings(new GetMappingsRequest().indices(stateIndexName));
        return mappings.actionGet();
    }

    private DocNode extractFieldNames(GetMappingsResponse response) {
        try {

            return DocNode.wrap(response.mappings().get(stateIndexName).getSourceAsMap().get(PROP));
        } catch (NullPointerException npe) {
            return DocNode.EMPTY;
        }
    }

    private boolean checkPropertyExists(DocNode node, String field) {
        if (node.isEmpty()) {
            return false;
        }
        String[] path = field.split("\\.");
        if ("actions".equals(path[0])) {
            if (path.length == 3) {
                return node.getAsNode(path[0], PROP, path[1], PROP).containsKey(path[2]);
            } else {
                return node.getAsNode(path[0], PROP, path[1], PROP, path[2], PROP, path[3], FIELDS)
                    .containsKey(path[4]);
            }
        } else {
            return node.getAsNode(path[0], PROP, path[1], FIELDS).containsKey(path[2])
                || node.getAsNode(path[0], PROP, path[1], PROP).containsKey(path[2]);
        }
    }
}
