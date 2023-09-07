package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetMapper {

    private final static Logger log = LogManager.getLogger(GetMapper.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    public GetMapper(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    public SearchRequest toScopedSearchRequest(GetRequest request, String tenant) {
        log.debug("Rewriting get request to search request - adding tenant scope");
        SearchRequest searchRequest = new SearchRequest(request.indices());
        BoolQueryBuilder query = RequestResponseTenantData.sgTenantIdsQuery(tenant, request.id())
                .must(RequestResponseTenantData.sgTenantFieldQuery(tenant));
        searchRequest.source(SearchSourceBuilder.searchSource().query(query).version(true).seqNoAndPrimaryTerm(true));
        return searchRequest;
    }

    /**
     *
     * @param response
     * @param getRequest the GetRequest that was mapped to SearchRequest in order to retrieve {@code response}
     */
    public GetResponse toUnscopedGetResponse(SearchResponse response, GetRequest getRequest) throws MappingException {
        log.debug("Rewriting search response to get response - removing tenant scope");
        long hits = response.getHits().getTotalHits().value;
        if (hits == 1) {
            return new GetResponse(searchHitToGetResult(response.getHits().getAt(0)));
        } else if (hits == 0) {
            return new GetResponse(new GetResult(getRequest.index(), RequestResponseTenantData.unscopedId(getRequest.id()),
                    SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null));
        } else {
            log.error("Rewriting search response to get response - unexpected number of hits: {}", hits);
            throw new MappingException("Unexpected number of hits");
        }
    }

    private GetResult searchHitToGetResult(SearchHit hit) {

        log.debug("Converting SearchHit: {} to GetResult", hit);

        Map<String, DocumentField> fields = hit.getFields();
        Map<String, DocumentField> documentFields;
        Map<String, DocumentField> metadataFields;

        if (fields.isEmpty()) {
            documentFields = Collections.emptyMap();
            metadataFields = Collections.emptyMap();
        } else {
            IndexMetadata indexMetadata = clusterService.state().getMetadata().indices().get(hit.getIndex());
            IndexService indexService = indexMetadata != null ? indicesService.indexService(indexMetadata.getIndex()) : null;

            if (indexService != null) {
                documentFields = new HashMap<>(fields.size());
                metadataFields = new HashMap<>();
                MapperService mapperService = indexService.mapperService();

                for (Map.Entry<String, DocumentField> entry : fields.entrySet()) {
                    if (mapperService.isMetadataField(entry.getKey())) {
                        metadataFields.put(entry.getKey(), entry.getValue());
                    } else {
                        documentFields.put(entry.getKey(), entry.getValue());
                    }
                }

                log.debug("Partitioned fields: {}; {}", metadataFields, documentFields);
            } else {
                log.warn("""
                        Could not find IndexService for {}; assuming all fields as document fields. \
                        This should not happen, however this should also not pose a big problem as ES mixes the fields again anyway.
                        IndexMetadata: {}
                        """, hit.getIndex(), indexMetadata);
                documentFields = fields;
                metadataFields = Collections.emptyMap();
            }
        }

        return new GetResult(hit.getIndex(), RequestResponseTenantData.unscopedId(hit.getId()), hit.getSeqNo(), hit.getPrimaryTerm(), hit.getVersion(), true,
                hit.getSourceRef(), documentFields, metadataFields);
    }

}
