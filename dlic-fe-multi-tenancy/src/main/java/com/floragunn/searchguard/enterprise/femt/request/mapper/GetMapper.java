package com.floragunn.searchguard.enterprise.femt.request.mapper;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.enterprise.femt.RequestResponseTenantData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GetMapper implements Unscoper<GetResponse> {

    private final static Logger log = LogManager.getLogger(GetMapper.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    public GetMapper(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService is required");
        this.indicesService = Objects.requireNonNull(indicesService, "indicesService is required");
    }

    public GetRequest toScopedGetRequest(GetRequest request, String tenant) {
        request.id(RequestResponseTenantData.scopedId(request.id(), tenant));
        return request;
    }

    @Override
    public GetResponse unscopeResponse(GetResponse response) {
        log.debug("Rewriting get response - removing tenant scope");
        PartitionedFields partitionedFields = partitionFieldsIntoDocAndMetadataFields(response);
        GetResult getResult = new GetResult(
                response.getIndex(),
                RequestResponseTenantData.unscopedId(response.getId()),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                response.getVersion(),
                response.isExists(),
                response.getSourceAsBytesRef(),
                partitionedFields.documentFields(),
                partitionedFields.metadataFields()
        );
        return new GetResponse(getResult);
    }

    private PartitionedFields partitionFieldsIntoDocAndMetadataFields(GetResponse getResponse) {
        Map<String, DocumentField> fields = getResponse.getFields();
        Map<String, DocumentField> documentFields;
        Map<String, DocumentField> metadataFields;

        if (fields.isEmpty()) {
            return PartitionedFields.empty();
        } else {
            IndexMetadata indexMetadata = clusterService.state().getMetadata().getProject().indices().get(getResponse.getIndex());
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

                if (log.isDebugEnabled()) {
                    log.debug("Partitioned fields: " + metadataFields + "; " + documentFields);
                }

            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Could not find IndexService for " + getResponse.getIndex() + "; assuming all fields as document fields."
                            + "This should not happen, however this should also not pose a big problem as ES mixes the fields again anyway.\n"
                            + "IndexMetadata: " + indexMetadata);
                }

                documentFields = fields;
                metadataFields = Collections.emptyMap();
            }
            return new PartitionedFields(documentFields, metadataFields);
        }
    }

    private record PartitionedFields(Map<String, DocumentField> documentFields, Map<String, DocumentField> metadataFields) {

        static PartitionedFields empty() {
            return new PartitionedFields(ImmutableMap.empty(), ImmutableMap.empty());
        }

    }

}
