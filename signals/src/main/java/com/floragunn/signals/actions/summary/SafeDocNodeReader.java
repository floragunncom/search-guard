package com.floragunn.signals.actions.summary;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.jayway.jsonpath.PathNotFoundException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.rest.RestStatus;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

class SafeDocNodeReader {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    private SafeDocNodeReader(){

    }

    static Long getLongValue(DocNode docNode, String key) {
        try {
            if (docNode.containsKey(key)) {
                return docNode.getNumber(key).longValue();
            } else {
                return null;
            }
        } catch (ConfigValidationException e) {
            throw new ElasticsearchStatusException("Key " + key + " is not an numeric value.", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    static Integer getIntValue(DocNode docNode, String key) {
        try {
            if (docNode.containsKey(key)) {
                return docNode.getNumber(key).intValue();
            } else {
                return null;
            }
        } catch (ConfigValidationException e) {
            throw new ElasticsearchStatusException("Key " + key + " is not an numeric value.", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    static Double getDoubleValue(DocNode docNode, String key) {
        try {
            if (docNode.containsKey(key)) {
                return docNode.getNumber(key).doubleValue();
            } else {
                return null;
            }
        } catch (ConfigValidationException e) {
            throw new ElasticsearchStatusException("Key " + key + " is not an numeric value.", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    static Instant getInstantValue(DocNode node, String key) {
        return Optional.ofNullable(key)//
            .map(node::getAsString)//
            .map(DATE_FORMATTER::parse)//
            .map(Instant::from)//
            .orElse(null);
    }

    static Optional<DocNode> findSingleNodeByJsonPath(DocNode docNode, String jsonPath) {
        try {
            return Optional.ofNullable(docNode.findSingleNodeByJsonPath(jsonPath));
        } catch (PathNotFoundException ex) {
            return Optional.empty();
        }
    }
}
