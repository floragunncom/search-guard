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
import java.util.regex.Pattern;

class SafeDocNodeReader {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    private SafeDocNodeReader(){

    }

    static Long getLongValue(DocNode docNode, String key) {
        try {
            if (docNode.containsKey(key)) {
                Number number = docNode.getNumber(key);
                if (number == null) { // node exists but its value is null
                    return null;
                }
                return number.longValue();
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
                Number number = docNode.getNumber(key);
                if (number == null) { // node exists but its value is null
                    return null;
                }
                return number.intValue();
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
                Number number = docNode.getNumber(key);
                if (number == null) { // node exists but its value is null
                    return null;
                }
                return number.doubleValue();
            } else {
                return null;
            }
        } catch (ConfigValidationException e) {
            throw new ElasticsearchStatusException("Key " + key + " is not an numeric value.", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    static String getStringValue(DocNode node, String key) {
        return Optional.ofNullable(key)//
            .map(node::getAsString)//
            .orElse(null);
    }

    static Boolean getBooleanValue(DocNode node, String key) {
        return Optional.ofNullable(key)//
            .map(node::getAsString)//
            .map(Boolean::valueOf)
            .orElse(null);
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

    static String getActionNameByPatterns(DocNode node, Pattern pattern1, Pattern pattern2) {
        String actionNameByPattern1 = getActionNameByPattern(node, pattern1);
        if (actionNameByPattern1 != null) {
            return actionNameByPattern1;
        }
        return getActionNameByPattern(node, pattern2);
    }

    static String getActionNameByPattern(DocNode node, Pattern pattern) {
        return Optional.ofNullable(pattern)
            .map(p -> p.matcher(node.toJsonString()))
            .map(matcher -> matcher.find() ? matcher.group() : null)
            .map(s -> s.split("\\.")[1])
            .orElse(null);
    }
}
