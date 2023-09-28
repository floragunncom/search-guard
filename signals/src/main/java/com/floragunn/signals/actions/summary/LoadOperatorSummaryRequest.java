package com.floragunn.signals.actions.summary;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.Action.Request;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;
import com.floragunn.signals.actions.summary.WatchFilter.Range;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getIntValue;

public class LoadOperatorSummaryRequest extends Request {
    public static final String FIELD_TENANT = "tenant";
    public static final String FIELD_SORTING = "sorting";
    public static final String FIELD_WATCH_STATUS_CODE = "watch_status_codes";
    public static final String FIELD_WATCH_ID = "watch_id";
    public static final String FIELD_SEVERITIES = "severities";
    public static final String FIELD_LEVEL_NUMERIC_EQUAL_TO = "level_numeric_equal_to";
    public static final String FIELD_LEVEL_NUMERIC_GREATER_THAN = "level_numeric_greater_than";
    public static final String FIELD_LEVEL_NUMERIC_LESS_THAN = "level_numeric_less_than";
    private final String tenant;// TODO field might be redundant
    private final String sorting;
    private final String watchId;
    private final List<String> watchStatusCodes;
    private final List<String> severities;
    private final Integer levelNumericEqualTo;
    private final Integer levelNumericGreaterThan;
    private final Integer levelNumericLessThan;

    public LoadOperatorSummaryRequest(UnparsedMessage message) throws ConfigValidationException {
        DocNode docNode = message.requiredDocNode();
        this.tenant = docNode.getAsString(FIELD_TENANT);
        this.sorting = docNode.getAsString(FIELD_SORTING);
        this.watchId = docNode.getAsString(FIELD_WATCH_ID);
        this.watchStatusCodes = Optional.<List<String>>ofNullable(docNode.getAsListOfStrings(FIELD_WATCH_STATUS_CODE))//
            .orElseGet(Collections::emptyList);
        this.severities = Optional.<List<String>>ofNullable(docNode.getAsListOfStrings(FIELD_SEVERITIES)).orElseGet(Collections::emptyList);
        this.levelNumericEqualTo = getIntValue(docNode, FIELD_LEVEL_NUMERIC_EQUAL_TO);
        this.levelNumericGreaterThan = getIntValue(docNode, FIELD_LEVEL_NUMERIC_GREATER_THAN);
        this.levelNumericLessThan = getIntValue(docNode, FIELD_LEVEL_NUMERIC_LESS_THAN);
    }

    private LoadOperatorSummaryRequest(String tenant, String sorting, DocNode requestBody) {
        this.tenant = tenant == null ? requestBody.getAsString(FIELD_TENANT) : tenant;
        this.sorting = sorting;
        this.watchStatusCodes = requestBody.getAsListOfStrings("status_codes");
        this.watchId = requestBody.getAsString("watch_id");
        this.severities = requestBody.getAsListOfStrings("severities");
        this.levelNumericEqualTo = getIntValue(requestBody, FIELD_LEVEL_NUMERIC_EQUAL_TO);
        this.levelNumericGreaterThan = getIntValue(requestBody, FIELD_LEVEL_NUMERIC_GREATER_THAN);
        this.levelNumericLessThan = getIntValue(requestBody, FIELD_LEVEL_NUMERIC_LESS_THAN);
        validateRange("level_numeric", levelNumericEqualTo, levelNumericGreaterThan, levelNumericLessThan);
    }

    public LoadOperatorSummaryRequest(String tenant, String sorting, UnparsedDocument<?> body) throws DocumentParseException {
        this(tenant, sorting, body.parseAsDocNode());
    }

    public String getSorting() {
        return sorting;
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.of(FIELD_TENANT, tenant, FIELD_SORTING, sorting, FIELD_WATCH_STATUS_CODE, watchStatusCodes,//
        FIELD_WATCH_ID, watchId)//
            .with(FIELD_SEVERITIES, severities) //
            .with(FIELD_LEVEL_NUMERIC_EQUAL_TO, levelNumericEqualTo)//
            .with(FIELD_LEVEL_NUMERIC_GREATER_THAN, levelNumericGreaterThan)//
            .with(FIELD_LEVEL_NUMERIC_LESS_THAN, levelNumericLessThan);
    }

    WatchFilter getWatchFilter() {
        Range<Integer> levelNumeric = rangeOrNull(levelNumericEqualTo, levelNumericGreaterThan, levelNumericLessThan);
        return new WatchFilter(watchId, watchStatusCodes, severities, levelNumeric);
    }

    private void validateRange(String rangeName, Number equal, Number greater, Number less) {
        if(Objects.nonNull(equal) && (Objects.nonNull(greater) || Objects.nonNull(less))) {
            String message = "Incorrect search criteria for " + rangeName + //
                ". If field equalTo is provided then both fields 'greaterThan' and 'lessThan' must be null";
            throw new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
        }
    }

    private <T> Range<T> rangeOrNull(T equalTo, T greaterThan, T lessThan) {
        if(Objects.nonNull(equalTo) || Objects.nonNull(greaterThan) || Objects.nonNull(lessThan)) {
            return new Range<>(equalTo, greaterThan, lessThan);
        } else {
            return null;
        }
    }
}
