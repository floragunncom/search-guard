package com.floragunn.signals.actions.summary;

import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getActionNameByPattern;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getActionNameByPatterns;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getBooleanValue;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getInstantValue;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getIntValue;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getStringValue;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.Action.Request;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;
import com.floragunn.signals.actions.summary.WatchFilter.Range;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.floragunn.signals.watch.result.Status;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class LoadOperatorSummaryRequest extends Request {
    private static final String DEFAULT_SORTING = "-severity_details.level_numeric";
    private final String tenant;// TODO field might be redundant
    private final String sorting;
    private final String watchId;
    private final List<String> watchStatusCodes;
    private final List<String> severities;
    private final Integer levelNumericEqualTo;
    private final Integer levelNumericGreaterThan;
    private final Integer levelNumericLessThan;
    private final List<String> actionNames;
    private final RangesFilters ranges;
    private final ActionProperties actionProperties;

    public LoadOperatorSummaryRequest(UnparsedMessage message) throws ConfigValidationException {
        DocNode docNode = message.requiredDocNode();
        this.tenant = docNode.getAsString(LoadOperatorSummaryRequestConstants.FIELD_TENANT);
        this.sorting = docNode.getAsString(LoadOperatorSummaryRequestConstants.FIELD_SORTING);
        this.watchId = docNode.getAsString(LoadOperatorSummaryRequestConstants.FIELD_WATCH_ID);
        this.watchStatusCodes = Optional.<List<String>>ofNullable(docNode.getAsListOfStrings(LoadOperatorSummaryRequestConstants.FIELD_WATCH_STATUS_CODE))//
            .orElseGet(Collections::emptyList);
        this.severities = Optional.<List<String>>ofNullable(docNode.getAsListOfStrings(LoadOperatorSummaryRequestConstants.FIELD_SEVERITIES)).orElseGet(Collections::emptyList);
        this.levelNumericEqualTo = getIntValue(docNode, LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_EQUAL_TO);
        this.levelNumericGreaterThan = getIntValue(docNode, LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_GREATER_THAN);
        this.levelNumericLessThan = getIntValue(docNode, LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_LESS_THAN);
        this.actionNames = docNode.getAsListOfStrings(LoadOperatorSummaryRequestConstants.FIELD_ACTION_NAMES);
        this.ranges = prepareRanges(docNode);
        this.actionProperties = prepareActionProperties(docNode);
    }

    private LoadOperatorSummaryRequest(String tenant, String sorting, DocNode requestBody) {
        this.tenant = tenant == null ? requestBody.getAsString(LoadOperatorSummaryRequestConstants.FIELD_TENANT) : tenant;
        this.sorting = sorting;
        this.watchStatusCodes = requestBody.getAsListOfStrings("status_codes");
        this.watchId = requestBody.getAsString("watch_id");
        this.severities = requestBody.getAsListOfStrings("severities");
        this.levelNumericEqualTo = getIntValue(requestBody, LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_EQUAL_TO);
        this.levelNumericGreaterThan = getIntValue(requestBody, LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_GREATER_THAN);
        this.levelNumericLessThan = getIntValue(requestBody, LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_LESS_THAN);

        this.actionNames = requestBody.getAsListOfStrings(LoadOperatorSummaryRequestConstants.FIELD_ACTION_NAMES);
        this.ranges = prepareRanges(requestBody);
        this.actionProperties = prepareActionProperties(requestBody);
        validateRange("level_numeric", levelNumericEqualTo, levelNumericGreaterThan, levelNumericLessThan);
    }

    LoadOperatorSummaryRequest(String tenant, List<Status.Code> statusCodes, String sorting) {
        this.tenant = tenant;
        this.sorting = sorting;
        this.watchStatusCodes = statusCodes.stream().map(Status.Code::toString).toList();
        this.watchId = null;
        this.severities = List.of();
        this.levelNumericEqualTo = null;
        this.levelNumericGreaterThan = null;
        this.levelNumericLessThan = null;
        this.actionNames = List.of();
        this.ranges = prepareRanges(DocNode.EMPTY);
        this.actionProperties = prepareActionProperties(DocNode.EMPTY);
    }

    private ActionProperties prepareActionProperties(DocNode docNode) {
        String extractedActionNameForCheckResult = getActionNameByPattern(docNode,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_CHECK_RESULT);
        String actionsCheckResultName = LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForCheckResult + ".last_check_result";
        Boolean actionsCheckResult = getBooleanValue(docNode, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForCheckResult + LoadOperatorSummaryRequestConstants.CHECK_RESULT_SUFFIX);
        String extractedActionNameForError = getActionNameByPattern(docNode, LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_ERROR);
        String actionsErrorName = LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForError + ".last_error";
        String actionsError = getStringValue(docNode, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForError + LoadOperatorSummaryRequestConstants.ERROR_SUFFIX);
        String extractedActionNameForStatusCode = getActionNameByPattern(docNode,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_STATUS_CODE);
        String actionsStatusCodeName = LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForStatusCode + ".last_status.code";
        String actionsStatusCode = getStringValue(docNode, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForStatusCode + LoadOperatorSummaryRequestConstants.STATUS_CODE_SUFFIX);
        String extractedActionNameForStatusDetails = getActionNameByPattern(docNode,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_STATUS_DETAILS);
        String actionsStatusDetailsName = LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForStatusDetails + ".last_status.detail";
        String actionsStatusDetails = getStringValue(docNode, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionNameForStatusDetails + LoadOperatorSummaryRequestConstants.STATUS_DETAILS_SUFFIX);
        return new ActionProperties(actionsCheckResultName, actionsCheckResult, actionsErrorName, actionsError,
            actionsStatusCodeName, actionsStatusCode, actionsStatusDetailsName, actionsStatusDetails);
    }

    private RangesFilters prepareRanges(DocNode requestBody) {
        String extractedActionCheckedRangeName = getActionNameByPatterns(requestBody,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_CHECKED_AFTER,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_CHECKED_BEFORE);
        String actionsCheckedName = LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionCheckedRangeName + ".last_check";
        Instant actionsCheckedBefore = getInstantValue(requestBody, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionCheckedRangeName + LoadOperatorSummaryRequestConstants.CHECKED_BEFORE_SUFFIX);
        Instant actionsCheckedAfter = getInstantValue(requestBody, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionCheckedRangeName + LoadOperatorSummaryRequestConstants.CHECKED_AFTER_SUFFIX);
        String extractedActionTriggeredRangeName = getActionNameByPatterns(requestBody,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_TRIGGERED_AFTER,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_TRIGGERED_BEFORE);
        String  actionsTriggeredName = LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionTriggeredRangeName + ".last_triggered";
        Instant actionsTriggeredBefore = getInstantValue(requestBody, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionTriggeredRangeName + LoadOperatorSummaryRequestConstants.TRIGGERED_BEFORE_SUFFIX);
        Instant actionsTriggeredAfter = getInstantValue(requestBody, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionTriggeredRangeName + LoadOperatorSummaryRequestConstants.TRIGGERED_AFTER_SUFFIX);
        String extractedActionExecutionRangeName = getActionNameByPatterns(requestBody,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_EXECUTION_AFTER,
            LoadOperatorSummaryRequestConstants.FIELD_ACTIONS_EXECUTION_BEFORE);
        String  actionsExecutionName = LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionExecutionRangeName + ".last_execution";
        Instant actionsExecutionBefore = getInstantValue(requestBody, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionExecutionRangeName + LoadOperatorSummaryRequestConstants.EXECUTION_BEFORE_SUFFIX);
        Instant actionsExecutionAfter = getInstantValue(requestBody, LoadOperatorSummaryRequestConstants.ACTIONS_PREFIX + extractedActionExecutionRangeName + LoadOperatorSummaryRequestConstants.EXECUTION_AFTER_SUFFIX);
        Range<Integer> levelNumericRange = rangeOrNull(levelNumericEqualTo, levelNumericGreaterThan, levelNumericLessThan, "last_execution.severity.level_numeric");
        Range<Instant> actionsCheckedRange = rangeOrNull(null, actionsCheckedAfter, actionsCheckedBefore, actionsCheckedName);
        Range<Instant> actionsTriggeredRange = rangeOrNull(null, actionsTriggeredAfter, actionsTriggeredBefore, actionsTriggeredName);
        Range<Instant> actionsExecutionRange = rangeOrNull(null, actionsExecutionAfter, actionsExecutionBefore, actionsExecutionName);
        return new RangesFilters(levelNumericRange, actionsCheckedRange, actionsTriggeredRange, actionsExecutionRange);
    }

    public LoadOperatorSummaryRequest(String tenant, String sorting, UnparsedDocument<?> body) throws DocumentParseException {
        this(tenant, sorting, body.parseAsDocNode());
    }
    
    public String getSortingOrDefault() {
        if(sorting == null || sorting.isBlank()) {
            return DEFAULT_SORTING;
        }
        return sorting;
    }

    @Override
    public Object toBasicObject() {
        return ImmutableMap.of(LoadOperatorSummaryRequestConstants.FIELD_TENANT, tenant,
                LoadOperatorSummaryRequestConstants.FIELD_SORTING, sorting,
                LoadOperatorSummaryRequestConstants.FIELD_WATCH_STATUS_CODE, watchStatusCodes)//
            .with(LoadOperatorSummaryRequestConstants.FIELD_SEVERITIES, severities) //
            .with(LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_EQUAL_TO, levelNumericEqualTo)//
            .with(LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_GREATER_THAN, levelNumericGreaterThan)//
            .with(LoadOperatorSummaryRequestConstants.FIELD_LEVEL_NUMERIC_LESS_THAN, levelNumericLessThan)//
            .with(LoadOperatorSummaryRequestConstants.FIELD_ACTION_NAMES, actionNames)//
            .with("ranges", ranges)
            .with("actionProperties", actionProperties);
    }

    WatchFilter getWatchFilter() {
        return new WatchFilter(watchId, watchStatusCodes, severities, actionNames, ranges, actionProperties);
    }

    String getTenant() {
        return tenant;
    }

    private void validateRange(String rangeName, Number equal, Number greater, Number less) {
        if(Objects.nonNull(equal) && (Objects.nonNull(greater) || Objects.nonNull(less))) {
            String message = "Incorrect search criteria for " + rangeName + //
                ". If field equalTo is provided then both fields 'greaterThan' and 'lessThan' must be null";
            throw new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
        }
    }

    private <T> Range<T> rangeOrNull(T equalTo, T greaterThan, T lessThan, String fieldName) {
        if(Objects.nonNull(equalTo) || Objects.nonNull(greaterThan) || Objects.nonNull(lessThan)) {
            return new Range<>(equalTo, greaterThan, lessThan, fieldName);
        } else {
            return null;
        }
    }
}
