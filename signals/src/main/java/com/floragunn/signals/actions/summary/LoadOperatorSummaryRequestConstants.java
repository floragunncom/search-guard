package com.floragunn.signals.actions.summary;

import java.util.regex.Pattern;

public class LoadOperatorSummaryRequestConstants {
    public static final String ACTIONS_PREFIX = "actions.";
    public static final String FIELD_TENANT = "tenant";
    public static final String FIELD_SORTING = "sorting";
    public static final String FIELD_SIZE = "size";
    public static final String FIELD_WATCH_STATUS_CODE = "watch_status_codes";
    public static final String FIELD_WATCH_ID = "watch_id";
    public static final String FIELD_SEVERITIES = "severities";
    public static final String FIELD_LEVEL_NUMERIC_EQUAL_TO = "level_numeric_equal_to";
    public static final String FIELD_LEVEL_NUMERIC_GREATER_THAN = "level_numeric_greater_than";
    public static final String FIELD_LEVEL_NUMERIC_LESS_THAN = "level_numeric_less_than";
    public static final String FIELD_ACTION_NAMES = "actions";
    public static final String CHECKED_BEFORE_SUFFIX = ".checkedBefore";
    public static final Pattern FIELD_ACTIONS_CHECKED_BEFORE = Pattern.compile("actions\\.(?<path>[^.]+)\\" + CHECKED_BEFORE_SUFFIX);
    public static final String CHECKED_AFTER_SUFFIX = ".checkedAfter";
    public static final Pattern FIELD_ACTIONS_CHECKED_AFTER = Pattern.compile("actions\\.(?<path>[^.]+)\\" + CHECKED_AFTER_SUFFIX);
    public static final String TRIGGERED_BEFORE_SUFFIX = ".triggeredBefore";
    public static final Pattern FIELD_ACTIONS_TRIGGERED_BEFORE = Pattern.compile("actions\\.(?<path>[^.]+)\\" + TRIGGERED_BEFORE_SUFFIX);
    public static final String TRIGGERED_AFTER_SUFFIX = ".triggeredAfter";
    public static final Pattern FIELD_ACTIONS_TRIGGERED_AFTER = Pattern.compile("actions\\.(?<path>[^.]+)\\" + TRIGGERED_AFTER_SUFFIX);
    public static final String EXECUTION_BEFORE_SUFFIX = ".executionBefore";
    public static final Pattern FIELD_ACTIONS_EXECUTION_BEFORE = Pattern.compile("actions\\.(?<path>[^.]+)\\" + EXECUTION_BEFORE_SUFFIX);
    public static final String EXECUTION_AFTER_SUFFIX = ".executionAfter";
    public static final Pattern FIELD_ACTIONS_EXECUTION_AFTER = Pattern.compile("actions\\.(?<path>[^.]+)\\" + EXECUTION_AFTER_SUFFIX);
    public static final String CHECK_RESULT_SUFFIX = ".checkResult";
    public static final Pattern FIELD_ACTIONS_CHECK_RESULT = Pattern.compile("actions\\.(?<path>[^.]+)\\" + CHECK_RESULT_SUFFIX);
    public static final String ERROR_SUFFIX = ".error";
    public static final Pattern FIELD_ACTIONS_ERROR = Pattern.compile("actions\\.(?<path>[^.]+)\\" + ERROR_SUFFIX);
    public static final String STATUS_CODE_SUFFIX = ".statusCode";
    public static final Pattern FIELD_ACTIONS_STATUS_CODE = Pattern.compile("actions\\.(?<path>[^.]+)\\" + STATUS_CODE_SUFFIX);
    public static final String STATUS_DETAILS_SUFFIX = ".statusDetails";
    public static final Pattern FIELD_ACTIONS_STATUS_DETAILS = Pattern.compile("actions\\.(?<path>[^.]+)\\" + STATUS_DETAILS_SUFFIX);
}