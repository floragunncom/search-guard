package com.floragunn.signals.actions.summary;

import org.elasticsearch.ElasticsearchException;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SummaryToWatchFieldMapper {

    public static final String DATE_TIME_FORMAT = "strict_date_time";
    private final static List<SummaryToWatchFieldMapper> FIELDS = Arrays.asList(
        new SummaryToWatchFieldMapper("severity", "last_status.severity.keyword"),
        new SummaryToWatchFieldMapper("status_code", "last_status.code.keyword"),
        new SummaryToWatchFieldMapper("severity_details.level_numeric", "last_execution.severity.level_numeric"),
        new SummaryToWatchFieldMapper("severity_details.current_value", "last_execution.severity.value"),
        new SummaryToWatchFieldMapper("severity_details.threshold", "last_execution.severity.threshold"),
        new SummaryToWatchFieldMapper("actions.status_code", "last_execution.severity.threshold"),
        new SummaryToWatchFieldMapper(Pattern.compile("actions\\.(?<path>[^.]+)\\.triggered"), "actions.$$$.last_triggered", DATE_TIME_FORMAT),
        new SummaryToWatchFieldMapper(Pattern.compile("actions\\.(?<path>[^.]+)\\.checked"), "actions.$$$.last_check", DATE_TIME_FORMAT),
        new SummaryToWatchFieldMapper(Pattern.compile("actions\\.(?<path>[^.]+)\\.check_result"), "actions.$$$.last_check_result"),
        new SummaryToWatchFieldMapper(Pattern.compile("actions\\.(?<path>[^.]+)\\.execution"), "actions.$$$.last_execution", DATE_TIME_FORMAT),
        new SummaryToWatchFieldMapper(Pattern.compile("actions\\.(?<path>[^.]+)\\.error"), "actions.$$$.last_error"),
        new SummaryToWatchFieldMapper(Pattern.compile("actions\\.(?<path>[^.]+)\\.status_code"), "actions.$$$.last_status.code.keyword"),
        new SummaryToWatchFieldMapper(Pattern.compile("actions\\.(?<path>[^.]+)\\.status_details"), "actions.$$$.last_status.detail.keyword")
    );

    private final Pattern fieldNamePattern;
    private final String documentFieldName;

    private final String sortingFormat;
    private final boolean usesPatterns;

    private SummaryToWatchFieldMapper(String inputName, String documentFieldName) {
        this.fieldNamePattern = Pattern.compile("\\Q" + Objects.requireNonNull(inputName) + "\\E");
        this.documentFieldName = Objects.requireNonNull(documentFieldName);
        this.usesPatterns = false;
        this.sortingFormat = null;
    }

    private SummaryToWatchFieldMapper(Pattern fieldNamePattern, String documentFieldName, String sortingFormat) {
        this.fieldNamePattern = Objects.requireNonNull(fieldNamePattern);
        this.documentFieldName = Objects.requireNonNull(documentFieldName);
        this.usesPatterns = true;
        this.sortingFormat = sortingFormat;
    }

    private SummaryToWatchFieldMapper(Pattern fieldNamePattern, String documentFieldName) {
        this(fieldNamePattern, documentFieldName, null);
    }

    public String getDocumentFieldName(String inputFieldName) {
        if(usesPatterns) {
            Matcher matcher = fieldNamePattern.matcher(inputFieldName);
            if(matcher.matches()) {
                String path = matcher.group("path");
                return documentFieldName.replace("$$$", path);
            } else {
                throw new ElasticsearchException("Incorrect field name " + inputFieldName + " for pattern " + this);
            }
        } else {
            return documentFieldName;
        }
    }

    private boolean matches(String fieldName) {
        return fieldNamePattern.matcher(fieldName).matches();
    }

    public static Optional<SummaryToWatchFieldMapper> findFieldByName(String fieldName) {
        return FIELDS.stream().filter(field -> field.matches(fieldName)).findAny();
    }

    public Optional<String> getSortingFormat() {
        return Optional.ofNullable(sortingFormat);
    }

    @Override
    public String toString() {
        return "SummaryField{" + "fieldNamePattern=" + fieldNamePattern + ", documentFieldName='" + documentFieldName + '\'' + ", usesPatterns=" + usesPatterns + '}';
    }

    static String getSearchFieldName(String inputName) {
        return findFieldByName(inputName).orElseThrow(() -> new ElasticsearchException("Incorrect watch summary field name" + inputName))
            .getDocumentFieldName(inputName);//TODO use dedicated method for searching
    }
}
