/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.google.common.base.Throwables;
import jakarta.annotation.Nullable;
import org.elasticsearch.common.time.DateFormatter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.MappingTypes.MAPPING_DATE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.MappingTypes.MAPPING_KEYWORD;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.MappingTypes.MAPPING_LONG;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.MappingTypes.MAPPING_TEXT_WITH_KEYWORD;

public record StepExecutionSummary(long number, LocalDateTime startTime, String name, StepExecutionStatus status, String message,
                                   @Nullable String details)
    implements Document<StepExecutionSummary> {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
    public static final String FIELD_START_TIME = "start_time";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_NO = "number";
    public static final String FIELD_DETAILS = "details";

    public static final ImmutableMap<String, Object> MAPPING = ImmutableMap.of("dynamic", true, "properties",
        ImmutableMap.of(FIELD_START_TIME, MAPPING_DATE,
            FIELD_STATUS, MAPPING_KEYWORD,
            FIELD_NAME, MAPPING_KEYWORD,
            FIELD_NO, MAPPING_LONG,
            FIELD_MESSAGE, MAPPING_TEXT_WITH_KEYWORD)//
            .with(FIELD_DETAILS, MAPPING_TEXT_WITH_KEYWORD));

    public StepExecutionSummary(long number, LocalDateTime startTime, String name, StepExecutionStatus status, String message) {
        this(number, startTime, name, status, message, (String) null);
    }

    public StepExecutionSummary(long number, LocalDateTime startTime, String name, StepExecutionStatus status, String message, Throwable details) {
        this(number, startTime, name, status, message, stacktraceToString(details));
    }

    private static String stacktraceToString(Throwable ex) {
        if(ex == null) {
            return null;
        }
        try {
            return Throwables.getStackTraceAsString(ex);
        } catch (Throwable e) {
            return "Cannot obtain stack trace from exception " + ex.getClass().getCanonicalName() + " with message " + ex.getMessage();
        }
    }

    public StepExecutionSummary {
        Objects.requireNonNull(startTime, "Start time is required");
        Objects.requireNonNull(name, "Name is required");
        Objects.requireNonNull(status, "Step status required");
        Objects.requireNonNull(message, "Message is required");
    }

    StepExecutionSummary(long number, LocalDateTime startTime, String name, StepResult stepResult) {
        this(number, startTime, name, Objects.requireNonNull(stepResult, "Step result is required").status(), stepResult.message(),
            stepResult.details());
    }

    @Override
    public ImmutableMap<String, Object> toBasicObject() {
        String formattedStartTime = DATE_FORMATTER.format(startTime);
        return OrderedImmutableMap.<String, Object>ofNonNull(FIELD_NO, number, FIELD_START_TIME, formattedStartTime, FIELD_NAME, name,
            FIELD_STATUS, status.name().toLowerCase(), FIELD_MESSAGE, message) //
            .with(FIELD_DETAILS, details);
    }

    public static StepExecutionSummary parse(DocNode docNode) {
        try {
            LocalDateTime startTime = LocalDateTime.from(DATE_FORMATTER.parse(docNode.getAsString(FIELD_START_TIME)));
            StepExecutionStatus status = StepExecutionStatus.valueOf(docNode.getAsString(FIELD_STATUS).toUpperCase());
            return new StepExecutionSummary(docNode.getNumber(FIELD_NO).longValue(), startTime, docNode.getAsString(FIELD_NAME), status,
                docNode.getAsString(FIELD_MESSAGE), docNode.getAsString(FIELD_DETAILS));
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Cannot parse step execution summary from json.", e);
        }
    }
}
