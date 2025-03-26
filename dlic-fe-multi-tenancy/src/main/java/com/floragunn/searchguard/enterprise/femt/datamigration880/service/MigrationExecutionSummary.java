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
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import jakarta.annotation.Nullable;
import org.elasticsearch.common.time.DateFormatter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.MappingTypes.MAPPING_DATE;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.MappingTypes.MAPPING_KEYWORD;

public record MigrationExecutionSummary(LocalDateTime startTime, ExecutionStatus status, @Nullable String tempIndexName,
                                        @Nullable String backupIndexName, ImmutableList<StepExecutionSummary> stages,
                                        @Nullable OptimisticLock lockData)
    implements Document<MigrationExecutionSummary> {

    private final static Duration DURATION_AGE_THRESHOLD = Duration.parse("PT15M");

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    public static final String FIELD_START_TIME = "start_time";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_TEMP_INDEX_NAME = "temp_index_name";
    public static final String FIELD_BACKUP_INDEX_NAME = "backup_index_name";
    public static final String FIELD_STAGES = "stages";
    public static final ImmutableMap<String, Object> MAPPING = ImmutableMap.of("dynamic", true, "properties",
        ImmutableMap.of(FIELD_START_TIME, MAPPING_DATE,
            FIELD_STATUS, MAPPING_KEYWORD,
            FIELD_TEMP_INDEX_NAME, MAPPING_KEYWORD,
            FIELD_BACKUP_INDEX_NAME, MAPPING_KEYWORD, FIELD_STAGES, StepExecutionSummary.MAPPING));

    public MigrationExecutionSummary {
        Objects.requireNonNull(startTime, "Start time is required");
        Objects.requireNonNull(status, "Status is required");
        Objects.requireNonNull(stages, "Stages list is required");
    }

    public MigrationExecutionSummary(LocalDateTime startTime, ExecutionStatus status, @Nullable String tempIndexName, @Nullable String backupIndexName,
        ImmutableList<StepExecutionSummary> stages) {
        this(startTime, status, tempIndexName, backupIndexName, stages, null);
    }
    public static MigrationExecutionSummary parse(DocNode docNode, Long primaryTerm, Long seqNo) {
        Objects.requireNonNull(docNode, "Doc node is require to parse data migration summary.");
        LocalDateTime startTime = LocalDateTime.from(DATE_FORMATTER.parse(docNode.getAsString(FIELD_START_TIME)));
        ExecutionStatus status = ExecutionStatus.valueOf(docNode.getAsString(FIELD_STATUS).toUpperCase());
        ImmutableList<StepExecutionSummary> stages = docNode.getAsListOfNodes(FIELD_STAGES).map(StepExecutionSummary::parse);
        return new MigrationExecutionSummary(startTime, status, docNode.getAsString(FIELD_TEMP_INDEX_NAME),
            docNode.getAsString(FIELD_BACKUP_INDEX_NAME), stages, new OptimisticLock(primaryTerm, seqNo));
    }

    @Override
    public Object toBasicObject() {
        String formattedStartTime = DATE_FORMATTER.format(startTime);
        ImmutableList<ImmutableMap<String, Object>> stepList = stages.map(StepExecutionSummary::toBasicObject);
        return OrderedImmutableMap.ofNonNull(FIELD_START_TIME, formattedStartTime, FIELD_STATUS, status.name().toLowerCase(),
            FIELD_TEMP_INDEX_NAME, tempIndexName, FIELD_BACKUP_INDEX_NAME, backupIndexName, FIELD_STAGES, stepList);
    }

    public boolean isSuccessful() {
        return ExecutionStatus.SUCCESS.equals(status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationExecutionSummary that = (MigrationExecutionSummary) o;
        return Objects.equals(startTime, that.startTime) && status == that.status && Objects.equals(tempIndexName,
            that.tempIndexName) && Objects.equals(backupIndexName, that.backupIndexName) && Objects.equals(stages, that.stages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, status, tempIndexName, backupIndexName, stages);
    }

    private Duration age(LocalDateTime now) {
        return Duration.between(startTime, now);
    }

    public boolean isMigrationInProgress(LocalDateTime now) {
        Objects.requireNonNull(now, "Current time is required to private data migration life span.");
        if(ExecutionStatus.SUCCESS.equals(status) || ExecutionStatus.FAILURE.equals(status)) {
            return false;
        }
        Duration age = age(now);
        return age.minus(DURATION_AGE_THRESHOLD).isNegative();
    }
}
