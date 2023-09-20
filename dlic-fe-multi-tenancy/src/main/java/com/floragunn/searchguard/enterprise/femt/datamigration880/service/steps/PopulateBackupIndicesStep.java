package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.IndexNameDataFormatter;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStep;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.DataMigrationContext.BACKUP_INDEX_NAME_PREFIX;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INVALID_BACKUP_INDEX_NAME;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.INVALID_DATE_IN_BACKUP_INDEX_NAME_ERROR;
import static com.floragunn.searchguard.enterprise.femt.datamigration880.service.StepExecutionStatus.OK;

public class PopulateBackupIndicesStep implements MigrationStep {

    private static final Logger log = LogManager.getLogger(PopulateBackupIndicesStep.class);

    private final StepRepository repository;

    public PopulateBackupIndicesStep(StepRepository repository) {
        this.repository = Objects.requireNonNull(repository, "Step repository is required");
    }

    @Override
    public StepResult execute(DataMigrationContext context) throws StepException {
        Optional<GetIndexResponse> responseOptional = repository.findIndexByNameOrAlias(BACKUP_INDEX_NAME_PREFIX + "*");
        responseOptional.ifPresentOrElse(response -> {
            List<String> sortedIndices = Arrays.stream(response.getIndices()) //
                .sorted(this.backupIndexComparator()) // TODO use comparator
                .collect(Collectors.toList());
            context.setBackupIndices(ImmutableList.of(sortedIndices));
        }, () -> context.setBackupIndices(ImmutableList.empty()));
        ImmutableList<String> backupIndices = context.getBackupIndices();
        String indicesString = backupIndices.stream().map(name -> "'" + name + "'").collect(Collectors.joining(", "));
        return new StepResult(OK, "Found " + backupIndices.size() + " backup indices", "Backup indices: " + indicesString);
    }

    private Comparator<String> backupIndexComparator() {
        return (indexOne, indexTwo) -> {
            LocalDateTime localDateOne = extractDateFromIndexName(indexOne);
            LocalDateTime localDateTwo = extractDateFromIndexName(indexTwo);
            return localDateTwo.compareTo(localDateOne);
        };
    }

    private LocalDateTime extractDateFromIndexName(String indexName) {
        if(!indexName.startsWith(BACKUP_INDEX_NAME_PREFIX)) {
            String message = "Backup index name does not start with prefix " + BACKUP_INDEX_NAME_PREFIX;
            String details = "Invalid index name '" + indexName + "'";
            throw new StepException(message, INVALID_BACKUP_INDEX_NAME, details);
        }
        String datePart = indexName.substring(BACKUP_INDEX_NAME_PREFIX.length());
        try {
            return IndexNameDataFormatter.parse(datePart);
        } catch (DateTimeParseException ex) {
            log.error("Cannot parse date from backup index name '{}'.", indexName, ex);
            String message = "Cannot extract data from backup index name";
            String details = "Index name '" + indexName + "'";
            throw new StepException(message, INVALID_DATE_IN_BACKUP_INDEX_NAME_ERROR, details);
        }
    }

    @Override
    public String name() {
        return "find backup indices";
    }
}
