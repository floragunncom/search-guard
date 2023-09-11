package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import java.util.Optional;

public interface MigrationStateRepository {
    void upsert(String id, MigrationExecutionSummary migrationExecutionSummary);

    void updateWithLock(String id, MigrationExecutionSummary migrationExecutionSummary, OptimisticLock lock) throws OptimisticLockException;

    boolean isIndexCreated();

    void createIndex() throws IndexAlreadyExistsException;

    Optional<MigrationExecutionSummary> findById(String id);

    void create(String migrationId, MigrationExecutionSummary summary) throws OptimisticLockException ;
}
