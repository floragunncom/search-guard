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

import java.util.Optional;

public interface MigrationStateRepository {
    void upsert(String id, MigrationExecutionSummary migrationExecutionSummary);

    void updateWithLock(String id, MigrationExecutionSummary migrationExecutionSummary, OptimisticLock lock) throws OptimisticLockException;

    boolean isIndexCreated();

    void createIndex() throws IndexAlreadyExistsException;

    Optional<MigrationExecutionSummary> findById(String id);

    void create(String migrationId, MigrationExecutionSummary summary) throws OptimisticLockException ;
}
