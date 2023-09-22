package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;

import java.io.Serial;

public class OptimisticLockException extends RepositoryException {
    @Serial
    private static final long serialVersionUID = -3255739453082458349L;

    public OptimisticLockException(String message, VersionConflictEngineException e) {
        super(message, RestStatus.INTERNAL_SERVER_ERROR, e);
    }
}
