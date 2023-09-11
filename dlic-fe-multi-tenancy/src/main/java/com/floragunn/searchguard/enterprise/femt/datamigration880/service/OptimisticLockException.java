package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;

public class OptimisticLockException extends RepositoryException {
    public OptimisticLockException(String message, VersionConflictEngineException e) {
        super(message, RestStatus.INTERNAL_SERVER_ERROR, e);
    }
}
