package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.rest.RestStatus;

public class IndexAlreadyExistsException extends RepositoryException {
    public IndexAlreadyExistsException(String message, ResourceAlreadyExistsException e) {
        super(message, RestStatus.CONFLICT, e);
    }
}
