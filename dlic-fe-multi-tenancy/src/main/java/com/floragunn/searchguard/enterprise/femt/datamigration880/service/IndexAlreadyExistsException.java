package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.rest.RestStatus;

import java.io.Serial;

public class IndexAlreadyExistsException extends RepositoryException {
    @Serial
    private static final long serialVersionUID = -7535050075560100315L;

    public IndexAlreadyExistsException(String message, ResourceAlreadyExistsException e) {
        super(message, RestStatus.CONFLICT, e);
    }
}
