package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.Serial;

class RepositoryException extends ElasticsearchStatusException {
    @Serial
    private static final long serialVersionUID = -6595578854509270371L;

    public RepositoryException(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, status, cause, args);
    }

    public RepositoryException(String msg, RestStatus status, Object... args) {
        super(msg, status, args);
    }

    public RepositoryException(StreamInput in) throws IOException {
        super(in);
    }
}