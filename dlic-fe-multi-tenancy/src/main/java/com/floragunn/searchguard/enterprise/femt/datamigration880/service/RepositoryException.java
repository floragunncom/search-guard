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