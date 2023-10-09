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
