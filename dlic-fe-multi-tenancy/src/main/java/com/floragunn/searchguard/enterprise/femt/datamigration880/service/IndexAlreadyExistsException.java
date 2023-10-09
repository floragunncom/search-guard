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
