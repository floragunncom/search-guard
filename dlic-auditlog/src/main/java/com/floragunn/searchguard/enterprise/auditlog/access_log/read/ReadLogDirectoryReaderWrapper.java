/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.access_log.read;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.IndexService;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.auditlog.AuditLogConfig;

public class ReadLogDirectoryReaderWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
    private static final Logger log = LogManager.getLogger(ReadLogDirectoryReaderWrapper.class);

    private final ReadLogContext context;

    public ReadLogDirectoryReaderWrapper(IndexService indexService, AuditLog auditlog, AuditLogConfig complianceConfig) {        
        this.context = new ReadLogContext(indexService, auditlog, complianceConfig);
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {
        try {
            return new ReadLogDirectoryReader(reader, context);
        } catch (RuntimeException e) {
            log.error("Error in ReadLogIndexDirectoryReaderWrapper", e);
            throw e;
        }
    }

}
