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

package com.floragunn.searchguard.enterprise.encrypted_indices.index;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperationsFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.Index;
import org.opensearch.index.IndexService;

import java.io.IOException;

public class DecryptingDirectoryReaderWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {
    private static final Logger log = LogManager.getLogger(DecryptingDirectoryReaderWrapper.class);

    private final IndexService indexService;
    private final AuditLog auditlog;
    private final Index index;
    private final ThreadContext threadContext;

    private final CryptoOperations cryptoOperations;

    public DecryptingDirectoryReaderWrapper(IndexService indexService, AuditLog auditlog, CryptoOperationsFactory cryptoOperationsFactory) {
        this.indexService = indexService;
        this.index = indexService.index();
        this.auditlog = auditlog;
        this.threadContext = indexService.getThreadPool().getThreadContext();
        this.cryptoOperations = cryptoOperationsFactory.createCryptoOperations(indexService.getIndexSettings());
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {
            if (this.cryptoOperations == null) {
                return reader;
            }

            return new DecryptingDirectoryReader(reader, cryptoOperations);
    }
}
