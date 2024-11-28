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

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

public class ReadLogDirectoryReader extends FilterDirectoryReader {
    private final ReadLogContext context;

    public ReadLogDirectoryReader(DirectoryReader in, ReadLogContext context) throws IOException {
        super(in, new SubReaderWrapper(context));
        this.context = context;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ReadLogDirectoryReader(in, context);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private static class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

        private final ReadLogContext context;

        SubReaderWrapper(ReadLogContext context) {
            this.context = context;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new FilterLeafReader(reader, context);
        }

        private static class FilterLeafReader extends SequentialStoredFieldsLeafReader {

            private final ReadLogContext context;

            FilterLeafReader(LeafReader delegate, ReadLogContext context) {
                super(delegate);

                this.context = context;
            }

            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return new ReadLogStoredFieldsReader(reader);
            }

            @Override
            public StoredFields storedFields() throws IOException {
                StoredFields storedFields = super.storedFields();
                if (context.getAuditLogConfig().isEnabled() && context.getAuditLogConfig().readHistoryEnabledForIndex(context.getIndex().getName())) {
                    return new StoredFields() {
                        @Override
                        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                            ComplianceAwareStoredFieldVisitor complianceAwareStoredFieldVisitor = new ComplianceAwareStoredFieldVisitor(visitor, context);
                            storedFields.document(docID, complianceAwareStoredFieldVisitor);
                            complianceAwareStoredFieldVisitor.finished();
                        }
                    };
                } else {
                    return storedFields;
                }

            }

            private class ReadLogStoredFieldsReader extends StoredFieldsReader {

                private final StoredFieldsReader delegate;

                public ReadLogStoredFieldsReader(StoredFieldsReader delegate) {
                    this.delegate = delegate;

                    //System.out.println("RLSFR " + context.getIndex().getName());
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }

                @Override
                public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                    if (context.getAuditLogConfig().isEnabled()
                            && context.getAuditLogConfig().readHistoryEnabledForIndex(context.getIndex().getName())) {
                        ComplianceAwareStoredFieldVisitor complianceAwareStoredFieldVisitor = new ComplianceAwareStoredFieldVisitor(visitor, context);
                        delegate.document(docID, complianceAwareStoredFieldVisitor);
                        complianceAwareStoredFieldVisitor.finished();
                    } else {
                        delegate.document(docID, visitor);
                    }
                }

                @Override
                public StoredFieldsReader clone() {
                    return new ReadLogStoredFieldsReader(delegate);
                }

                @Override
                public void checkIntegrity() throws IOException {
                    delegate.checkIntegrity();
                }
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return getDelegate().getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return getDelegate().getReaderCacheHelper();
            }
        }

    }
}
