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

import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;
import java.util.Objects;

public class DecryptingDirectoryReader extends FilterDirectoryReader {

    private final CryptoOperations cryptoOperations;

    public DecryptingDirectoryReader(DirectoryReader in, CryptoOperations cryptoOperations) throws IOException {
        super(in, new SubReaderWrapper(Objects.requireNonNull(cryptoOperations, "cryptoOperations must not be null")));
        this.cryptoOperations = cryptoOperations;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new DecryptingDirectoryReader(in, cryptoOperations);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private static class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

        private final CryptoOperations cryptoOperations;

        SubReaderWrapper(CryptoOperations cryptoOperations) {
            this.cryptoOperations = cryptoOperations;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new FilterLeafReader(reader, cryptoOperations);
        }

        private static class FilterLeafReader extends SequentialStoredFieldsLeafReader {
            private static final Logger log = LogManager.getLogger(FilterLeafReader.class);
            private final CryptoOperations cryptoOperations;

            FilterLeafReader(LeafReader delegate, CryptoOperations cryptoOperations) {
                super(delegate);
                this.cryptoOperations = cryptoOperations;
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                in.document(docID, new DecryptingStoredFieldVisitor(visitor, cryptoOperations));
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }


            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return new DecryptingStoredFieldsReader(reader);
            }

            private class DecryptingStoredFieldsReader extends StoredFieldsReader {

                private final StoredFieldsReader delegate;

                public DecryptingStoredFieldsReader(StoredFieldsReader delegate) {
                    this.delegate = delegate;
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }

                @Override
                public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
                    delegate.visitDocument(docID, new DecryptingStoredFieldVisitor(visitor, cryptoOperations));
                }

                @Override
                public StoredFieldsReader clone() {
                    return new DecryptingStoredFieldsReader(delegate);
                }

                @Override
                public void checkIntegrity() throws IOException {
                    delegate.checkIntegrity();
                }
            }
        }
    }
}
