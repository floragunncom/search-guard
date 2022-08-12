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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

public class DecryptingDirectoryReader extends FilterDirectoryReader {

    public DecryptingDirectoryReader(DirectoryReader in) throws IOException {
        super(in, new SubReaderWrapper());
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new DecryptingDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private static class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

        SubReaderWrapper() {
            //this.dlsFlsContext = dlsFlsContext;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new FilterLeafReader(reader);
        }

        private static class FilterLeafReader extends SequentialStoredFieldsLeafReader {
            private static final Logger log = LogManager.getLogger(FilterLeafReader.class);


            FilterLeafReader(LeafReader delegate) {
                super(delegate);
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                in.document(docID, new DecryptingStoredFieldVisitor(visitor));

                //if (dlsFlsContext.hasFlsRestriction() || dlsFlsContext.hasFieldMasking()) {
                //    in.document(docID, new FlsStoredFieldVisitor(visitor, dlsFlsContext.getFlsRule(), dlsFlsContext.getFieldMaskingRule()));
                //} else {
                //    in.document(docID, visitor);
                //}
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


            /*@Override
            public Fields getTermVectors(final int docID) throws IOException {
                Fields fields = in.getTermVectors(docID);

                if (dlsFlsContext.hasFieldMasking() || fields == null) {
                    return fields;
                }

                return new Fields() {

                    @Override
                    public Iterator<String> iterator() {
                        return Iterators.<String>filter(fields.iterator(), (field) -> dlsFlsContext.getFlsRule().isAllowed(field));
                    }

                    @Override
                    public Terms terms(String field) throws IOException {

                        if (!dlsFlsContext.getFlsRule().isAllowed(field)) {
                            return null;
                        }

                        return wrapTerms(field, in.terms(field));
                    }

                    @Override
                    public int size() {
                        return flsFieldInfos.size();
                    }
                };
            }*/


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

                    delegate.visitDocument(docID, new DecryptingStoredFieldVisitor(visitor));

                   // if (dlsFlsContext.hasFlsRestriction() || dlsFlsContext.hasFieldMasking()) {
                    //     visitor = new FlsStoredFieldVisitor(visitor, dlsFlsContext.getFlsRule(), dlsFlsContext.getFieldMaskingRule());
                    // }

                    // delegate.visitDocument(docID, visitor);
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
