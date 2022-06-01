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
package com.floragunn.searchguard.enterprise.dlsfls.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.support.ConfigConstants;
import com.google.common.collect.Iterators;

public class DlsFlsDirectoryReader extends FilterDirectoryReader {
    private final DlsFlsContext dlsFlsContext;

    public DlsFlsDirectoryReader(DirectoryReader in, DlsFlsContext dlsFlsContext) throws IOException {
        super(in, new SubReaderWrapper(dlsFlsContext));
        this.dlsFlsContext = dlsFlsContext;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new DlsFlsDirectoryReader(in, dlsFlsContext);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    private static class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

        private final DlsFlsContext dlsFlsContext;

        SubReaderWrapper(DlsFlsContext dlsFlsContext) {
            this.dlsFlsContext = dlsFlsContext;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new FilterLeafReader(reader, dlsFlsContext);
        }

        private static class FilterLeafReader extends SequentialStoredFieldsLeafReader {
            private static final Logger log = LogManager.getLogger(FilterLeafReader.class);

            private final FieldInfos flsFieldInfos;
            private final DlsFlsContext dlsFlsContext;
            private final DlsGetEvaluator dlsGetEvaluator;

            FilterLeafReader(LeafReader delegate, DlsFlsContext dlsFlsContext) {
                super(delegate);

                this.dlsFlsContext = dlsFlsContext;

                try {
                    if (!dlsFlsContext.hasFlsRestriction()) {
                        FlsRule flsRule = dlsFlsContext.getFlsRule();

                        FieldInfos originalFieldInfos = delegate.getFieldInfos();
                        List<FieldInfo> restrictedFieldInfos = new ArrayList<>(originalFieldInfos.size());

                        for (FieldInfo fieldInfo : originalFieldInfos) {
                            if (flsRule.isAllowed(fieldInfo.name)) {
                                restrictedFieldInfos.add(fieldInfo);
                            }
                        }

                        this.flsFieldInfos = new FieldInfos(restrictedFieldInfos.toArray(new FieldInfo[restrictedFieldInfos.size()]));

                    } else {
                        this.flsFieldInfos = delegate.getFieldInfos();
                    }

                    this.dlsGetEvaluator = new DlsGetEvaluator(this, dlsFlsContext.getDlsQuery(), in, applyDlsHere());
                } catch (RuntimeException e) {
                    log.error("Got exception while initializing " + this, e);
                    throw e;
                }
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                if (log.isTraceEnabled()) {
                    log.trace("FilterLeafReader.document()\nindex: " + dlsFlsContext.getIndexService().index().getName() + "\nfls: "
                            + dlsFlsContext.getFlsRule() + "\nfieldMasking: " + dlsFlsContext.getFieldMaskingRule());
                }

                if (dlsFlsContext.hasFlsRestriction() || dlsFlsContext.hasFieldMasking()) {
                    in.document(docID, new FlsStoredFieldVisitor(visitor, dlsFlsContext.getFlsRule(), dlsFlsContext.getFieldMaskingRule()));
                } else {
                    in.document(docID, visitor);
                }
            }

            @Override
            public Bits getLiveDocs() {
                return dlsGetEvaluator.getLiveDocs();
            }

            @Override
            public int numDocs() {
                return dlsGetEvaluator.numDocs();
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return dlsGetEvaluator.getReaderCacheHelper();
            }

            @Override
            public boolean hasDeletions() {
                return dlsGetEvaluator.hasDeletions();
            }

            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return new DlsFlsStoredFieldsReader(reader);
            }

            @Override
            public FieldInfos getFieldInfos() {
                return flsFieldInfos;
            }

            @Override
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
            }

            private Terms wrapTerms(String field, Terms terms) throws IOException {
                if (terms == null) {
                    return null;
                }

                try {
                    if ("_field_names".equals(field)) {
                        return new FilteredTerms(terms);
                    }

                    return terms;
                } catch (RuntimeException e) {
                    log.error("Got exception in wrapTerms(" + field + ")", e);
                    throw e;
                }
            }

            private boolean applyDlsHere() {
                if (isSuggest()) {
                    return true;
                }

                String action = getRuntimeActionName();

                return !action.startsWith("indices:data/read/search");
            }

            private String getRuntimeActionName() {
                return (String) dlsFlsContext.getThreadContext().getTransient(ConfigConstants.SG_ACTION_NAME);
            }

            private boolean isSuggest() {
                return dlsFlsContext.getThreadContext().getTransient("_sg_issuggest") == Boolean.TRUE;
            }

            private class DlsFlsStoredFieldsReader extends StoredFieldsReader {

                private final StoredFieldsReader delegate;

                public DlsFlsStoredFieldsReader(StoredFieldsReader delegate) {
                    this.delegate = delegate;
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }

                @Override
                public long ramBytesUsed() {
                    return delegate.ramBytesUsed();
                }

                @Override
                public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
                    if (log.isTraceEnabled()) {
                        log.trace("DlsFlsStoredFieldsReader.visitDocument()\nindex: " + dlsFlsContext.getIndexService().index().getName() + "\nfls: "
                                + dlsFlsContext.getFlsRule() + "\nfieldMasking: " + dlsFlsContext.getFieldMaskingRule());
                    }

                    if (dlsFlsContext.hasFlsRestriction() || dlsFlsContext.hasFieldMasking()) {
                        visitor = new FlsStoredFieldVisitor(visitor, dlsFlsContext.getFlsRule(), dlsFlsContext.getFieldMaskingRule());
                    }

                    delegate.visitDocument(docID, visitor);
                }

                @Override
                public StoredFieldsReader clone() {
                    return new DlsFlsStoredFieldsReader(delegate);
                }

                @Override
                public void checkIntegrity() throws IOException {
                    delegate.checkIntegrity();
                }
            }

            private final class FilteredTerms extends FilterTerms {

                //According to 
                //https://www.elastic.co/guide/en/elasticsearch/reference/6.8/mapping-field-names-field.html
                //"The _field_names field used to index the names of every field in a document that contains any value other than null"
                //"For fields which have either doc_values or norm enabled the exists query will still be available but will not use the _field_names field."
                //That means if a field has no doc values (which is always the case for an analyzed string) and no norms we need to strip the non allowed fls fields
                //from the _field_names field. They are stored as terms, so we need to create a FilterTerms implementation which skips the terms (=field names)not allowed by fls

                public FilteredTerms(Terms delegate) throws IOException {
                    super(delegate);
                }

                @Override
                public TermsEnum iterator() throws IOException {
                    return new FilteredTermsEnum(in.iterator());
                }

                private final class FilteredTermsEnum extends FilterTermsEnum {

                    public FilteredTermsEnum(TermsEnum delegate) {
                        super(delegate);
                    }

                    @Override
                    public BytesRef next() throws IOException {
                        //wind forward in the sequence of terms until we reached the end or we find a allowed term(=field name)
                        //so that calling this method never return a term which is not allowed by fls rules
                        for (BytesRef nextBytesRef = in.next(); nextBytesRef != null; nextBytesRef = in.next()) {
                            if (!dlsFlsContext.getFlsRule().isAllowed(nextBytesRef.utf8ToString())) {
                                continue;
                            } else {
                                return nextBytesRef;
                            }
                        }
                        return null;
                    }

                    @Override
                    public SeekStatus seekCeil(BytesRef text) throws IOException {
                        //Get the current seek status for a given term in the original sequence of terms
                        final SeekStatus delegateStatus = in.seekCeil(text);

                        //So delegateStatus here is either FOUND or NOT_FOUND
                        //check if the current term (=field name) is allowed
                        //If so just return current seek status
                        if (delegateStatus != SeekStatus.END && dlsFlsContext.getFlsRule().isAllowed(in.term().utf8ToString())) {
                            return delegateStatus;
                        } else if (delegateStatus == SeekStatus.END) {
                            //If we hit the end just return END 
                            return SeekStatus.END;
                        } else {
                            //If we are not at the end and the current term (=field name) is not allowed just check if
                            //we are at the end of the (filtered) iterator
                            if (this.next() != null) {
                                return SeekStatus.NOT_FOUND;
                            } else {
                                return SeekStatus.END;
                            }
                        }
                    }

                    @Override
                    public boolean seekExact(BytesRef term) throws IOException {
                        return dlsFlsContext.getFlsRule().isAllowed(term.utf8ToString()) && in.seekExact(term);
                    }

                    @Override
                    public void seekExact(long ord) throws IOException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long ord() throws IOException {
                        throw new UnsupportedOperationException();
                    }

                }
            }
        }

    }
}
