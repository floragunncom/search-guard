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
/*
 * Includes code from https://github.com/opensearch-project/security/blob/6e78dd9d1a1e5e05d50b626d796bd3011ac5c530/src/main/java/org/opensearch/security/configuration/DlsFlsFilterLeafReader.java
 * which is Copyright OpenSearch Contributors
 */
package com.floragunn.searchguard.enterprise.dlsfls.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.indices.IndicesModule;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;
import com.floragunn.searchguard.support.ConfigConstants;
import com.google.common.collect.Iterators;

public class DlsFlsDirectoryReader extends FilterDirectoryReader {
    private final DlsFlsActionContext dlsFlsContext;

    public DlsFlsDirectoryReader(DirectoryReader in, DlsFlsActionContext dlsFlsContext) throws IOException {
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

        private final DlsFlsActionContext dlsFlsContext;

        SubReaderWrapper(DlsFlsActionContext dlsFlsContext) {
            this.dlsFlsContext = dlsFlsContext;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new FilterLeafReader(reader, dlsFlsContext);
        }

        private static class FilterLeafReader extends SequentialStoredFieldsLeafReader {
            private static final Logger log = LogManager.getLogger(FilterLeafReader.class);

            private static final ImmutableSet<String> META_FIELDS = ImmutableSet.of(IndicesModule.getBuiltInMetadataFields()).with("_primary_term");

            private final FieldInfos flsFieldInfos;
            private final DlsFlsActionContext dlsFlsContext;
            private final DlsGetEvaluator dlsGetEvaluator;

            FilterLeafReader(LeafReader delegate, DlsFlsActionContext dlsFlsContext) {
                super(delegate);

                this.dlsFlsContext = dlsFlsContext;

                try {
                    if (dlsFlsContext.hasFlsRestriction()) {

                        FieldInfos originalFieldInfos = delegate.getFieldInfos();
                        List<FieldInfo> restrictedFieldInfos = new ArrayList<>(originalFieldInfos.size());

                        for (FieldInfo fieldInfo : originalFieldInfos) {
                            if (isMetaField(fieldInfo.name) || dlsFlsContext.isAllowed(fieldInfo.name)) {
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
            public StoredFields storedFields() throws IOException {
                StoredFields storedFields = super.storedFields();
                if (log.isTraceEnabled()) {
                    log.trace("FilterLeafReader.storedFields()\nindex: " + dlsFlsContext.getIndexService().index().getName() + "\nfls: "
                            + dlsFlsContext.getFlsRule() + "\nfieldMasking: " + dlsFlsContext.getFieldMaskingRule());
                }

                if (dlsFlsContext.hasFlsRestriction() || dlsFlsContext.hasFieldMasking()) {
                    return new StoredFields() {
                        @Override
                        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                            storedFields.document(docID, new FlsStoredFieldVisitor(visitor, dlsFlsContext));
                        }
                    };
                } else {
                    return storedFields;
                }
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                if (log.isTraceEnabled()) {
                    log.trace("FilterLeafReader.document()\nindex: " + dlsFlsContext.getIndexService().index().getName() + "\nfls: "
                            + dlsFlsContext.getFlsRule() + "\nfieldMasking: " + dlsFlsContext.getFieldMaskingRule());
                }

                if (dlsFlsContext.hasFlsRestriction() || dlsFlsContext.hasFieldMasking()) {
                    in.document(docID, new FlsStoredFieldVisitor(visitor, dlsFlsContext));
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
            public Fields getTermVectors(int docID) throws IOException {
                Fields fields = in.getTermVectors(docID);

                if (!dlsFlsContext.hasFlsRestriction() || fields == null) {
                    return fields;
                }

                return new Fields() {

                    @Override
                    public Iterator<String> iterator() {
                        return Iterators.<String>filter(fields.iterator(), dlsFlsContext::isAllowed);
                    }

                    @Override
                    public Terms terms(String field) throws IOException {
                        if (FieldNamesFieldMapper.NAME.equals(field)) {
                            return new FilteredTerms(in.terms(field));
                        }

                        if (isMetaField(field) || dlsFlsContext.isAllowed(field)) {
                            return in.terms(field);
                        } else {
                            return null;
                        }
                    }

                    @Override
                    public int size() {
                        return flsFieldInfos.size();
                    }
                };
            }

            @Override
            public NumericDocValues getNumericDocValues(String field) throws IOException {
                return isMetaField(field) || dlsFlsContext.isAllowed(field) ? in.getNumericDocValues(field) : null;
            }

            @Override
            public BinaryDocValues getBinaryDocValues(String field) throws IOException {
                BinaryDocValues binaryDocValues = in.getBinaryDocValues(field);

                if (binaryDocValues == null) {
                    return null;
                }

                if (isMetaField(field)) {
                    return binaryDocValues;
                }

                if (!dlsFlsContext.isAllowed(field)) {
                    return null;
                }

                FieldMaskingRule.Field fieldMasking = dlsFlsContext.getFieldMaskingRule().get(field);

                if (fieldMasking == null) {
                    return binaryDocValues;
                }

                return new BinaryDocValues() {

                    @Override
                    public int nextDoc() throws IOException {
                        return binaryDocValues.nextDoc();
                    }

                    @Override
                    public int docID() {
                        return binaryDocValues.docID();
                    }

                    @Override
                    public long cost() {
                        return binaryDocValues.cost();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return binaryDocValues.advance(target);
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return binaryDocValues.advanceExact(target);
                    }

                    @Override
                    public BytesRef binaryValue() throws IOException {
                        return fieldMasking.apply(binaryDocValues.binaryValue());
                    }
                };
            }

            @Override
            public SortedDocValues getSortedDocValues(final String field) throws IOException {
                if (isMetaField(field)) {
                    return in.getSortedDocValues(field);
                }

                if (!dlsFlsContext.isAllowedButPossiblyMasked(field)) {
                    // isAllowedButPossiblyMasked is safe here because we process field masking later on
                    return null;
                }

                SortedDocValues sortedDocValues = in.getSortedDocValues(field);

                if (sortedDocValues == null) {
                    return null;
                }

                FieldMaskingRule.Field fieldMasking = dlsFlsContext.getFieldMaskingRule().get(field);

                if (fieldMasking == null) {
                    return sortedDocValues;
                }

                return new SortedDocValues() {

                    @Override
                    public int lookupTerm(BytesRef key) throws IOException {
                        return sortedDocValues.lookupTerm(key);
                    }

                    @Override
                    public TermsEnum termsEnum() throws IOException {
                        return new MaskedTermsEnum(sortedDocValues.termsEnum(), fieldMasking);
                    }

                    @Override
                    public TermsEnum intersect(CompiledAutomaton automaton) throws IOException {
                        return new MaskedTermsEnum(sortedDocValues.intersect(automaton), fieldMasking);
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return sortedDocValues.nextDoc();
                    }

                    @Override
                    public int docID() {
                        return sortedDocValues.docID();
                    }

                    @Override
                    public long cost() {
                        return sortedDocValues.cost();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return sortedDocValues.advance(target);
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return sortedDocValues.advanceExact(target);
                    }

                    @Override
                    public int ordValue() throws IOException {
                        return sortedDocValues.ordValue();
                    }

                    @Override
                    public BytesRef lookupOrd(int ord) throws IOException {
                        return fieldMasking.apply(sortedDocValues.lookupOrd(ord));
                    }

                    @Override
                    public int getValueCount() {
                        return sortedDocValues.getValueCount();
                    }
                };
            }

            @Override
            public SortedNumericDocValues getSortedNumericDocValues(final String field) throws IOException {
                return isMetaField(field) || dlsFlsContext.isAllowed(field) ? in.getSortedNumericDocValues(field) : null;
            }

            @Override
            public SortedSetDocValues getSortedSetDocValues(final String field) throws IOException {
                if (isMetaField(field)) {
                    return in.getSortedSetDocValues(field);
                }

                if (!dlsFlsContext.isAllowedButPossiblyMasked(field)) {
                    // isAllowedButPossiblyMasked is safe here because we process field masking later on
                    return null;
                }

                SortedSetDocValues sortedSetDocValues = in.getSortedSetDocValues(field);

                if (sortedSetDocValues == null) {
                    return null;
                }

                FieldMaskingRule.Field fieldMasking = dlsFlsContext.getFieldMaskingRule().get(field);

                if (fieldMasking == null) {
                    return sortedSetDocValues;
                }

                return new SortedSetDocValues() {

                    @Override
                    public int docValueCount() {
                        return sortedSetDocValues.docValueCount();
                    }

                    @Override
                    public long lookupTerm(BytesRef key) throws IOException {
                        return sortedSetDocValues.lookupTerm(key);
                    }

                    @Override
                    public TermsEnum termsEnum() throws IOException {
                        return new MaskedTermsEnum(sortedSetDocValues.termsEnum(), fieldMasking);
                    }

                    @Override
                    public TermsEnum intersect(CompiledAutomaton automaton) throws IOException {
                        return new MaskedTermsEnum(sortedSetDocValues.intersect(automaton), fieldMasking);
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return sortedSetDocValues.nextDoc();
                    }

                    @Override
                    public int docID() {
                        return sortedSetDocValues.docID();
                    }

                    @Override
                    public long cost() {
                        return sortedSetDocValues.cost();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return sortedSetDocValues.advance(target);
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return sortedSetDocValues.advanceExact(target);
                    }

                    @Override
                    public long nextOrd() throws IOException {
                        return sortedSetDocValues.nextOrd();
                    }

                    @Override
                    public BytesRef lookupOrd(long ord) throws IOException {
                        return fieldMasking.apply(sortedSetDocValues.lookupOrd(ord));
                    }

                    @Override
                    public long getValueCount() {
                        return sortedSetDocValues.getValueCount();
                    }
                };
            }

            @Override
            public NumericDocValues getNormValues(final String field) throws IOException {
                return isMetaField(field) || dlsFlsContext.isAllowed(field) ? in.getNormValues(field) : null;
            }

            @Override
            public PointValues getPointValues(String field) throws IOException {
                // this is not a string so we cannot hash this, therefore we return null
                return isMetaField(field) || dlsFlsContext.isAllowed(field) ? in.getPointValues(field) : null;
            }

            @Override
            public Terms terms(String field) throws IOException {
                if (FieldNamesFieldMapper.NAME.equals(field)) {
                    return new FilteredTerms(in.terms(field));
                }

                if (isMetaField(field)) {
                    return in.terms(field);
                }

                if (dlsFlsContext.isAllowed(field)) {
                    return in.terms(field);
                } else {
                    return null;
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

            private boolean isMetaField(String field) {
                return META_FIELDS.contains(field);
            }

            private static class MaskedTermsEnum extends TermsEnum {

                private final TermsEnum delegate;
                private final FieldMaskingRule.Field fieldMasking;

                public MaskedTermsEnum(TermsEnum delegate, FieldMaskingRule.Field fieldMasking) {
                    this.delegate = delegate;
                    this.fieldMasking = fieldMasking;
                }

                @Override
                public BytesRef next() throws IOException {
                    return delegate.next(); //no masking here
                }

                @Override
                public AttributeSource attributes() {
                    return delegate.attributes();
                }

                @Override
                public boolean seekExact(BytesRef text) throws IOException {
                    return delegate.seekExact(text);
                }

                @Override
                public SeekStatus seekCeil(BytesRef text) throws IOException {
                    return delegate.seekCeil(text);
                }

                @Override
                public void seekExact(long ord) throws IOException {
                    delegate.seekExact(ord);
                }

                @Override
                public void seekExact(BytesRef term, TermState state) throws IOException {
                    delegate.seekExact(term, state);
                }

                @Override
                public BytesRef term() throws IOException {
                    return fieldMasking.apply(delegate.term());
                }

                @Override
                public long ord() throws IOException {
                    return delegate.ord();
                }

                @Override
                public int docFreq() throws IOException {
                    return delegate.docFreq();
                }

                @Override
                public long totalTermFreq() throws IOException {
                    return delegate.totalTermFreq();
                }

                @Override
                public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                    return delegate.postings(reuse, flags);
                }

                @Override
                public ImpactsEnum impacts(int flags) throws IOException {
                    return delegate.impacts(flags);
                }

                @Override
                public TermState termState() throws IOException {
                    return delegate.termState();
                }

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
                public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                    if (log.isTraceEnabled()) {
                        log.trace("DlsFlsStoredFieldsReader.visitDocument()\nindex: " + dlsFlsContext.getIndexService().index().getName() + "\nfls: "
                                + dlsFlsContext.getFlsRule() + "\nfieldMasking: " + dlsFlsContext.getFieldMaskingRule());
                    }

                    if (dlsFlsContext.hasFlsRestriction() || dlsFlsContext.hasFieldMasking()) {
                        visitor = new FlsStoredFieldVisitor(visitor, dlsFlsContext);
                    }

                    delegate.document(docID, visitor);
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

                public FilteredTerms(Terms delegate) {
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
                            if (isAllowed(nextBytesRef)) {
                                return nextBytesRef;
                            } else {
                                continue;
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
                        if (delegateStatus != SeekStatus.END && isAllowed(in.term())) {
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
                        return isAllowed(term) && in.seekExact(term);
                    }

                    @Override
                    public void seekExact(long ord) throws IOException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long ord() throws IOException {
                        throw new UnsupportedOperationException();
                    }

                    private boolean isAllowed(BytesRef term) {
                        String fieldName = term.utf8ToString();
                        return isMetaField(fieldName) || dlsFlsContext.isAllowed(fieldName);
                    }

                }
            }
        }

    }
}
