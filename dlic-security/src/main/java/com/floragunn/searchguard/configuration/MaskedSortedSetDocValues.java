package com.floragunn.searchguard.configuration;

import java.io.IOException;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

public class MaskedSortedSetDocValues extends SortedSetDocValues {

    private final SortedSetDocValues base;
    private final MaskedField maskedField;
    private final MaskedTermsDict dict;
    
    public MaskedSortedSetDocValues(SortedSetDocValues base, MaskedField maskedField) throws IOException {
        this.base = base;
        this.maskedField = maskedField;
        this.dict = MaskedTermsDict.create(base.termsEnum(), maskedField, (int) base.getValueCount());
    }
    
    @Override
    public long nextOrd() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
        return dict.lookupOrd(ord);
    }

    @Override
    public long getValueCount() {
        return dict.getValueCount();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return base.advanceExact(target);
    }

    @Override
    public int docID() {
        return base.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return base.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
       return base.advance(target);
    }

    @Override
    public long cost() {
       return base.cost();
    }

}
