package com.floragunn.searchguard.configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

public class MaskedTermsDict extends BaseTermsEnum {

    private int valueCount;
    private final Entry[] entries;
    private final TermsEnum base;
    private int current = -1;

    MaskedTermsDict(ArrayList<Entry> entries, TermsEnum base) throws IOException {
        this.entries = entries.toArray(new Entry[entries.size()]);
        this.base = base;
        this.valueCount = entries.size();
    }

    public int getValueCount() {
        return valueCount;
    }

    
    public BytesRef lookupOrd(long ord) throws IOException {
        if (ord >= valueCount) {
            throw new IOException("ord " + ord + " is out of bounds: " + valueCount);
        }
        
        return this.entries[(int) ord].maskedTerm;
    }
    
    @Override
    public BytesRef next() throws IOException {
        if (current < 0) {
            current = 0;
        } else {
            current++;
        }

        if (current >= valueCount) {
            current = -1;
            return null;
        }

        return entries[current].maskedTerm;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        int pos = Arrays.binarySearch(this.entries, new Entry(text, 0));

        if (pos >= 0) {
            current = pos;
            return SeekStatus.FOUND;
        } else {
            int insertionPoint = -pos - 1;

            if (insertionPoint == entries.length) {
                return SeekStatus.END;
            } else {
                current = insertionPoint;
                return SeekStatus.NOT_FOUND;
            }
        }
    }

    @Override
    public void seekExact(long ord) throws IOException {
        if (ord >= valueCount) {
            throw new IOException("ord " + ord + " is out of bounds: " + valueCount);
        }

        current = (int) ord;
    }

    @Override
    public BytesRef term() throws IOException {
        return entries[current].maskedTerm;
    }

    @Override
    public long ord() throws IOException {
        return current;
    }

    @Override
    public int docFreq() throws IOException {
        Entry currentEntry = entries[current];

        base.seekExact(currentEntry.originalOrd1);

        int result = base.docFreq();

        if (currentEntry.originalOrd2toN != null) {
            for (int i = 0; i < currentEntry.originalOrd2toN.length; i++) {
                base.seekExact(currentEntry.originalOrd2toN[i]);
                result += base.docFreq();
            }
        }

        return result;
    }

    @Override
    public long totalTermFreq() throws IOException {
        Entry currentEntry = entries[current];

        base.seekExact(currentEntry.originalOrd1);

        long result = base.totalTermFreq();

        if (currentEntry.originalOrd2toN != null) {
            for (int i = 0; i < currentEntry.originalOrd2toN.length; i++) {
                base.seekExact(currentEntry.originalOrd2toN[i]);
                result += base.totalTermFreq();
            }
        }

        return result;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        Entry currentEntry = entries[current];

        if (currentEntry.originalOrdCount == 1) {
            base.seekExact(currentEntry.originalOrd1);
            return base.postings(reuse, flags);
        } else {
          //  return new MergedPostingsEnum(currentEntry, base);
            throw new UnsupportedOperationException();
        }        
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        Entry currentEntry = entries[current];

        if (currentEntry.originalOrdCount == 1) {
            base.seekExact(currentEntry.originalOrd1);
            return base.impacts(flags);
        } else {
          //  return new MergedPostingsEnum(currentEntry, base);
            throw new UnsupportedOperationException();
        }  
    }

    public static MaskedTermsDict create(TermsEnum base, MaskedField maskedField, int sizeHint) throws IOException {
        if (sizeHint < 10) {
            sizeHint = 10;
        } else if (sizeHint > 10000) {
            sizeHint = 10000;
        }

        ArrayList<Entry> entries = new ArrayList<>(sizeHint);

        for (BytesRef term = base.next(); term != null; term = base.next()) {
            BytesRef maskedTerm = maskedField.mask(term);
            entries.add(new Entry(maskedTerm, base.ord()));
        }

        entries.sort(null);

        return new MaskedTermsDict(mergeEntries(entries), base);
    }

    private static int countValues(ArrayList<Entry> entries) {
        int result = 0;

        Entry prevEntry = null;

        for (Entry entry : entries) {

            if (prevEntry == null || !entry.maskedTerm.equals(prevEntry.maskedTerm)) {
                result++;
            }

            prevEntry = entry;
        }

        return result;
    }

    private static ArrayList<Entry> mergeEntries(ArrayList<Entry> entries) {
        int valueCount = 0;

        Entry prevEntry = null;
        Entry groupHeadEntry = null;

        for (Entry entry : entries) {

            if (prevEntry == null) {
                prevEntry = entry;
                valueCount++;
                continue;
            }

            if (prevEntry.maskedTerm.equals(entry.maskedTerm)) {
                if (groupHeadEntry == null) {
                    groupHeadEntry = prevEntry;
                }

                groupHeadEntry.originalOrdCount++;
                entry.originalOrdCount = 0;
            } else {
                groupHeadEntry = null;
                valueCount++;
            }

            prevEntry = entry;
        }

        if (valueCount == entries.size()) {
            return entries;
        }

        ArrayList<Entry> result = new ArrayList<>(valueCount);

        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);

            if (entry.originalOrdCount == 1) {
                result.add(entry);
                continue;
            }

            assert entry.originalOrdCount != 0;

            long[] originalOrd2toN = new long[entry.originalOrdCount - 1];

            for (int k = 0; k < entry.originalOrdCount - 1; k++) {
                originalOrd2toN[k] = entries.get(i + k + 1).originalOrd1;
            }

            entry.originalOrd2toN = originalOrd2toN;
            result.add(entry);

            i += entry.originalOrdCount - 1;
        }

        return result;
    }

    private static class Entry implements Comparable<Entry> {
        final BytesRef maskedTerm;
        private long originalOrd1;
        private long[] originalOrd2toN;
        private int originalOrdCount;

        Entry(BytesRef maskedTerm, long originalOrd) {
            this.maskedTerm = maskedTerm;
            this.originalOrd1 = originalOrd;
            this.originalOrdCount = 1;
        }

        void addOriginalOrd(long originalOrd) {
            if (this.originalOrd2toN == null) {
                this.originalOrd2toN = new long[4];
            } else if (this.originalOrdCount > this.originalOrd2toN.length) {
                long[] newOriginalOrd2toN = new long[this.originalOrd2toN.length + 16];
                System.arraycopy(this.originalOrd2toN, 0, newOriginalOrd2toN, 0, this.originalOrd2toN.length);
                this.originalOrd2toN = newOriginalOrd2toN;
            }

            this.originalOrd2toN[this.originalOrdCount - 1] = originalOrd;
            this.originalOrdCount++;
        }

        @Override
        public int compareTo(Entry o) {
            return this.maskedTerm.compareTo(o.maskedTerm);
        }

    }

    
    private static class MergedPostingsEnum extends PostingsEnum {

        private final Entry entry;
        private final TermsEnum base;
        private int currentOriginalOrd = 0;
        
        MergedPostingsEnum(Entry entry, TermsEnum base) throws IOException {
            this.entry = entry;
            this.base = base;
            base.seekExact(entry.originalOrd1);
        }
        
        @Override
        public int freq() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int nextPosition() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int startOffset() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int endOffset() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public BytesRef getPayload() throws IOException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int docID() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int nextDoc() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int advance(int target) throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long cost() {
            // TODO Auto-generated method stub
            return 0;
        }
        
    }
}
