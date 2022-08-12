package com.floragunn.searchguard.enterprise.encrypted_indices.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

public class EncryptedDefaultCodec extends FilterCodec {

    public EncryptedDefaultCodec() {
        super("encc", new SimpleTextCodec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return new StoredFieldsFormat() {
            @Override
            public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
                return delegate.storedFieldsFormat().fieldsReader(directory, si, fn, context);
            }

            @Override
            public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
                StoredFieldsWriter d = delegate.storedFieldsFormat().fieldsWriter(directory,si,context);
                return new StoredFieldsWriter() {
                    @Override
                    public void startDocument() throws IOException {
                        System.out.println("  ---- Start document ----");
                        d.startDocument();
                    }

                    @Override
                    public void writeField(FieldInfo info, IndexableField field) throws IOException {
                        System.out.println("  ---- Write field "+info.name+" ----");
                        if(info.name.equals("enc")) {
                            new Exception("enc written "+field.stringValue()).printStackTrace();
                        }
                        else  //do not write the enc field????? //TODO hacky
                        d.writeField(info, field);

                    }

                    @Override
                    public void finish(int numDocs) throws IOException {
                        System.out.println("  ---- Finish document ----");
                        d.finish(numDocs);
                    }

                    @Override
                    public void close() throws IOException {
                        d.close();
                    }

                    @Override
                    public long ramBytesUsed() {
                        return  d.ramBytesUsed();
                    }
                };
            }
        };
    }
}
