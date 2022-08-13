package com.floragunn.searchguard.enterprise.encrypted_indices.codec;

import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

public class EncryptedDefaultCodec extends FilterCodec {

    /**
     * this is currently not working
     *
     * idea is to encrypt the _source into something which is not json
     * and to decrypt on retrieval
     *
     * seems to conflict with shard replication where we miss a hook to decrypt
     * so that
     *
     * Caused by: java.lang.NullPointerException
     * 	at java.util.Objects.requireNonNull(Objects.java:208) ~[?:?]
     * 	at org.opensearch.index.mapper.SourceToParse.<init>(SourceToParse.java:65) ~[opensearch-2.0.0.jar:2.0.0]
     * 	at org.opensearch.index.shard.IndexShard.applyTranslogOperation(IndexShard.java:1752) ~[opensearch-2.0.0.jar:2.0.0]
     * 	at org.opensearch.index.shard.IndexShard.applyTranslogOperation(IndexShard.java:1723) ~[opensearch-2.0.0.jar:2.0.0]
     * 	at org.opensearch.indices.recovery.RecoveryTarget.lambda$indexTranslogOperations$2(RecoveryTarget.java:396) ~[opensearch-2.0.0.jar:2.0.0]
     * 	at org.opensearch.action.ActionListener.completeWith(ActionListener.java:342) ~[opensearch-2.0.0.jar:2.0.0]
     * 	at org.opensearch.indices.recovery.RecoveryTarget.indexTranslogOperations(RecoveryTarget.java:371) ~[opensearch-2.0.0.jar:2.0.0]
     * 	at org.opensearch.indices.recovery.PeerRecoveryTargetService$TranslogOperationsRequestHandler.performTranslogOps(PeerRecoveryTargetService.java:462) ~[opensearch-2.0.0.jar:2.0.0]
     *
     * because there is not starting '{' to detect the xcontent type
     *
     */

    private final CryptoOperations cryptoOperations = null;//new DummyCryptoOperations();

    public EncryptedDefaultCodec() {
        super("encc", new SimpleTextCodec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return new StoredFieldsFormat() {
            @Override
            public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
                org.apache.lucene.codecs.StoredFieldsReader d = delegate.storedFieldsFormat().fieldsReader(directory, si, fn, context);
                return new PrimitiveDecryptingStoredFieldsReader(d, cryptoOperations);
            }

            @Override
            public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
                StoredFieldsWriter d = delegate.storedFieldsFormat().fieldsWriter(directory,si,context);
                return new StoredFieldsWriter() {
                    @Override
                    public void startDocument() throws IOException {
                        d.startDocument();
                    }

                    @Override
                    public void writeField(FieldInfo info, IndexableField field) throws IOException {
                        if(info.name.equals("_source")) {
                            //((Field) field).setBytesValue(cryptoOperations.encryptBytesRef(field.binaryValue()));
                        }

                        d.writeField(info, field);


                    }

                    @Override
                    public void finish(int numDocs) throws IOException {
                        d.finish(numDocs);
                    }

                    @Override
                    public void close() throws IOException {
                        d.close();
                    }

                    @Override
                    public long ramBytesUsed() {
                        return d.ramBytesUsed();
                    }
                };
            }
        };
    }

    private static class PrimitiveDecryptingStoredFieldsReader extends StoredFieldsReader {

        private final StoredFieldsReader delegate;
        private final CryptoOperations cryptoOperations;

        private PrimitiveDecryptingStoredFieldsReader(StoredFieldsReader delegate, CryptoOperations cryptoOperations) {
            this.delegate = delegate;
            this.cryptoOperations = cryptoOperations;
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            return delegate;
        }

        @Override
        public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {

            delegate.visitDocument(docID, new StoredFieldVisitor() {
                @Override
                public Status needsField(FieldInfo fieldInfo) throws IOException {
                    return visitor.needsField(fieldInfo);
                }

                @Override
                public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                    if(fieldInfo.name.equals("_source")) {
                        //System.out.println("codec readdec "+new String(cryptoOperations.decryptByteArray(value)));
                        //visitor.binaryField(fieldInfo, cryptoOperations.decryptByteArray(value));
                        new Exception("dec").printStackTrace();
                    } else {
                        visitor.binaryField(fieldInfo, value);
                    }

                }
            });
        }

        @Override
        public StoredFieldsReader clone() {
            return new PrimitiveDecryptingStoredFieldsReader(delegate, cryptoOperations);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    };
}
