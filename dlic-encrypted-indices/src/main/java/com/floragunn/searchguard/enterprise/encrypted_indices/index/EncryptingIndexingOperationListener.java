package com.floragunn.searchguard.enterprise.encrypted_indices.index;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.DummyCryptoOperations;
import com.google.common.io.CharStreams;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesModule;

import java.io.IOException;
import java.util.ListIterator;

public class EncryptingIndexingOperationListener implements IndexingOperationListener {

    private final GuiceDependencies guiceDependencies;

    private final CryptoOperations cryptoOperations = new DummyCryptoOperations();

    protected static final ImmutableSet<String> META_FIELDS = ImmutableSet.of(IndicesModule.getBuiltInMetadataFields()).without("_source").with("_primary_term");

    public EncryptingIndexingOperationListener(GuiceDependencies guiceDependencies) {
        this.guiceDependencies = guiceDependencies;
    }

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index _operation) {
        //return value is ignored!!

        final IndexService indexService = guiceDependencies.getIndicesService().indexService(shardId.getIndex());

        if (indexService == null || !indexService.getIndexSettings().getSettings().getAsBoolean("index.encryption_enabled", false)) {
            return null;
        }

        System.out.println("###### preIndex "+shardId.getIndex().getUUID()+" "+_operation.isRetry()+" "+_operation.operationType()+" "+_operation.origin());
        final BytesRef encryptedSource;

        if(_operation.origin() != Engine.Operation.Origin.PRIMARY && _operation.origin() != Engine.Operation.Origin.REPLICA){
            final BytesRef originalSource = _operation.source().toBytesRef();
            //assert Cryptor.dummy().isSourceEncrypted(originalSource);

            BytesReference dec = new BytesArray(cryptoOperations.decryptSource(originalSource));

            ParsedDocument decryptedParsedDocument = indexService.mapperService().documentMapper().parse(
                    new SourceToParse(shardId.getIndexName(), _operation.id(),dec , XContentType.JSON, _operation.routing()));

            _operation.parsedDoc().docs().clear();
            _operation.parsedDoc().docs().addAll(decryptedParsedDocument.docs());
            encryptedSource = originalSource;

        } else {
            encryptedSource = encryptSourceField(_operation);
        }

        final ListIterator<ParseContext.Document> documentListIterator = _operation.docs().listIterator();

        while (documentListIterator.hasNext()) {
            final ParseContext.Document document = documentListIterator.next();

            final ListIterator<IndexableField> fieldListIterator = document.getFields().listIterator();

            while (fieldListIterator.hasNext()) {

                final IndexableField f = fieldListIterator.next();

                if (f instanceof Field) {

                    final Field field = (Field) f;
                    try {
                        encryptField(field, encryptedSource);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                } else {
                    System.out.println("Unhandled field "+f.name()+" "+f.getClass());
                    //TODO
                }
            }
        }

        //return does not matter because it is ignored
        //by composite listener
        return null;
    }

    private void encryptField(Field field, BytesRef encryptedSource) throws IOException {

        if(META_FIELDS.contains(field.name())) {
            return;
        }

        if (field.fieldType().stored() && field.fieldType().indexOptions() != IndexOptions.NONE) {
            throw new RuntimeException("Stored indexed fields are not supported yet");
        }

        if (field.fieldType().stored()) {
            System.out.println("not indexed encrypt field "+field.name()+" "+field.fieldType()+" "+field.fieldType().indexOptions());

            if(field.numericValue() != null) {
                Number number = field.numericValue();
                System.out.println("number of type "+number.getClass());
            } if(field.stringValue() != null) {
                field.setStringValue(cryptoOperations.encryptString(field.stringValue()));
            } else if (field.readerValue() != null) {
                field.setReaderValue(cryptoOperations.encryptReader(field.readerValue()));
            } else if (field.binaryValue() != null) {
                if(field.name().equals("_source")) {
                    //assert Cryptor.dummy().isSourceEncrypted(encryptedSource);
                    field.setBytesValue(encryptedSource);
                } else {
                    field.setBytesValue(cryptoOperations.encryptBytesRef(field.binaryValue()));
                }

            } else {
                System.out.println("nix for "+field.getClass());
                throw new RuntimeException();
            }
        } else if (field.fieldType().indexOptions() != IndexOptions.NONE) {
            System.out.println("Indexed but not stored field "+field.name()+" will not be encrypted. Value:");

                //unencrypted token stream
                System.out.println(field.getClass() + " and tokenStreamValue " + field.tokenStreamValue());
                if (field.readerValue() != null) {
                    System.out.println(CharStreams.toString(field.readerValue()));
                } else if (field.stringValue() != null) {
                    System.out.println(field.stringValue());
                }
        }
    }

    private BytesRef encryptSourceField(Engine.Index operation) {
        final BytesRef source = operation.parsedDoc().source().toBytesRef();
        final BytesRef encryptedSource = cryptoOperations.encryptSource(source);
        operation.parsedDoc().setSource(new BytesArray(encryptedSource), operation.parsedDoc().getXContentType());
        return encryptedSource;
    }
}
