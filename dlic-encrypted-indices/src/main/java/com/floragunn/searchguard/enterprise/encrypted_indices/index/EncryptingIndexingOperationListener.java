package com.floragunn.searchguard.enterprise.encrypted_indices.index;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperationsFactory;
import com.google.common.io.CharStreams;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesModule;

import java.util.ListIterator;

public class EncryptingIndexingOperationListener implements IndexingOperationListener {

    private final GuiceDependencies guiceDependencies;

    private final CryptoOperationsFactory cryptoOperationsFactory;

    private final ThreadContext threadContext;

    protected static final ImmutableSet<String> META_FIELDS = ImmutableSet.of(IndicesModule.getBuiltInMetadataFields()).without("_source").with("_primary_term");

    public EncryptingIndexingOperationListener(GuiceDependencies guiceDependencies, CryptoOperationsFactory cryptoOperationsFactory) {
        this.guiceDependencies = guiceDependencies;
        this.cryptoOperationsFactory = cryptoOperationsFactory;
        threadContext = this.guiceDependencies.getTransportService().getThreadPool().getThreadContext();
    }

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index _operation) {
        try {
            return preIndex0(shardId, _operation);
        } catch (Exception e) {
            _operation.parsedDoc().docs().clear();
            _operation.parsedDoc().setSource(null, null);
            throw new RuntimeException(e);
        }
    }
    private Engine.Index preIndex0(ShardId shardId, Engine.Index _operation) throws Exception {
        //return value is ignored!!

        final IndexService indexService = guiceDependencies.getIndicesService().indexService(shardId.getIndex());

        if (indexService == null) {
            throw new RuntimeException("indexService must not be null");
        }

        final CryptoOperations cryptoOperations = cryptoOperationsFactory.createCryptoOperations(indexService.getIndexSettings());

        if (cryptoOperations == null) {
            return null;
        }

        System.out.println("###### preIndex "+shardId.getIndex().getUUID()+" "+_operation.isRetry()+" "+_operation.operationType()+" "+_operation.origin());
        final BytesRef encryptedSource;

        if(_operation.origin() != Engine.Operation.Origin.PRIMARY && _operation.origin() != Engine.Operation.Origin.REPLICA){
            final BytesRef originalSource = _operation.source().toBytesRef();
            //assert Cryptor.dummy().isSourceEncrypted(originalSource);

            BytesReference dec = new BytesArray(cryptoOperations.decryptSource(originalSource, _operation.id()));

            ParsedDocument decryptedParsedDocument = indexService.mapperService().documentMapper().parse(
                    new SourceToParse(shardId.getIndexName(), _operation.id(),dec , XContentType.JSON, _operation.routing()));

            _operation.parsedDoc().docs().clear();
            _operation.parsedDoc().docs().addAll(decryptedParsedDocument.docs());
            encryptedSource = originalSource;

        } else {
            encryptedSource = encryptSourceField(_operation, cryptoOperations);
        }

        final ListIterator<ParseContext.Document> documentListIterator = _operation.docs().listIterator();

        while (documentListIterator.hasNext()) {
            final ParseContext.Document document = documentListIterator.next();

            final ListIterator<IndexableField> fieldListIterator = document.getFields().listIterator();

            while (fieldListIterator.hasNext()) {

                final IndexableField f = fieldListIterator.next();

                if (f instanceof Field) {

                    final Field field = (Field) f;
                    encryptField(field, encryptedSource, cryptoOperations, _operation.id());
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

    private void encryptField(Field field, BytesRef encryptedSource, CryptoOperations cryptoOperations, String id) throws Exception {

        if(META_FIELDS.contains(field.name())) {
            return;
        }

        if (field.fieldType().stored() && field.fieldType().indexOptions() != IndexOptions.NONE) {
            throw new RuntimeException("Stored indexed fields are not supported yet ("+field.name()+")");
        }

        if (field.fieldType().stored()) {
            System.out.println("not indexed encrypt field "+field.name()+" "+field.fieldType()+" "+field.fieldType().indexOptions());

            if(field.numericValue() != null) {
                Number number = field.numericValue();
                System.out.println("number of type "+number.getClass());
            } else if(field.stringValue() != null) {
                field.setStringValue(cryptoOperations.encryptString(field.stringValue(), field.name(), id));
            } else if (field.readerValue() != null) {
                field.setReaderValue(cryptoOperations.encryptReader(field.readerValue(), field.name(), id));
            } else if (field.binaryValue() != null) {
                if(field.name().equals("_source")) {
                    //assert Cryptor.dummy().isSourceEncrypted(encryptedSource);
                    field.setBytesValue(encryptedSource);
                } else {
                    field.setBytesValue(cryptoOperations.encryptBytesRef(field.binaryValue(), field.name(), id));
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

    private BytesRef encryptSourceField(Engine.Index operation, CryptoOperations cryptoOperations) throws Exception {
        final BytesRef source = operation.parsedDoc().source().toBytesRef();
        final BytesRef encryptedSource = cryptoOperations.encryptSource(source, operation.id());
        operation.parsedDoc().setSource(new BytesArray(encryptedSource), operation.parsedDoc().getXContentType());
        return encryptedSource;
    }
}
