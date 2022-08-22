package com.floragunn.searchguard.enterprise.encrypted_indices.index;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.GuiceDependencies;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperationsFactory;
import com.google.common.io.CharStreams;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.KeywordFieldMapper;
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

    private final Client client;

    protected static final ImmutableSet<String> META_FIELDS = ImmutableSet.of(IndicesModule.getBuiltInMetadataFields()).without("_source").with("_primary_term");

    public EncryptingIndexingOperationListener(Client client, GuiceDependencies guiceDependencies, CryptoOperationsFactory cryptoOperationsFactory) {
        this.guiceDependencies = guiceDependencies;
        this.cryptoOperationsFactory = cryptoOperationsFactory;
        threadContext = this.guiceDependencies.getTransportService().getThreadPool().getThreadContext();
        this.client = client;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        client.admin().indices().flush(new FlushRequest(shardId.getIndexName()).indicesOptions(IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED).force(true)).actionGet();
        client.admin().indices().forceMerge(new ForceMergeRequest(shardId.getIndexName()).indicesOptions(IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED).maxNumSegments(1)).actionGet();
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
        System.out.println(_operation.source().utf8ToString());
        final BytesRef encryptedSource;

        //_operation.source() is for the translog

        //Engine.Operation.Origin.PRIMARY.isRecovery()
        //recovery is the process of syncing a replica shard from a primary shard

        //Engine.Operation.Origin.LOCAL_RESET.isFromTranslog() {

        if(_operation.origin() != Engine.Operation.Origin.PRIMARY && _operation.origin() != Engine.Operation.Origin.REPLICA){

            if(1==1) {
                _operation.parsedDoc().docs().clear();
                _operation.parsedDoc().setSource(null, null);
                new RuntimeException("origin "+ _operation.origin()+" not supported").printStackTrace();
            }

            final BytesRef originalSource = _operation.source().toBytesRef();
            //assert Cryptor.dummy().isSourceEncrypted(originalSource);

            //for indexing terms/tokens
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

                //f.fieldType().docValuesType()
                //NONE,NUMERIC,BINARY,SORTED,SORTED_NUMERIC,SORTED_SET,

                //inverted index
                //f.fieldType().indexOptions()
                //NONE,DOCS,DOCS_AND_FREQS,DOCS_AND_FREQS_AND_POSITIONS,DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,

                //inverted index
                //should be analyzed by analyzer
                //needs indexOptions() != NONE
                //f.fieldType().tokenized()

                //f.fieldType().omitNorms()

                //inverted index
                //needs indexOptions() != NONE
                //f.fieldType().storeTermVectorOffsets();
                //f.fieldType().storeTermVectorPositions();
                //f.fieldType().storeTermVectors()
                //TODO realtime, payloads

                //store value
                //f.fieldType().stored()

                //TODO dates

                if (f instanceof Field) {

                    final Field field = (Field) f;

                    System.out.println("Field: "+field.name()+" "+field.getClass()+" "+field.fieldType()+" "+field.fieldType().docValuesType());

                    if(META_FIELDS.contains(field.name())) {
                        continue;
                    }

                    if (field.fieldType().stored() && field.fieldType().indexOptions() != IndexOptions.NONE) {
                        throw new RuntimeException("Stored indexed fields are not supported yet ("+field.name()+")");
                    }

                    if(field instanceof KeywordFieldMapper.KeywordField) {
                        fieldListIterator.remove();
                        fieldListIterator.add(new KeywordFieldMapper.KeywordField(
                                field.name(),
                                cryptoOperations.hashBytesRef(field.binaryValue()),
                                (FieldType) field.fieldType()

                        ));
                    } else {
                        encryptField(field, encryptedSource, cryptoOperations, _operation.id());
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

    private void encryptField(Field field, BytesRef encryptedSource, CryptoOperations cryptoOperations, String id) throws Exception {



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
        } else if (field.fieldType().docValuesType() == DocValuesType.BINARY) {
            field.setBytesValue(cryptoOperations.encryptBytesRef(field.binaryValue(), field.name(), id));
        } else if (field.fieldType().docValuesType() == DocValuesType.SORTED) {
            field.setBytesValue(cryptoOperations.encryptBytesRef(field.binaryValue(), field.name(), id));
        } else if (field.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
            field.setBytesValue(cryptoOperations.encryptBytesRef(field.binaryValue(), field.name(), id));
        } else {
            //the non-stored indexed fields
            System.out.println("skip "+field.name());
        }

    }

    private BytesRef encryptSourceField(Engine.Index operation, CryptoOperations cryptoOperations) throws Exception {
        final BytesRef source = operation.parsedDoc().source().toBytesRef();
        final BytesRef encryptedSource = cryptoOperations.encryptSource(source, operation.id());
        operation.parsedDoc().setSource(new BytesArray(encryptedSource), operation.parsedDoc().getXContentType());
        return encryptedSource;
    }
}
