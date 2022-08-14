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

import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

class DecryptingStoredFieldVisitor extends StoredFieldVisitor {
    private static final Logger log = LogManager.getLogger(DecryptingStoredFieldVisitor.class);
    private final StoredFieldVisitor delegate;

    private final CryptoOperations cryptoOperations;
    private String id = null;

    public DecryptingStoredFieldVisitor(StoredFieldVisitor delegate, CryptoOperations cryptoOperations) {
        super();
        this.delegate = delegate;
        this.cryptoOperations = cryptoOperations;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        try {
            if(EncryptingIndexingOperationListener.META_FIELDS.contains(fieldInfo.name)) {
                if(fieldInfo.name.equals("_id")) {
                    //id = Base64.getEncoder().encodeToString(value);
                    //id = id.replace('/','_');
                    id = Uid.decodeId(value);
                }
                delegate.binaryField(fieldInfo, value);
            } else if (fieldInfo.name.equals("_source")){
                delegate.binaryField(fieldInfo, cryptoOperations.decryptSourceAsByteArray(value, id));
            } else {
                delegate.binaryField(fieldInfo, cryptoOperations.decryptByteArray(value, fieldInfo.name, id));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public void stringField(final FieldInfo fieldInfo, final String value) throws IOException {
        try {
            if(EncryptingIndexingOperationListener.META_FIELDS.contains(fieldInfo.name)) {
                delegate.stringField(fieldInfo, value);
            } else {
                delegate.stringField(fieldInfo, cryptoOperations.decryptString(value, fieldInfo.name, id));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public void intField(final FieldInfo fieldInfo, final int value) throws IOException {
        delegate.intField(fieldInfo, value);
    }

    @Override
    public void longField(final FieldInfo fieldInfo, final long value) throws IOException {
        delegate.longField(fieldInfo, value);
    }

    @Override
    public void floatField(final FieldInfo fieldInfo, final float value) throws IOException {
        delegate.floatField(fieldInfo, value);
    }

    @Override
    public void doubleField(final FieldInfo fieldInfo, final double value) throws IOException {
        delegate.doubleField(fieldInfo, value);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return delegate.needsField(fieldInfo);
    }

    @Override
    public boolean equals(final Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

}