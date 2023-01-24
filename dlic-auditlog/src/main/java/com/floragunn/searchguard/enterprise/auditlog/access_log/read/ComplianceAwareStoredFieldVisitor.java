/*
  * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.auditlog.access_log.read;

import com.floragunn.searchsupport.dfm.MaskedFieldsConsumer;
import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

public class ComplianceAwareStoredFieldVisitor extends StoredFieldVisitor implements MaskedFieldsConsumer {
    private final StoredFieldVisitor delegate;
    private FieldReadCallback fieldReadCallback;

    public ComplianceAwareStoredFieldVisitor(StoredFieldVisitor delegate, ReadLogContext context) {
        super();
        this.delegate = delegate;
        this.fieldReadCallback = new FieldReadCallback(context);
    }

    @Override
    public void binaryField(final FieldInfo fieldInfo, final byte[] value) throws IOException {
        fieldReadCallback.binaryFieldRead(fieldInfo, value, (f) -> false);
        delegate.binaryField(fieldInfo, value);
    }

    @Override
    public Status needsField(final FieldInfo fieldInfo) throws IOException {
        return delegate.needsField(fieldInfo);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public void stringField(final FieldInfo fieldInfo, final byte[] value) throws IOException {
        fieldReadCallback.stringFieldRead(fieldInfo, value, (f) -> false);
        delegate.stringField(fieldInfo, value);
    }

    @Override
    public void intField(final FieldInfo fieldInfo, final int value) throws IOException {
        fieldReadCallback.numericFieldRead(fieldInfo, value);
        delegate.intField(fieldInfo, value);
    }

    @Override
    public void longField(final FieldInfo fieldInfo, final long value) throws IOException {
        fieldReadCallback.numericFieldRead(fieldInfo, value);
        delegate.longField(fieldInfo, value);
    }

    @Override
    public void floatField(final FieldInfo fieldInfo, final float value) throws IOException {
        fieldReadCallback.numericFieldRead(fieldInfo, value);
        delegate.floatField(fieldInfo, value);
    }

    @Override
    public void doubleField(final FieldInfo fieldInfo, final double value) throws IOException {
        fieldReadCallback.numericFieldRead(fieldInfo, value);
        delegate.doubleField(fieldInfo, value);
    }

    @Override
    public boolean equals(final Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public void finished() {
        fieldReadCallback.finished();
        fieldReadCallback = null;
    }

    @Override
    public void binaryMaskedField(FieldInfo fieldInfo, byte[] value, Function<String, Boolean> masked) throws IOException {
        fieldReadCallback.binaryFieldRead(fieldInfo, value, masked);
        delegate.binaryField(fieldInfo, value);
    }

    @Override
    public void stringMaskedField(FieldInfo fieldInfo, byte[] value) throws IOException {
        fieldReadCallback.stringFieldRead(fieldInfo, value, (f) -> true);
        delegate.stringField(fieldInfo, value);
    }
}
