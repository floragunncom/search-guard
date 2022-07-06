package com.floragunn.searchsupport.dfm;

import org.apache.lucene.index.FieldInfo;

import java.io.IOException;
import java.util.function.Function;

public interface MaskedFieldsConsumer {

    void binaryMaskedField(final FieldInfo fieldInfo, final byte[] value, Function<String, Boolean> masked ) throws IOException;
    void stringMaskedField(final FieldInfo fieldInfo, final String value) throws IOException;

}
