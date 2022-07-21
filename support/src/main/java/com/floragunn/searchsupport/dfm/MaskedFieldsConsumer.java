/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchsupport.dfm;

import org.apache.lucene.index.FieldInfo;

import java.io.IOException;
import java.util.function.Function;

public interface MaskedFieldsConsumer {

    void binaryMaskedField(final FieldInfo fieldInfo, final byte[] value, Function<String, Boolean> masked) throws IOException;

    void stringMaskedField(final FieldInfo fieldInfo, final String value) throws IOException;
}
