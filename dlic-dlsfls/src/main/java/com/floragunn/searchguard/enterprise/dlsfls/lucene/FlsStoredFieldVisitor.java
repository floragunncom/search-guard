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
/*
 * Includes code from https://github.com/opensearch-project/security/blob/70591197c705ca6f42f765186a05837813f80ff3/src/main/java/org/opensearch/security/privileges/dlsfls/FlsStoredFieldVisitor.java
 * which is Copyright OpenSearch Contributors
 */
package com.floragunn.searchguard.enterprise.dlsfls.lucene;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Deque;

import com.floragunn.searchsupport.dfm.MaskedFieldsConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.indices.IndicesModule;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;

/**
 * Applies FLS and field masking while reading documents
 */
class FlsStoredFieldVisitor extends StoredFieldVisitor {
    private static final Logger log = LogManager.getLogger(FlsStoredFieldVisitor.class);

    /**
     * Meta fields like _id get always included, regardless of settings
     */
    private static final  ImmutableSet<String> META_FIELDS = ImmutableSet.of(IndicesModule.getBuiltInMetadataFields()).with("_primary_term");
    
    private final StoredFieldVisitor delegate;
    private final DlsFlsActionContext dlsFlsContext;
    private final FlsRule flsRule;
    private final FieldMaskingRule fieldMaskingRule;

    public FlsStoredFieldVisitor(StoredFieldVisitor delegate, DlsFlsActionContext dlsFlsContext) {
        super();
        this.delegate = delegate;
        this.dlsFlsContext = dlsFlsContext;
        this.flsRule = dlsFlsContext.getFlsRule();
        this.fieldMaskingRule = dlsFlsContext.getFieldMaskingRule();
        
        if (log.isDebugEnabled()) {
            log.debug("Created FlsStoredFieldVisitor for " + flsRule + "; " + fieldMaskingRule);
        }
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {

        if (fieldInfo.name.equals("_source")) {
            try {
                if (delegate instanceof MaskedFieldsConsumer) {
                    ((MaskedFieldsConsumer) delegate).binaryMaskedField(fieldInfo,
                            DocumentFilter.filter(Format.JSON, value, flsRule, fieldMaskingRule),
                            (f) -> fieldMaskingRule != null && fieldMaskingRule.get(f) != null);
                } else {
                    delegate.binaryField(fieldInfo, DocumentFilter.filter(Format.JSON, value, flsRule, fieldMaskingRule));
                }

            } catch (DocumentParseException e) {
                throw new ElasticsearchException("Cannot filter source of document", e);
            }
        } else {
            delegate.binaryField(fieldInfo, value);
        }
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return META_FIELDS.contains(fieldInfo.name) || dlsFlsContext.isAllowed(fieldInfo.name) ? delegate.needsField(fieldInfo) : Status.NO;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public void stringField(final FieldInfo fieldInfo, final String value) throws IOException {
        FieldMaskingRule.Field field = this.fieldMaskingRule.get(fieldInfo.name);

        if (field != null) {
            if (delegate instanceof MaskedFieldsConsumer) {
                ((MaskedFieldsConsumer) delegate).stringMaskedField(fieldInfo, field.apply(value));
            } else {
                delegate.stringField(fieldInfo, field.apply(value));
            }

        } else {
            delegate.stringField(fieldInfo, value);
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
    public boolean equals(final Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    static class DocumentFilter {
        public static byte[] filter(Format format, byte[] bytes, FlsRule flsRule, FieldMaskingRule fieldMaskingRule)
                throws DocumentParseException, IOException {
            try (InputStream in = new ByteArrayInputStream(bytes); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                filter(format, in, out, flsRule, fieldMaskingRule);
                return out.toByteArray();
            }
        }

        public static void filter(Format format, InputStream in, OutputStream out, FlsRule flsRule, FieldMaskingRule fieldMaskingRule)
                throws DocumentParseException, IOException {
            try (JsonParser parser = format.getJsonFactory().createParser(in);
                    JsonGenerator generator = format.getJsonFactory().createGenerator(out)) {
                new DocumentFilter(parser, generator, flsRule, fieldMaskingRule).copy();
            }
        }

        private final JsonParser parser;
        private final JsonGenerator generator;
        private final FlsRule flsRule;
        private final FieldMaskingRule fieldMaskingRule;
        private String currentName;
        private String fullCurrentName;
        private String fullParentName;
        private Deque<String> nameStack = new ArrayDeque<>();

        DocumentFilter(JsonParser parser, JsonGenerator generator, FlsRule flsRule, FieldMaskingRule fieldMaskingRule) {
            this.parser = parser;
            this.generator = generator;
            this.flsRule = flsRule;
            this.fieldMaskingRule = fieldMaskingRule;
        }

        @SuppressWarnings("incomplete-switch")
        private void copy() throws IOException {
            boolean skipNext = false;
            
            for (JsonToken token = parser.currentToken() != null ? parser.currentToken() : parser.nextToken(); token != null; token = parser
                    .nextToken()) {

                if (!skipNext) {
                    switch (token) {

                    case START_OBJECT:
                        generator.writeStartObject();
                        if (fullParentName != null) {
                            nameStack.add(fullParentName);
                        }
                        this.fullParentName = this.fullCurrentName;
                        break;

                    case START_ARRAY:
                        generator.writeStartArray();
                        break;

                    case END_OBJECT:
                        generator.writeEndObject();
                        if (nameStack.isEmpty()) {
                            fullParentName = null;
                        } else {
                            fullParentName = nameStack.removeLast();
                        }
                        break;

                    case END_ARRAY:
                        generator.writeEndArray();
                        break;

                    case FIELD_NAME:
                        this.currentName = parser.currentName();
                        this.fullCurrentName = this.fullParentName == null ? this.currentName : this.fullParentName + "." + this.currentName;
                        if (META_FIELDS.contains(fullCurrentName) || flsRule.isAllowed(fullCurrentName)) {
                            generator.writeFieldName(parser.currentName());
                        } else {
                            skipNext = true;
                        }
                        break;

                    case VALUE_TRUE:
                        generator.writeBoolean(Boolean.TRUE);
                        break;

                    case VALUE_FALSE:
                        generator.writeBoolean(Boolean.FALSE);
                        break;

                    case VALUE_NULL:
                        generator.writeNull();
                        break;

                    case VALUE_NUMBER_FLOAT:
                        generator.writeNumber(parser.getDecimalValue());
                        break;

                    case VALUE_NUMBER_INT:
                        generator.writeNumber(parser.getBigIntegerValue());
                        break;

                    case VALUE_STRING:
                        FieldMaskingRule.Field field = fieldMaskingRule.get(this.fullCurrentName);

                        if (field != null) {
                            generator.writeString(field.apply(parser.getText()));
                        } else {
                            generator.writeString(parser.getText());
                        }

                        break;

                    case VALUE_EMBEDDED_OBJECT:
                        generator.writeEmbeddedObject(parser.getEmbeddedObject());
                        break;

                    default:
                        throw new IllegalStateException("Unexpected token: " + token);

                    }

                } else {
                    skipNext = false;
                    switch (token) {

                    case START_OBJECT:
                        parser.skipChildren();
                        break;

                    case START_ARRAY:
                        parser.skipChildren();
                        break;

                    }
                }
            }
        }
    }
}