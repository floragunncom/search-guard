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
        // queuedFieldName will contain the unqualified name of a field that was encountered, but not yet written.
        // It is necessary to queue the field names because it can depend on the type of the following value whether
        // the field/value pair will be written: If the value is object-valued, we will also start writing the object
        // if we expect the object to contain allowed values, even if the object itself is not fully allowed.
        private String queuedFieldName;
        // fullCurrentName contains the qualified name of the current field. Changes for every FIELD_NAME token. Does
        // include names of parent objects concatenated by ".". If the current field is named "c" and the parent
        // objects are named "a", "b", this will contain "a.b.c".
        private String fullCurrentName;
        // fullParentName contains the qualified name of the object containing the current field. Will be null if the
        // current field is at the root object of the document.
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
            for (JsonToken token = parser.currentToken() != null ? parser.currentToken() : parser.nextToken(); token != null; token = parser
                    .nextToken()) {

                if (this.queuedFieldName != null) {
                    String fullQueuedFieldName = this.fullParentName == null ? this.queuedFieldName : this.fullParentName + "." + this.queuedFieldName;
                    this.queuedFieldName = null;

                    if (META_FIELDS.contains(fullQueuedFieldName) || isFieldVisible(fullQueuedFieldName, token)) {
                        generator.writeFieldName(parser.currentName());
                        this.fullCurrentName = fullQueuedFieldName;
                    } else {
                        // If the current field name is disallowed by FLS, we will skip the next token.
                        // If the next token is an object or array start, all the child tokens will be also skipped
                        if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                            parser.skipChildren();
                        }
                        continue;
                    }
                }

                switch (token) {
                    case FIELD_NAME:
                        // We do not immediately write field names, because we need to know the type of the value
                        // when checking FLS rules
                        this.queuedFieldName = parser.currentName();
                        break;

                    case START_OBJECT:
                        generator.writeStartObject();
                        if (this.fullParentName != null) {
                            nameStack.add(this.fullParentName);
                        }
                        this.fullParentName = this.fullCurrentName;
                        break;

                    case END_OBJECT:
                        generator.writeEndObject();
                        this.fullCurrentName = this.fullParentName;
                        if (nameStack.isEmpty()) {
                            this.fullParentName = null;
                        } else {
                            this.fullParentName = nameStack.removeLast();
                        }
                        break;

                    case START_ARRAY:
                        generator.writeStartArray();
                        break;

                    case END_ARRAY:
                        generator.writeEndArray();
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

            }
        }

        private boolean isFieldVisible(String fullQueuedFieldName, JsonToken token) {
            boolean startOfObjectOrArray = (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY);

            if (token != JsonToken.VALUE_STRING && fieldMaskingRule.get(fullQueuedFieldName) != null) {
                // If we have a non-hashable attribute value, just make it invisible
                return false;
            }

            return flsRule.isAllowedAssumingParentsAreAllowed(fullQueuedFieldName)
                    || (startOfObjectOrArray && flsRule.isObjectAllowedAssumingParentsAreAllowed(fullQueuedFieldName));
        }
    }
}