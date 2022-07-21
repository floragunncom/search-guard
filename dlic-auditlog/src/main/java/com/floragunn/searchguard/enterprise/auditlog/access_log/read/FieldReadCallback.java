/*
 * Copyright 2018-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.access_log.read;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.Index;
import org.opensearch.index.mapper.Uid;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.support.SourceFieldsContext;
import com.github.wnameless.json.flattener.JsonFlattener;

public final class FieldReadCallback {

    private static final Logger log = LogManager.getLogger(FieldReadCallback.class);
    private final ReadLogContext context;
    private final Index index;
    private Function<Map<String, ?>, Map<String, Object>> filterFunction;
    private SourceFieldsContext sfc;
    private Doc doc;

    public FieldReadCallback(ReadLogContext context) {
        this.context = context;
        this.index = context.getIndex();

        try {
            sfc = (SourceFieldsContext) HeaderHelper.deserializeSafeFromHeader(context.getThreadContext(), "_sg_source_field_context");
            if (sfc != null && sfc.hasIncludesOrExcludes()) {
                if (log.isTraceEnabled()) {
                    log.trace("_sg_source_field_context: " + sfc);
                }

                filterFunction = XContentMapValues.filter(sfc.getIncludes(), sfc.getExcludes());
            }
        } catch (Exception e) {
            log.warn("Cannot deserialize _sg_source_field_context because of {}", e.toString());
        }
    }

    private boolean recordField(final String fieldName, Function<String, Boolean> masked) {
        if (masked.apply(fieldName)) {
            return false;
        }
        return context.getAuditLogConfig().readHistoryEnabledForField(index.getName(), fieldName);
    }

    public void binaryFieldRead(final FieldInfo fieldInfo, byte[] fieldValue, Function<String, Boolean> masked) {
        try {
            if (!recordField(fieldInfo.name, (f) -> false) && !fieldInfo.name.equals("_source") && !fieldInfo.name.equals("_id")) {
                return;
            }

            if (fieldInfo.name.equals("_source")) {

                if (filterFunction != null) {
                    final Map<String, Object> filteredSource = filterFunction.apply(DocReader.json().readObject(fieldValue));
                    fieldValue = DocWriter.json().writeAsBytes(filteredSource);
                }

                Map<String, Object> filteredSource = new JsonFlattener(new String(fieldValue, StandardCharsets.UTF_8)).flattenAsMap();
                for (String k : filteredSource.keySet()) {
                    if (!recordField(k, (f) -> masked.apply(f) && filteredSource.get(k) instanceof String)) {
                        continue;
                    }
                    fieldRead0(k, filteredSource.get(k));
                }
            } else if (fieldInfo.name.equals("_id")) {
                fieldRead0(fieldInfo.name, Uid.decodeId(fieldValue));
            } else {
                fieldRead0(fieldInfo.name, new String(fieldValue, StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.error("Unexpected error reading binary field '{}' in index '{}'", fieldInfo.name, index.getName());
        }
    }

    public void stringFieldRead(final FieldInfo fieldInfo, final String fieldValue, Function<String, Boolean> masked) {
        try {
            if (!recordField(fieldInfo.name, masked)) {
                return;
            }
            fieldRead0(fieldInfo.name, fieldValue);
        } catch (Exception e) {
            log.error("Unexpected error reading string field '{}' in index '{}'", fieldInfo.name, index.getName());
        }
    }

    public void numericFieldRead(final FieldInfo fieldInfo, final Number fieldValue) {
        try {
            if (!recordField(fieldInfo.name, (f) -> false)) {
                return;
            }
            fieldRead0(fieldInfo.name, fieldValue);
        } catch (Exception e) {
            log.error("Unexpected error reading numeric field '{}' in index '{}'", fieldInfo.name, index.getName());
        }
    }

    private void fieldRead0(final String fieldName, final Object fieldValue) {
        if (doc != null) {
            if (fieldName.equals("_id")) {
                doc.setId(fieldValue.toString());
            } else {
                doc.addField(new Field(fieldName, fieldValue));
            }
        } else {
            final String indexName = index.getName();
            if (fieldName.equals("_id")) {
                doc = new Doc(indexName, fieldValue.toString());
            } else {
                doc = new Doc(indexName, null);
                doc.addField(new Field(fieldName, fieldValue));
            }
        }
    }

    public void finished() {
        if (doc == null) {
            return;
        }
        try {
            Map<String, String> f = new HashMap<String, String>();
            for (Field fi : doc.fields) {
                f.put(fi.fieldName, String.valueOf(fi.fieldValue));
            }
            context.getAuditLog().logDocumentRead(doc.indexName, doc.id, null, f);
        } catch (Exception e) {
            log.error("Unexpected error finished compliance read entry {} in index '{}': {}", doc.id, index.getName(), e.toString(), e);
        } finally {
            doc = null;
            sfc = null;
        }
    }

    private class Doc {
        final String indexName;
        String id;
        final List<Field> fields = new ArrayList<Field>();

        public Doc(String indexName, String id) {
            super();
            this.indexName = indexName;
            this.id = id;
        }

        public void addField(Field f) {
            fields.add(f);
        }

        public void setId(String id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "Doc [indexName=" + indexName + ", id=" + id + ", fields=" + fields + "]";
        }
    }

    private class Field {
        final String fieldName;
        final Object fieldValue;

        public Field(String fieldName, Object fieldValue) {
            super();
            this.fieldName = fieldName;
            this.fieldValue = fieldValue;
        }

        @Override
        public String toString() {
            return "Field [fieldName=" + fieldName + ", fieldValue=" + fieldValue + "]";
        }
    }

}
