/*
 * Copyright 2021-2022 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbers;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.RedactableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.support.SgUtils;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.google.common.base.Charsets;

public class SgDynamicConfiguration<T> implements ToXContent, Document<Object>, RedactableDocument, ComponentStateProvider, Destroyable {

    private static final Logger log = LogManager.getLogger(SgDynamicConfiguration.class);

    private final CType<T> ctype;
    private final ComponentState componentState;

    private final Map<String, T> centries = new LinkedHashMap<>();
    private long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    private String uninterpolatedJson;

    /**
     * This is the version of the config type! If you are looking for the version of the config document, see docVersion!
     */
    private int version = -1;
    private long docVersion = -1;
    private ValidationErrors validationErrors;

    public static <T> SgDynamicConfiguration<T> empty(CType<T> type) {
        return new SgDynamicConfiguration<T>(type);
    }

    public static <T> SgDynamicConfiguration<T> fromJson(String uninterpolatedJson, CType<T> ctype, long docVersion, long seqNo, long primaryTerm,
            Settings settings, ConfigurationRepository.Context parserContext) throws IOException, ConfigValidationException {
        // TODO do replacement only for legacy config
        String jsonString = SgUtils.replaceEnvVars(uninterpolatedJson, settings);

        return fromDocNode(DocNode.wrap(DocReader.json().splitAttributesAtDotsStartingAtDepth(1).read(jsonString)), uninterpolatedJson, ctype,
                docVersion, seqNo, primaryTerm, parserContext);
    }

    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, ?> map, CType<T> ctype, ConfigurationRepository.Context parserContext)
            throws ConfigValidationException {
        return fromMap(map, ctype, -1, -1, -1, parserContext);
    }

    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, ?> map, CType<T> ctype, long docVersion, long seqNo, long primaryTerm,
            ConfigurationRepository.Context parserContext) throws ConfigValidationException {
        return fromDocNode(DocNode.wrap(map), null, ctype, docVersion, seqNo, primaryTerm, parserContext);
    }

    public static <T> SgDynamicConfiguration<T> fromDocNode(DocNode docNode, String uninterpolatedJson, CType<T> ctype, long docVersion, long seqNo,
            long primaryTerm, ConfigurationRepository.Context parserContext) throws ConfigValidationException {
        SgDynamicConfiguration<T> result = new SgDynamicConfiguration<>(ctype);
        result.seqNo = seqNo;
        result.primaryTerm = primaryTerm;
        result.docVersion = docVersion;
        result.version = 2;
        result.uninterpolatedJson = uninterpolatedJson;
        result.componentState.setConfigVersion(docVersion);

        if (docNode.hasNonNull("_sg_meta")) {
            result._sg_meta = Meta.parse(docNode.getAsNode("_sg_meta"));
        }

        Parser.ReturningValidationResult<T, ConfigurationRepository.Context> parser = ctype.getParser();

        if (parser == null) {
            throw new IllegalArgumentException("Unsupported ctype " + ctype);
        }

        ValidationErrors validationErrors = new ValidationErrors();

        for (Map.Entry<String, Object> entry : docNode.entrySet()) {

            String id = entry.getKey();

            if (id.startsWith("_sg")) {
                continue;
            }

            try {
                ValidationResult<T> parsedEntry = parser.parse(DocNode.wrap(entry.getValue()), parserContext);

                if (parsedEntry.hasResult()) {
                    result.centries.put(id, parsedEntry.peek());
                }

                validationErrors.add(ctype.getArity() == CType.Arity.SINGLE ? null : id, parsedEntry.getValidationErrors());
            } catch (Exception e) {
                log.error("Unexpected exception while parsing " + entry, e);
                validationErrors.add(new ValidationError(ctype.getArity() == CType.Arity.SINGLE ? null : id, e.getMessage()).cause(e));
            }
        }

        // TODO clean up overloads and return ValidationResult
        // validationErrors.throwExceptionForPresentErrors();

        if (validationErrors.hasErrors()) {
            log.error("Errors in configuration " + ctype + "\n" + validationErrors.toDebugString());
            result.componentState.setState(State.PARTIALLY_INITIALIZED, "has_errors");
            result.componentState.addDetail(validationErrors.toBasicObject());
        } else {
            result.componentState.initialized();
        }

        result.validationErrors = validationErrors;

        return result;

    }

    public SgDynamicConfiguration(CType<T> ctype) {
        this.ctype = ctype;
        this.componentState = new ComponentState(0, "config", ctype.getName());
    }

    private Meta _sg_meta;

    public Meta get_sg_meta() {
        return _sg_meta;
    }

    public void set_sg_meta(Meta _sg_meta) {
        this._sg_meta = _sg_meta;
    }

    public String getETag() {
        return primaryTerm + "." + seqNo;
    }

    void setCEntries(String key, T value) {
        putCEntry(key, value);
    }

    public Map<String, T> getCEntries() {
        return centries;
    }

    public void removeHidden() {
        uninterpolatedJson = null;

        for (Entry<String, T> entry : new HashMap<String, T>(centries).entrySet()) {
            if (entry.getValue() instanceof Hideable && ((Hideable) entry.getValue()).isHidden()) {
                centries.remove(entry.getKey());
            }
        }
    }

    public SgDynamicConfiguration<T> withoutStatic() {
        SgDynamicConfiguration<T> result = empty(ctype);

        result.version = this.version;
        result.docVersion = this.docVersion;
        result.seqNo = this.seqNo;
        result.primaryTerm = this.primaryTerm;

        for (Entry<String, T> entry : new HashMap<String, T>(centries).entrySet()) {
            if (!(entry.getValue() instanceof StaticDefinable) || !((StaticDefinable) entry.getValue()).isStatic()) {
                result.putCEntry(entry.getKey(), entry.getValue());
            }
        }

        result.uninterpolatedJson = this.uninterpolatedJson;

        return result;
    }

    public void removeStatic() {
        for (Entry<String, T> entry : new HashMap<String, T>(centries).entrySet()) {
            if (entry.getValue() instanceof StaticDefinable && ((StaticDefinable) entry.getValue()).isStatic()) {
                centries.remove(entry.getKey());
            }
        }
    }

    public void removeOthers(String key) {
        uninterpolatedJson = null;

        T tmp = this.centries.get(key);
        this.centries.clear();
        this.centries.put(key, tmp);
    }

    public T putCEntry(String key, T value) {
        uninterpolatedJson = null;
        return centries.put(key, value);
    }

    public void setEntries(Map<String, T> values) {
        uninterpolatedJson = null;
        centries.clear();
        centries.putAll(values);
    }

    public T getCEntry(String key) {
        return centries.get(key);
    }

    public boolean exists(String key) {
        return centries.containsKey(key);
    }

    @Override
    public String toString() {
        return "SgDynamicConfiguration [seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + ", ctype=" + ctype + ", version=" + version + ", centries="
                + centries + ", getImplementingClass()=" + getImplementingClass() + "]";
    }

    @Override
    public Object toBasicObject() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>(centries.size() + 1);

        if (_sg_meta != null) {
            result.put("_sg_meta", _sg_meta.toBasicObject());
        }

        for (Map.Entry<String, T> entry : centries.entrySet()) {
            result.put(entry.getKey(), ((Document<?>) entry.getValue()).toBasicObject());
        }

        return result;
    }

    @Override
    public Object toRedactedBasicObject() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>(centries.size() + 1);

        if (_sg_meta != null) {
            result.put("_sg_meta", _sg_meta.toBasicObject());
        }

        for (Map.Entry<String, T> entry : centries.entrySet()) {
            if (entry.getValue() instanceof RedactableDocument) {
                result.put(entry.getKey(), ((RedactableDocument) entry.getValue()).toRedactedBasicObject());
            } else {
                result.put(entry.getKey(), ((Document<?>) entry.getValue()).toBasicObject());
            }
        }

        return result;
    }

    /**
     * This is for compatibility with the old REST API where clients (such as the Kibana config UI) depend on all attributes being present, even if they are empty.
     */
    public Object toRedactedLegacyBasicObject() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>(centries.size() + 1);

        if (_sg_meta != null) {
            result.put("_sg_meta", _sg_meta.toBasicObject());
        }

        for (Map.Entry<String, T> entry : centries.entrySet()) {
            if (entry.getValue() instanceof HasLegacyFormat) {
                result.put(entry.getKey(), ((HasLegacyFormat) entry.getValue()).toRedactedLegacyBasicObject());
            } else if (entry.getValue() instanceof RedactableDocument) {
                result.put(entry.getKey(), ((RedactableDocument) entry.getValue()).toRedactedBasicObject());
            } else {
                result.put(entry.getKey(), ((Document<?>) entry.getValue()).toDeepBasicObject());
            }
        }

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        if (uninterpolatedJson != null) {
            builder.rawValue(new ByteArrayInputStream(uninterpolatedJson.getBytes(Charsets.UTF_8)), XContentType.JSON);
        } else if (Document.class.isAssignableFrom(this.ctype.getType())) {
            return builder.value(toBasicObject());
        } else {
            builder.startObject();

            if (get_sg_meta() != null) {
                builder.field("_sg_meta", get_sg_meta().toBasicObject());
            }

            for (Map.Entry<String, T> entry : centries.entrySet()) {
                String key = entry.getKey();
                T value = entry.getValue();

                builder.field(key, ((Document<?>) value).toBasicObject());
            }

            builder.endObject();
        }

        return builder;
    }

    public void resetUninterpolatedJson() {
        uninterpolatedJson = null;
    }

    @Override
    public boolean isFragment() {
        return false;
    }
    
    public boolean documentExists() {
        return seqNo >= 0 && primaryTerm >= 0;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public CType<T> getCType() {
        return ctype;
    }

    public int getVersion() {
        return version;
    }

    public Class<T> getImplementingClass() {
        if (ctype == null) {
            return null;
        }

        return ctype.getType();
    }

    public SgDynamicConfiguration<T> copy() {
        SgDynamicConfiguration<T> result = new SgDynamicConfiguration<>(this.ctype);
        result.seqNo = seqNo;
        result.primaryTerm = primaryTerm;
        result.docVersion = docVersion;
        result.version = version;
        result.uninterpolatedJson = uninterpolatedJson;
        result.centries.putAll(this.centries);
        return result;
    }

    public void remove(String key) {
        this.uninterpolatedJson = null;
        centries.remove(key);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void add(SgDynamicConfiguration other) {
        if (other.ctype == null || !other.ctype.equals(this.ctype)) {
            throw new IllegalArgumentException("Config " + other + " has invalid ctype. Expected: " + this.ctype);
        }

        if (other.getImplementingClass() == null || !other.getImplementingClass().equals(this.getImplementingClass())) {
            throw new IllegalArgumentException("Config " + other + " has invalid implementingClass. Expected: " + this.getImplementingClass());
        }

        if (other.version != this.version) {
            throw new IllegalArgumentException("Config " + other + " has invalid version. Expected: " + this.version);
        }

        this.centries.putAll(other.centries);
    }

    public long getDocVersion() {
        return docVersion;
    }

    public String getUninterpolatedJson() {
        return uninterpolatedJson;
    }

    public static interface HasLegacyFormat {
        Object toRedactedLegacyBasicObject();
    }

    public ValidationErrors getValidationErrors() {
        return validationErrors;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    @Override
    public void destroy() {
        if (!Destroyable.class.isAssignableFrom(ctype.getClass())) {
            return;
        }

        for (T entry : this.centries.values()) {
            if (entry instanceof Destroyable) {
                try {
                    ((Destroyable) entry).destroy();
                } catch (Exception e) {
                    log.error("Error while destroying " + entry, e);
                }
            }
        }
    }

}