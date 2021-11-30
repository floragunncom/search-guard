/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.sgconf.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.RedactableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.codova.validation.jackson.JacksonExceptions;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.sgconf.StaticDefinable;
import com.floragunn.searchguard.support.SgUtils;
import com.google.common.base.Charsets;

public class SgDynamicConfiguration<T> implements ToXContent, Document<Object>, RedactableDocument {
    
    private static final Logger log = LogManager.getLogger(SgDynamicConfiguration.class);

    @JsonIgnore
    private final Map<String, T> centries = new HashMap<>();
    private long seqNo= -1;
    private long primaryTerm= -1;
    private CType<T> ctype;
    private String uninterpolatedJson;
    
    /**
     * This is the version of the config type! If you are looking for the version of the config document, see docVersion!
     */
    private int version = -1;
    private long docVersion = -1;
    
    public static <T> SgDynamicConfiguration<T> empty() {
        return new SgDynamicConfiguration<T>();
    }
    
    public static <T> SgDynamicConfiguration<T> empty(CType<T> type) {
        SgDynamicConfiguration<T> result =  new SgDynamicConfiguration<T>();
        result.ctype = type;
        return result;
    }
        
    public static <T> SgDynamicConfiguration<T> from(Reader reader, CType<T> ctype, Format docType, ConfigurationRepository.Context parserContext) throws IOException, ConfigValidationException {
        if (ctype.getParser() != null) {
            return fromMap(DocReader.format(docType).readObject(reader), ctype, parserContext);
        } else {
            return SgDynamicConfiguration.fromNode(DefaultObjectMapper.YAML_MAPPER.readTree(reader), ctype, 2, 0, 0, 0, parserContext);
        }
    }
    
    public static <T> SgDynamicConfiguration<T> fromJson(String uninterpolatedJson, CType<T> ctype, long docVersion, long seqNo, long primaryTerm, Settings settings, ConfigurationRepository.Context parserContext) throws IOException, ConfigValidationException {
        String jsonString = SgUtils.replaceEnvVars(uninterpolatedJson, settings);
      
        JsonNode jsonNode = DefaultObjectMapper.readTree(jsonString);
        int configVersion = 1;
        
        if (jsonNode.get("_sg_meta") != null) {
            if (!jsonNode.get("_sg_meta").get("type").asText().equals(ctype.toLCString())) {
                throw new RuntimeException("Illegal config: _sg_meta does not match ctype: " + ctype + "; " + jsonNode.get("_sg_meta"));
            }
            configVersion = jsonNode.get("_sg_meta").get("config_version").asInt();
        }

        if (log.isDebugEnabled()) {
            log.debug("Load " + ctype + " with version " + configVersion);
        }
        
        if (ctype.getParser() != null) {
            return fromJsonWithParser(jsonString, uninterpolatedJson, ctype, docVersion, seqNo, primaryTerm, null, parserContext);
        } 

        if (CType.ACTIONGROUPS == ctype) {
            try {
                return SgDynamicConfiguration.fromJson(jsonString, uninterpolatedJson, ctype, configVersion, docVersion, seqNo, primaryTerm, parserContext);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to load " + ctype + " with version " + configVersion + " - Try loading legacy format ...");
                }
                return SgDynamicConfiguration.fromJson(jsonString, uninterpolatedJson, ctype, 0, docVersion, seqNo, primaryTerm, parserContext);
            }
        }

        return SgDynamicConfiguration.fromJson(jsonString, uninterpolatedJson, ctype, configVersion, docVersion, seqNo, primaryTerm, parserContext);
    }

    public static <T> SgDynamicConfiguration<T> fromJson(String json, String uninterpolatedJson, CType<T> ctype, int version, long docVersion, long seqNo, long primaryTerm, ConfigurationRepository.Context parserContext) throws IOException, ConfigValidationException {
        SgDynamicConfiguration<T> sdc;
        if(ctype != null) {
            if (ctype.getParser() != null) {
                return fromJsonWithParser(json, uninterpolatedJson, ctype, docVersion, seqNo, primaryTerm, null, parserContext);
            } 

            final Class<?> implementationClass = ctype.getType();
            if(implementationClass == null) {
                throw new IllegalArgumentException("No implementation class found for "+ctype+" and config version "+version);
            }
                        
            sdc = DefaultObjectMapper.readValue(json, DefaultObjectMapper.getTypeFactory().constructParametricType(SgDynamicConfiguration.class, implementationClass));
        
            validate(sdc, version, ctype);
        
        } else {
            sdc = new SgDynamicConfiguration<T>();
        }
        
        sdc.ctype = ctype;
        sdc.seqNo = seqNo;
        sdc.primaryTerm = primaryTerm;
        sdc.docVersion = docVersion;
        sdc.version = version;
        sdc.uninterpolatedJson = uninterpolatedJson;

        return sdc;
    }
    
    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, ?> map, CType<T> ctype, ConfigurationRepository.Context parserContext) throws ConfigValidationException {
        int configVersion = getConfigVersion(map, ctype);
        
        return fromMap(map, ctype, configVersion, -1, -1, -1, parserContext);
    }
    
   
    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, ?> map, CType<T> ctype, int version, long docVersion, long seqNo, long primaryTerm, ConfigurationRepository.Context parserContext) throws ConfigValidationException {
        SgDynamicConfiguration<T> sdc;               
        if(ctype != null) {
            if (ctype.getParser() != null) {
                return fromMapWithParser(DocNode.wrap(map), null, ctype, docVersion, seqNo, primaryTerm, null, parserContext);
            }
            
            final Class<?> implementationClass = ctype.getType();
            if(implementationClass == null) {
                throw new IllegalArgumentException("No implementation class found for "+ctype+" and config version "+version);
            }
                      
            try {
                sdc = DefaultObjectMapper.convertValue(map, DefaultObjectMapper.getTypeFactory().constructParametricType(SgDynamicConfiguration.class, implementationClass));
            } catch (IllegalArgumentException e) {
                throw JacksonExceptions.toConfigValidationException(e);
            }
            
            validate(sdc, version, ctype);
        
        } else {
            sdc = new SgDynamicConfiguration<T>();
        }
        
        sdc.ctype = ctype;
        sdc.seqNo = seqNo;
        sdc.primaryTerm = primaryTerm;
        sdc.version = version;
        sdc.docVersion = docVersion;

        return sdc;
    }
    
    public static <T> SgDynamicConfiguration<T> fromJsonWithParser(String json, String uninterpolatedJson, CType<T> ctype, long docVersion, long seqNo, long primaryTerm, SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigurationRepository.Context parserContext) throws ConfigValidationException {                
        return fromMapWithParser(DocNode.parse(Format.JSON).from(json), uninterpolatedJson, ctype, docVersion, seqNo, primaryTerm, searchGuardModulesRegistry, parserContext);
    }
    
    public static <T> SgDynamicConfiguration<T> fromMapWithParser(DocNode docNode, String uninterpolatedJson, CType<T> ctype, long docVersion, long seqNo, long primaryTerm, SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigurationRepository.Context parserContext) throws ConfigValidationException {       
        SgDynamicConfiguration<T> result = new SgDynamicConfiguration<>();
        result.ctype = ctype;
        result.seqNo = seqNo;
        result.primaryTerm = primaryTerm;
        result.docVersion = docVersion;
        result.version = 2;
        result.uninterpolatedJson = uninterpolatedJson;

        if (docNode.hasNonNull("_sg_meta")) {
            result._sg_meta = Meta.parse(docNode.getAsNode("_sg_meta"));
        }
        
        Parser<?, ConfigurationRepository.Context> parser = ctype.getParser();

        if (parser == null) {
            throw new IllegalArgumentException("Unsupported ctype " + ctype);
        }

        ValidationErrors validationErrors = new ValidationErrors();

        for (Map.Entry<String, Object> entry : docNode.entrySet()) {

            String id = entry.getKey();

            if (id.startsWith("_sg")) {
                continue;
            }

            if (!(entry.getValue() instanceof Map)) {
                validationErrors.add(new InvalidAttributeValue(id, entry.getValue(), "A JSON object"));
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> record = (Map<String, Object>) entry.getValue();

            try {
                @SuppressWarnings("unchecked")
                T frontendConfig = (T) parser.parse(record, parserContext);

                result.centries.put(id, frontendConfig);
            } catch (ConfigValidationException e) {
                validationErrors.add(id, e);
            } catch (Exception e) {
                log.error("Unexpected exception while parsing " + entry, e);
                validationErrors.add(new ValidationError(id, e.getMessage()).cause(e));
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return result;

    }

    
    public static void validate(SgDynamicConfiguration<?> sdc, int version, CType<?> ctype) throws ConfigValidationException {
        if(version < 2 && sdc.get_sg_meta() != null) {
            throw new ConfigValidationException(
                    new ValidationError("_sg_meta", "A version of " + version + " can not have a _sg_meta key for " + ctype));
        }
        
        if(version >= 2 && sdc.get_sg_meta() == null) {
            throw new ConfigValidationException(
                    new ValidationError("_sg_meta", "A version of " + version + " must have a _sg_meta key for " + ctype));
        }
        
        if(version < 2 && ctype == CType.CONFIG && (sdc.getCEntries().size() != 1 || !sdc.getCEntries().keySet().contains("searchguard"))) {
            throw new ConfigValidationException(
                    new ValidationError(null, "A version of " + version + " must have a single toplevel key named 'searchguard' for " + ctype));
        }
        
        if(version >= 2 && ctype == CType.CONFIG && (sdc.getCEntries().size() != 1 || !sdc.getCEntries().keySet().contains("sg_config"))) {
            throw new ConfigValidationException(
                    new ValidationError(null, "A version of " + version + " must have a single toplevel key named 'sg_config' for " + ctype));
        }
        
    }

    @SuppressWarnings("unchecked")
    public static <T> SgDynamicConfiguration<T> fromNode(JsonNode json, CType<?> ctype, int version, long docVersion, long seqNo, long primaryTerm, ConfigurationRepository.Context parserContext) throws IOException, ConfigValidationException {
        return (SgDynamicConfiguration<T>) fromJson(DefaultObjectMapper.writeValueAsString(json, false), null, ctype, version, docVersion, seqNo, primaryTerm, parserContext);
    }
    

    @SuppressWarnings("unchecked")
    public static <T> SgDynamicConfiguration<T> fromNode(JsonNode json, Class<T> configType, int version, long docVersion, long seqNo,
            long primaryTerm, ConfigurationRepository.Context parserContext) throws IOException, ConfigValidationException {
        return (SgDynamicConfiguration<T>) fromJson(DefaultObjectMapper.writeValueAsString(json, false), null, CType.getByClass(configType), version, docVersion, seqNo, primaryTerm, parserContext);
    }
    
    private static int getConfigVersion(Map<String, ?> map, CType<?> ctype) {
        if (!(map.get("_sg_meta") instanceof Map)) {
            return 1;
        }
        
        Map<?,?> meta = (Map<?,?>) map.get("_sg_meta");
        
        Object version = meta.get("config_version");
        
        if (version instanceof Number) {
            return ((Number) version).intValue();
        } else {
            return 1;
        }
    }
    
    
    //for Jackson
    private SgDynamicConfiguration() {
        super();
    }
    
    private Meta _sg_meta;

    public Meta get_sg_meta() {
        return _sg_meta;
    }

    public void set_sg_meta(Meta _sg_meta) {
        this._sg_meta = _sg_meta;
    }

    @JsonIgnore
    public String getETag() {
        return primaryTerm + "." + seqNo;
    }
        
    @JsonAnySetter
    void setCEntries(String key, T value) {
        putCEntry(key, value);
    }
    
    @JsonAnyGetter
    public Map<String, T> getCEntries() {
        return centries;
    }
    
    @JsonIgnore
    public void removeHidden() {
        uninterpolatedJson = null;

        for(Entry<String, T> entry: new HashMap<String, T>(centries).entrySet()) {
            if(entry.getValue() instanceof Hideable && ((Hideable) entry.getValue()).isHidden()) {
                centries.remove(entry.getKey());
            }
        }
    }
    
    @JsonIgnore
    public void removeStatic() {
        uninterpolatedJson = null;

        for(Entry<String, T> entry: new HashMap<String, T>(centries).entrySet()) {
            if(entry.getValue() instanceof StaticDefinable && ((StaticDefinable) entry.getValue()).isStatic()) {
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
    
    @JsonIgnore
    public T putCEntry(String key, T value) {
        uninterpolatedJson = null;
        return centries.put(key, value);
    }
        
    @JsonIgnore
    public T getCEntry(String key) {
        return centries.get(key);
    }
    
    @JsonIgnore
    public boolean exists(String key) {
        return centries.containsKey(key);
    }

    @JsonIgnore
    public BytesReference toBytesReference() throws IOException {
        return XContentHelper.toXContent(this, XContentType.JSON, false);
    }

    @Override
    public String toString() {
        return "SgDynamicConfiguration [seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + ", ctype=" + ctype + ", version=" + version
                 + ", centries=" + centries + ", getImplementingClass()=" + getImplementingClass() + "]";
    }
    
    @Override
    public Object toBasicObject() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>(centries.size() + 1);

        if (_sg_meta != null) {
            result.put("_sg_meta", _sg_meta.toBasicObject());
        }

        for (Map.Entry<String, T> entry : centries.entrySet()) {
            if (entry.getValue() instanceof Document) {
                result.put(entry.getKey(), ((Document<?>) entry.getValue()).toBasicObject());
            } else {
                try {
                    result.put(entry.getKey(), DocReader.json().read(DefaultObjectMapper.writeValueAsString(this, false)));
                } catch (DocumentParseException | JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
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
            } else if (entry.getValue() instanceof Document) {
                result.put(entry.getKey(), ((Document<?>) entry.getValue()).toBasicObject());
            } else {
                try {
                    result.put(entry.getKey(), DocReader.json().read(DefaultObjectMapper.writeValueAsString(entry.getValue(), false)));
                } catch (DocumentParseException | JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return result;
    }

    @Override
    @JsonIgnore
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        if (uninterpolatedJson != null) {
            builder.rawValue(new ByteArrayInputStream(uninterpolatedJson.getBytes(Charsets.UTF_8)), XContentType.JSON);
        } else  if (Document.class.isAssignableFrom(this.ctype.getType())) {
            return builder.value(toBasicObject());
        } else {
            boolean omitDefaults = params != null && params.paramAsBoolean("omit_defaults", false);
            builder.startObject();
                        
            if (get_sg_meta() != null) {
                builder.field("_sg_meta", get_sg_meta().toBasicObject());
            }
            
            for (Map.Entry<String, T> entry : centries.entrySet()) {
                String key = entry.getKey();
                T value = entry.getValue();
                
                if (value instanceof Document) {
                    builder.field(key, ((Document<?>) value).toBasicObject());
                } else {                    
                    builder.rawField(key,
                            new ByteArrayInputStream(DefaultObjectMapper.writeValueAsString(value, omitDefaults).getBytes(Charsets.UTF_8)),
                            XContentType.JSON);
                }
            }
            
            builder.endObject();
        }
        
        return builder;
    }
    
    public void resetUninterpolatedJson() {
        uninterpolatedJson = null;
    }

    @Override
    @JsonIgnore
    public boolean isFragment() {
        return false;
    }

    @JsonIgnore
    public long getSeqNo() {
        return seqNo;
    }

    @JsonIgnore
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    @JsonIgnore
    public CType<?> getCType() {
        return ctype;
    }
    
    @JsonIgnore
    public int getVersion() {
        return version;
    }
    
    @JsonIgnore
    public Class<?> getImplementingClass() {
        if (ctype == null) {
            return null;
        }

        return ctype.getType();
    }

    @JsonIgnore
    public SgDynamicConfiguration<T> copy() {
        if (Document.class.isAssignableFrom(this.getImplementingClass())) {
            SgDynamicConfiguration<T> result = new SgDynamicConfiguration<>();
            result.ctype = ctype;
            result.seqNo = seqNo;
            result.primaryTerm = primaryTerm;
            result.docVersion = docVersion;
            result.version = version;
            result.uninterpolatedJson = uninterpolatedJson;
            result.centries.putAll(this.centries);
            return result;
        } else {
            try {
                return fromJson(DefaultObjectMapper.writeValueAsString(this, false), uninterpolatedJson, ctype, version, docVersion, seqNo,
                        primaryTerm, null);
            } catch (IOException | ConfigValidationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @JsonIgnore
    public void remove(String key) {
       centries.remove(key);
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void add(SgDynamicConfiguration other) {
        if(other.ctype == null || !other.ctype.equals(this.ctype)) {
            throw new IllegalArgumentException("Config " + other + " has invalid ctype. Expected: " + this.ctype);
        }
        
        if(other.getImplementingClass() == null || !other.getImplementingClass().equals(this.getImplementingClass())) {
            throw new IllegalArgumentException("Config " + other + " has invalid implementingClass. Expected: " + this.getImplementingClass());
        }
        
        if(other.version != this.version) {
            throw new IllegalArgumentException("Config " + other + " has invalid version. Expected: " + this.version);
        }
        
        this.centries.putAll(other.centries);
    }

    @JsonIgnore
    public long getDocVersion() {
        return docVersion;
    }
    
    @JsonIgnore
    public String getUninterpolatedJson() {
        return uninterpolatedJson;
    }



}