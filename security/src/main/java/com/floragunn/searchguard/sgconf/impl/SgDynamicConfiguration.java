package com.floragunn.searchguard.sgconf.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ConfigVariableProviders;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.codova.validation.jackson.JacksonExceptions;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.modules.SearchGuardModulesRegistry;
import com.floragunn.searchguard.sgconf.Hashed;
import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.sgconf.StaticDefinable;
import com.floragunn.searchguard.sgconf.impl.v7.FrontendConfig;
import com.floragunn.searchguard.support.SgUtils;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;

public class SgDynamicConfiguration<T> implements ToXContent {
    
    private static final Logger log = LogManager.getLogger(SgDynamicConfiguration.class);
    private static final TypeReference<HashMap<String,Object>> typeRefMSO = new TypeReference<HashMap<String,Object>>() {};

    @JsonIgnore
    private final Map<String, T> centries = new HashMap<>();
    private long seqNo= -1;
    private long primaryTerm= -1;
    private CType ctype;
    private String uninterpolatedJson;
    
    /**
     * This is the version of the config type! If you are looking for the version of the config document, see docVersion!
     */
    private int version = -1;
    private long docVersion = -1;
    
    public static <T> SgDynamicConfiguration<T> empty() {
        return new SgDynamicConfiguration<T>();
    }
    
    public static <T> SgDynamicConfiguration<T> fromJson(String uninterpolatedJson, CType ctype, long docVersion, long seqNo, long primaryTerm, Settings settings, SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigVariableProviders configVariableProviders) throws IOException, ConfigValidationException {
        String jsonString = SgUtils.replaceEnvVars(uninterpolatedJson, settings);
        
        if (ctype == CType.FRONTEND_CONFIG) {
            return fromJsonNew(jsonString, uninterpolatedJson, ctype, docVersion, seqNo, primaryTerm, searchGuardModulesRegistry, configVariableProviders);
        }
     
        JsonNode jsonNode = DefaultObjectMapper.readTree(jsonString);
        int configVersion = 1;

        if (jsonNode.get("_sg_meta") != null) {
            assert jsonNode.get("_sg_meta").get("type").asText().equals(ctype.toLCString());
            configVersion = jsonNode.get("_sg_meta").get("config_version").asInt();
        }

        if (log.isDebugEnabled()) {
            log.debug("Load " + ctype + " with version " + configVersion);
        }

        if (CType.ACTIONGROUPS == ctype) {
            try {
                return SgDynamicConfiguration.fromJson(jsonString, uninterpolatedJson, ctype, configVersion, docVersion, seqNo, primaryTerm);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to load " + ctype + " with version " + configVersion + " - Try loading legacy format ...");
                }
                return SgDynamicConfiguration.fromJson(jsonString, uninterpolatedJson, ctype, 0, docVersion, seqNo, primaryTerm);
            }
        }

        return SgDynamicConfiguration.fromJson(jsonString, uninterpolatedJson, ctype, configVersion, docVersion, seqNo, primaryTerm);
    }

    public static <T> SgDynamicConfiguration<T> fromJson(String json, String uninterpolatedJson, CType ctype, int version, long docVersion, long seqNo, long primaryTerm) throws IOException, ConfigValidationException {
        SgDynamicConfiguration<T> sdc;
        if(ctype != null) {
            final Class<?> implementationClass = ctype.getImplementationClass().get(version);
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
    
    public static <T> SgDynamicConfiguration<T> fromJsonNew(String json, String uninterpolatedJson, CType ctype, long docVersion, long seqNo, long primaryTerm, SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigVariableProviders configVariableProviders) throws ConfigValidationException {
        Map<String, Object> parsedJson = ValidatingJsonParser.readObjectAsMap(json);
        
        return fromMapNew(parsedJson, uninterpolatedJson, ctype, docVersion, seqNo, primaryTerm, searchGuardModulesRegistry, configVariableProviders);
    }
    
    public static <T> SgDynamicConfiguration<T> fromMapNew(Map<String, Object> parsedJson, String uninterpolatedJson, CType ctype, long docVersion, long seqNo, long primaryTerm, SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigVariableProviders configVariableProviders) throws ConfigValidationException {
        SgDynamicConfiguration<T> result = new SgDynamicConfiguration<>();
        result.ctype = ctype;
        result.seqNo = seqNo;
        result.primaryTerm = primaryTerm;
        result.docVersion = docVersion;
        result.version = 2;
        result.uninterpolatedJson = uninterpolatedJson;
        
        if (ctype == CType.FRONTEND_CONFIG) {
            
            ValidationErrors validationErrors = new ValidationErrors();
            
            for (Map.Entry<String, Object> entry : parsedJson.entrySet()) {
                
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
                    T frontendConfig = (T) FrontendConfig.parse(record,
                            searchGuardModulesRegistry != null ? searchGuardModulesRegistry.getApiAuthenticationFrontends() : null,
                            configVariableProviders);

                    result.centries.put(id, frontendConfig);
                } catch (ConfigValidationException e) {
                    validationErrors.add(id, e);
                }
            }
            
            validationErrors.throwExceptionForPresentErrors();
            
            return result;
            
        } else {
            throw new IllegalArgumentException("Unsupported ctype " + ctype);
        }
    }
    
    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, Object> map, CType ctype, SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigVariableProviders configVariableProviders) throws ConfigValidationException {
        int configVersion = getConfigVersion(map, ctype);
        
        return fromMap(map, ctype, configVersion, -1, -1, -1, searchGuardModulesRegistry, configVariableProviders);
    }
    
   
    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, Object> map, CType ctype, int version, long docVersion, long seqNo, long primaryTerm, SearchGuardModulesRegistry searchGuardModulesRegistry, ConfigVariableProviders configVariableProviders) throws ConfigValidationException {
        SgDynamicConfiguration<T> sdc;
        if(ctype != null) {
            final Class<?> implementationClass = ctype.getImplementationClass().get(version);
            if(implementationClass == null) {
                throw new IllegalArgumentException("No implementation class found for "+ctype+" and config version "+version);
            }
            
            if (ctype == CType.FRONTEND_CONFIG) {
                return fromMapNew(map, null, ctype, docVersion, seqNo, primaryTerm, searchGuardModulesRegistry, configVariableProviders);
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
    
    public static void validate(SgDynamicConfiguration<?> sdc, int version, CType ctype) throws ConfigValidationException {
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

    public static <T> SgDynamicConfiguration<T> fromNode(JsonNode json, CType ctype, int version, long docVersion, long seqNo, long primaryTerm) throws IOException, ConfigValidationException {
        if (ctype == CType.FRONTEND_CONFIG) {
            return fromJsonNew(DefaultObjectMapper.writeValueAsString(json, false), null, ctype, docVersion, seqNo, primaryTerm, null, null);
        } else {        
            return fromJson(DefaultObjectMapper.writeValueAsString(json, false), null, ctype, version, docVersion, seqNo, primaryTerm);
        }
    }
    

    public static <T> SgDynamicConfiguration<T> fromNode(JsonNode json, Class<T> configType, int version, long docVersion, long seqNo,
            long primaryTerm) throws IOException, ConfigValidationException {
        return fromJson(DefaultObjectMapper.writeValueAsString(json, false), null, CType.getByClass(configType), version, docVersion, seqNo, primaryTerm);
    }
    
    private static int getConfigVersion(Map<String, Object> map, CType ctype) {
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
        for(Entry<String, T> entry: new HashMap<String, T>(centries).entrySet()) {
            if(entry.getValue() instanceof Hideable && ((Hideable) entry.getValue()).isHidden()) {
                centries.remove(entry.getKey());
            }
        }
    }
    
    @JsonIgnore
    public void removeStatic() {
        for(Entry<String, T> entry: new HashMap<String, T>(centries).entrySet()) {
            if(entry.getValue() instanceof StaticDefinable && ((StaticDefinable) entry.getValue()).isStatic()) {
                centries.remove(entry.getKey());
            }
        }
    }
    
    @JsonIgnore
    public void clearHashes() {
        for(Entry<String, T> entry: centries.entrySet()) {
            if(entry.getValue() instanceof Hashed) {
               ((Hashed) entry.getValue()).clearHash(); 
            }
        }
    }
    

    public void removeOthers(String key) {
        T tmp = this.centries.get(key);
        this.centries.clear();
        this.centries.put(key, tmp);
    }
    
    @JsonIgnore
    public T putCEntry(String key, T value) {
        return centries.put(key, value);
    }
    
    @JsonIgnore
    public void putCObject(String key, Object value) {
        centries.put(key, (T) value);
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
    @JsonIgnore
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final boolean omitDefaults = params != null && params.paramAsBoolean("omit_defaults", false);
        return builder.map(DefaultObjectMapper.readValue(DefaultObjectMapper.writeValueAsString(this, omitDefaults), typeRefMSO));
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
    public CType getCType() {
        return ctype;
    }
    
    @JsonIgnore
    public void setCType(CType ctype) {
        this.ctype = ctype;
    }

    @JsonIgnore
    public int getVersion() {
        return version;
    }
    
    @JsonIgnore
    public Class<?> getImplementingClass() {
        return ctype==null?null:ctype.getImplementationClass().get(getVersion());
    }

    @JsonIgnore
    public SgDynamicConfiguration<T> deepClone() {
        try {
            if (ctype == CType.FRONTEND_CONFIG) {
                // TODO
                return this;
            }
            
            return fromJson(DefaultObjectMapper.writeValueAsString(this, false), uninterpolatedJson, ctype, version, docVersion, seqNo, primaryTerm);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
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