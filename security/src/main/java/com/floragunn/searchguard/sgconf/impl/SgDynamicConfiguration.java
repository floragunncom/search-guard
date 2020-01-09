package com.floragunn.searchguard.sgconf.impl;

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.sgconf.Hashed;
import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.sgconf.StaticDefinable;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;

public class SgDynamicConfiguration<T> implements ToXContent {
    
    private static final TypeReference<HashMap<String,Object>> typeRefMSO = new TypeReference<HashMap<String,Object>>() {};

    @JsonIgnore
    private final Map<String, T> centries = new HashMap<>();
    private long seqNo= -1;
    private long primaryTerm= -1;
    private CType ctype;
    private int version = -1;
    
    public static <T> SgDynamicConfiguration<T> empty() {
        return new SgDynamicConfiguration<T>();
    }

    public static <T> SgDynamicConfiguration<T> fromJson(String json, CType ctype, int version, long seqNo, long primaryTerm) throws IOException {
        SgDynamicConfiguration<T> sdc = null;
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
        sdc.version = version;

        return sdc;
    }
    
    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, Object> map, CType ctype) throws IOException {
        int configVersion = getConfigVersion(map, ctype);
        
        return fromMap(map, ctype, configVersion, -1, -1);
    }
    
   
    public static <T> SgDynamicConfiguration<T> fromMap(Map<String, Object> map, CType ctype, int version, long seqNo, long primaryTerm) throws IOException {
        SgDynamicConfiguration<T> sdc = null;
        if(ctype != null) {
            final Class<?> implementationClass = ctype.getImplementationClass().get(version);
            if(implementationClass == null) {
                throw new IllegalArgumentException("No implementation class found for "+ctype+" and config version "+version);
            }
            sdc = DefaultObjectMapper.convertValue(map, DefaultObjectMapper.getTypeFactory().constructParametricType(SgDynamicConfiguration.class, implementationClass));
        
            validate(sdc, version, ctype);
        
        } else {
            sdc = new SgDynamicConfiguration<T>();
        }
        
        sdc.ctype = ctype;
        sdc.seqNo = seqNo;
        sdc.primaryTerm = primaryTerm;
        sdc.version = version;

        return sdc;
    }
    
    public static void validate(SgDynamicConfiguration sdc, int version, CType ctype) throws IOException {
        if(version < 2 && sdc.get_sg_meta() != null) {
            throw new IOException("A version of "+version+" can not have a _sg_meta key for "+ctype);
        }
        
        if(version >= 2 && sdc.get_sg_meta() == null) {
            throw new IOException("A version of "+version+" must have a _sg_meta key for "+ctype);
        }
        
        if(version < 2 && ctype == CType.CONFIG && (sdc.getCEntries().size() != 1 || !sdc.getCEntries().keySet().contains("searchguard"))) {
            throw new IOException("A version of "+version+" must have a single toplevel key named 'searchguard' for "+ctype);
        }
        
        if(version >= 2 && ctype == CType.CONFIG && (sdc.getCEntries().size() != 1 || !sdc.getCEntries().keySet().contains("sg_config"))) {
            throw new IOException("A version of "+version+" must have a single toplevel key named 'sg_config' for "+ctype);
        }
        
    }

    public static <T> SgDynamicConfiguration<T> fromNode(JsonNode json, CType ctype, int version, long seqNo, long primaryTerm) throws IOException {
        return fromJson(DefaultObjectMapper.writeValueAsString(json, false), ctype, version, seqNo, primaryTerm);
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
        return "SgDynamicConfiguration [seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + ", ctype=" + ctype + ", version=" + version + ", centries="
                + centries + ", getImplementingClass()=" + getImplementingClass() + "]";
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
            return fromJson(DefaultObjectMapper.writeValueAsString(this, false), ctype, version, seqNo, primaryTerm);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @JsonIgnore
    public void remove(String key) {
       centries.remove(key);
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean add(SgDynamicConfiguration other) {
        if(other.ctype == null || !other.ctype.equals(this.ctype)) {
            return false;
        }
        
        if(other.getImplementingClass() == null || !other.getImplementingClass().equals(this.getImplementingClass())) {
            return false;
        }
        
        if(other.version != this.version) {
            return false;
        }
        
        this.centries.putAll(other.centries);
        return true;
    }
    
    @JsonIgnore
    @SuppressWarnings({ "rawtypes" })
    public boolean containsAny(SgDynamicConfiguration other) {
        return !Collections.disjoint(this.centries.keySet(), other.centries.keySet());
    }

    
}