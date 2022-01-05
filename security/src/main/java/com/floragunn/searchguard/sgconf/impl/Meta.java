package com.floragunn.searchguard.sgconf.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;

public class Meta implements Document<Meta> {

    private String type;
    private int config_version;

    private CType<?> cType;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
        cType = CType.fromString(type);
    }

    public int getConfig_version() {
        return config_version;
    }

    public void setConfig_version(int config_version) {
        this.config_version = config_version;
    }

    @JsonIgnore
    public CType<?> getCType() {
        return cType;
    }

    @Override
    public String toString() {
        return "Meta [type=" + type + ", config_version=" + config_version + ", cType=" + cType + "]";
    }

    @Override
    public Map<String, Object> toBasicObject() {
        Map<String, Object> result = new LinkedHashMap<>(2);
        result.put("type", type);
        result.put("config_version", config_version);
        return result;
    }
    
    public static Meta parse(DocNode docNode) {
        Meta result = new Meta();
        result.type = docNode.getAsString("type");
        result.config_version = docNode.get("config_version") instanceof Number ? ((Number) docNode.get("config_version")).intValue() : -1;
        return result;
    }

}
