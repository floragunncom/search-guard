package com.floragunn.dlic.auth.ldap2;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.unboundid.ldap.sdk.Filter;

public class ParametrizedFilter {
    
    private final String filter;
    private final Map<Integer, String> placeholders = new HashMap<>();

    public ParametrizedFilter(String filter) {
        this.filter = filter;
    }

    public void setParameter(int placeholder, String value) {
        if(value != null) {
            placeholders.put(placeholder, value);
        }
    }

    @Override
    public String toString() {
        if(filter == null) {
            return null;
        }
        
        String f = filter;
        
        for(Entry<Integer, String> placeholder: placeholders.entrySet()) {
            f = f.replace("{"+placeholder.getKey().intValue()+"}", encode(placeholder.getValue()));
        }
        
        return f;
    }

    private String encode(String value) {
        return Filter.encodeValue(value);
    }
}
