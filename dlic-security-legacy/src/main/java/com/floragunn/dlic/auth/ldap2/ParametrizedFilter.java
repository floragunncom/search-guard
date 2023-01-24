/*
  * Copyright 2023 by floragunn GmbH - All rights reserved
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
package com.floragunn.dlic.auth.ldap2;

import com.unboundid.ldap.sdk.Filter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ParametrizedFilter {

    private final String filter;
    private final Map<Integer, String> placeholders = new HashMap<>();

    public ParametrizedFilter(String filter) {
        this.filter = filter;
    }

    public void setParameter(int placeholder, String value) {
        if (value != null) {
            placeholders.put(placeholder, value);
        }
    }

    @Override
    public String toString() {
        if (filter == null) {
            return null;
        }

        String f = filter;

        for (Entry<Integer, String> placeholder : placeholders.entrySet()) {
            f = f.replace("{" + placeholder.getKey().intValue() + "}", encode(placeholder.getValue()));
        }

        return f;
    }

    private String encode(String value) {
        return Filter.encodeValue(value);
    }
}
