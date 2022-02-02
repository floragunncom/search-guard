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

package com.floragunn.searchguard.enterprise.auth.ldap;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.unboundid.ldap.sdk.SearchScope;

public class LDAP {
    static SearchScope getSearchScope(String name) throws ConfigValidationException {
        for (SearchScope searchScope : SearchScope.values()) {
            if (searchScope.getName().equalsIgnoreCase(name)) {
                return searchScope;
            }
        }

        throw new ConfigValidationException(new InvalidAttributeValue(null, name,
                Arrays.asList(SearchScope.values()).stream().map((s) -> s.getName()).collect(Collectors.joining("|"))));
    }
}