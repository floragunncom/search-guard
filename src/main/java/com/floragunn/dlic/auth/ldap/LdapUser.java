/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.ldap;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapEntry;

import com.floragunn.dlic.auth.ldap.util.Utils;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class LdapUser extends User {

    private static final long serialVersionUID = 1L;
    private final transient LdapEntry userEntry;
    private final String originalUsername;

    public LdapUser(final String name, String originalUsername, final LdapEntry userEntry,
            final AuthCredentials credentials, int customAttrMaxValueLen, List<String> whiteListedAttributes) {
        super(name, null, credentials);
        this.originalUsername = originalUsername;
        this.userEntry = userEntry;
        Map<String, String> attributes = getCustomAttributesMap();
        attributes.putAll(extractLdapAttributes(originalUsername, userEntry, customAttrMaxValueLen, whiteListedAttributes));
    }

    /**
     * May return null because ldapEntry is transient
     * 
     * @return ldapEntry or null if object was deserialized
     */
    public LdapEntry getUserEntry() {
        return userEntry;
    }

    public String getDn() {
        return userEntry.getDn();
    }

    public String getOriginalUsername() {
        return originalUsername;
    }
    
    public static Map<String, String> extractLdapAttributes(String originalUsername, final LdapEntry userEntry
            , int customAttrMaxValueLen, List<String> whiteListedAttributes) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ldap.original.username", originalUsername);
        attributes.put("ldap.dn", userEntry.getDn());

        if (customAttrMaxValueLen > 0) {
            for (LdapAttribute attr : userEntry.getAttributes()) {
                if (attr != null && !attr.isBinary() && !attr.getName().toLowerCase().contains("password")) {
                    final String val = Utils.getSingleStringValue(attr);
                    // only consider attributes which are not binary and where its value is not
                    // longer than customAttrMaxValueLen characters
                    if (val != null && val.length() > 0 && val.length() <= customAttrMaxValueLen) {
                        if (whiteListedAttributes != null && !whiteListedAttributes.isEmpty()) {
                            if (WildcardMatcher.matchAny(whiteListedAttributes, attr.getName())) {
                                attributes.put("attr.ldap." + attr.getName(), val);
                            }
                        } else {
                            attributes.put("attr.ldap." + attr.getName(), val);
                        }
                    }
                }
            }
        }
        return Collections.unmodifiableMap(attributes);
    }
}
