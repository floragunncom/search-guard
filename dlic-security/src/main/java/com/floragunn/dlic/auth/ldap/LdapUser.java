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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapEntry;

import com.floragunn.dlic.auth.ldap.LdapUser.DirEntry.DirAttribute;
import com.floragunn.dlic.auth.ldap.util.Utils;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.SearchResultEntry;

public class LdapUser extends User {

    private static final long serialVersionUID = 1L;
    private final transient DirEntry userEntry;
    private final String originalUsername;

    public LdapUser(final String name, String originalUsername, final DirEntry userEntry,
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
    public DirEntry getUserEntry() {
        return userEntry;
    }

    public String getDn() {
        return userEntry.getDN();
    }

    public String getOriginalUsername() {
        return originalUsername;
    }
    
    public static Map<String, String> extractLdapAttributes(String originalUsername, final DirEntry userEntry
            , int customAttrMaxValueLen, List<String> whiteListedAttributes) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ldap.original.username", originalUsername);
        attributes.put("ldap.dn", userEntry.getDN());

        if (customAttrMaxValueLen > 0) {
            for (DirAttribute attr : userEntry.getAttributes()) {
                if (attr != null && !attr.isBinary() && !attr.getName().toLowerCase().contains("password")) {
                    final String val = attr.getStringValue();
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
    
    public static final class DirEntry{
        private LdapEntry ldaptiveEntry;
        private SearchResultEntry ubEntry;
        
        public DirEntry(LdapEntry ldaptiveEntry) {
            this.ldaptiveEntry = Objects.requireNonNull(ldaptiveEntry);
        }
        
        public DirEntry(SearchResultEntry ubEntry) {
            this.ubEntry = Objects.requireNonNull(ubEntry);
        }
        
        public String getDN() {
            return ldaptiveEntry != null? ldaptiveEntry.getDn():ubEntry.getDN();
        }
        
        public LdapEntry getLdaptiveEntry() {
            return ldaptiveEntry;
        }

        public SearchResultEntry getUbEntry() {
            return ubEntry;
        }
        
        public Collection<DirAttribute> getAttributes() {
            if(ldaptiveEntry != null) {
                return ldaptiveEntry.getAttributes().stream().map(attr->new DirAttribute(attr)).collect(Collectors.toList());
            } else {
                return ubEntry.getAttributes().stream().map(attr->new DirAttribute(attr)).collect(Collectors.toList());
            }
        }
        
        public static final class DirAttribute {
            
            private LdapAttribute ldaptiveAttribute;
            private Attribute ubAttribute;
            
            public DirAttribute(Attribute ubAttribute) {
                super();
                this.ubAttribute = Objects.requireNonNull(ubAttribute);
            }

            public DirAttribute(LdapAttribute ldaptiveAttribute) {
                super();
                this.ldaptiveAttribute = Objects.requireNonNull(ldaptiveAttribute);
            }

            public boolean isBinary() {
                return ldaptiveAttribute != null? ldaptiveAttribute.isBinary():ubAttribute.needsBase64Encoding();
            }

            public String getName() {
                return ldaptiveAttribute != null? ldaptiveAttribute.getName(false):ubAttribute.getBaseName();
            }

            public int size() {
                return ldaptiveAttribute != null? ldaptiveAttribute.size():ubAttribute.size();
            }

            public String getStringValue() {
                return ldaptiveAttribute != null? Utils.getSingleStringValue(ldaptiveAttribute):Utils.getSingleStringValue(ubAttribute);
            }
        }
    }
}
