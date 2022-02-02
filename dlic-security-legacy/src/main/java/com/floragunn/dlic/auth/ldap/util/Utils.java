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

package com.floragunn.dlic.auth.ldap.util;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;
import org.ldaptive.Connection;
import org.ldaptive.LdapAttribute;

import com.unboundid.ldap.sdk.Attribute;

public final class Utils {
    
    private static final Logger log = LogManager.getLogger(Utils.class);

    private Utils() {

    }

    public static void unbindAndCloseSilently(final Connection connection) {
        if (connection == null) {
            return;
        }

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    connection.close(); //this never throws an exception
                    //see org.ldaptive.DefaultConnectionFactory.DefaultConnection#close()
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // ignore
        }
    }
    
    public static List<Map.Entry<String, Settings>> getOrderedBaseSettings(Settings settings) {
        return getOrderedBaseSettings(settings.getAsGroups());
    }
    
    public static List<Map.Entry<String, Settings>> getOrderedBaseSettings(Map<String, Settings> settingsMap) {
        return getOrderedBaseSettings(settingsMap.entrySet());
    }

    public static List<Map.Entry<String, Settings>> getOrderedBaseSettings(Set<Map.Entry<String, Settings>> set) {
        List<Map.Entry<String, Settings>> result = new ArrayList<>(set);

        sortBaseSettings(result);

        return Collections.unmodifiableList(result);
    }

    private static void sortBaseSettings(List<Map.Entry<String, Settings>> list) {
        list.sort(new Comparator<Map.Entry<String, Settings>>() {

            @Override
            public int compare(Map.Entry<String, Settings> o1, Map.Entry<String, Settings> o2) {
                int attributeOrder = Integer.compare(o1.getValue().getAsInt("order", Integer.MAX_VALUE),
                        o2.getValue().getAsInt("order", Integer.MAX_VALUE));

                if (attributeOrder != 0) {
                    return attributeOrder;
                }

                return o1.getKey().compareTo(o2.getKey());
            }
        });
    }

    public static String getSingleStringValue(LdapAttribute attribute) {
        if(attribute == null) {
            return null;
        }
        
        if(attribute.size() > 1) {
            if(log.isDebugEnabled()) {
                log.debug("Multiple values found for {} ({})", attribute.getName(false), attribute);
            }
        }
        
        return attribute.getStringValue();
    }
    
    public static String getSingleStringValue(Attribute attribute) {
        if(attribute == null) {
            return null;
        }
        
        if(attribute.size() > 1) {
            if(log.isDebugEnabled()) {
                log.debug("Multiple values found for {} ({})", attribute.getBaseName(), attribute);
            }
        }
        
        return attribute.getValue();
    }
}
