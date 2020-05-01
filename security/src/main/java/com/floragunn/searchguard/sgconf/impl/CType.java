package com.floragunn.searchguard.sgconf.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.floragunn.searchguard.sgconf.impl.v6.ActionGroupsV6;
import com.floragunn.searchguard.sgconf.impl.v6.ConfigV6;
import com.floragunn.searchguard.sgconf.impl.v6.InternalUserV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleMappingsV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleV6;
import com.floragunn.searchguard.sgconf.impl.v7.*;

public enum CType {

    INTERNALUSERS(toMap(1, InternalUserV6.class, 2,
            InternalUserV7.class)),
    ACTIONGROUPS(toMap(0, List.class, 1, ActionGroupsV6.class, 2,
            ActionGroupsV7.class)),
    CONFIG(toMap(1, ConfigV6.class, 2, ConfigV7.class)),
    ROLES(toMap(1, RoleV6.class, 2, RoleV7.class)), 
    ROLESMAPPING(toMap(1, RoleMappingsV6.class, 2, RoleMappingsV7.class)),
    TENANTS(toMap(2, TenantV7.class)),
    BLOCKS(toMap(2, BlocksV7.class));

    private Map<Integer, Class<?>> implementations;

    CType(Map<Integer, Class<?>> implementations) {
        this.implementations = implementations;
    }

    public Map<Integer, Class<?>> getImplementationClass() {
        return Collections.unmodifiableMap(implementations);
    }

    public static CType fromString(String value) {
        return CType.valueOf(value.toUpperCase());
    }

    public String toLCString() {
        return this.toString().toLowerCase();
    }

    public static Set<String> lcStringValues() {
        return Arrays.stream(CType.values()).map(CType::toLCString).collect(Collectors.toSet());
    }

    public static Set<CType> fromStringValues(String[] strings) {
        return Arrays.stream(strings).map(CType::fromString).collect(Collectors.toSet());
    }

    private static Map<Integer, Class<?>> toMap(Object... objects) {
        final Map<Integer, Class<?>> map = new HashMap<>();
        for (int i = 0; i < objects.length; i = i + 2) {
            map.put((Integer) objects[i], (Class<?>) objects[i + 1]);
        }
        return Collections.unmodifiableMap(map);
    }
}
