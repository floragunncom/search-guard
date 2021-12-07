/*
 * Copyright 2015-2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.sgconf.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.floragunn.codova.validation.ValidatingFunction;
import com.floragunn.searchguard.configuration.internal_users.InternalUser;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleMappingsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchsupport.util.ImmutableSet;

public class CType<T> {

    public static final CType<InternalUser> INTERNALUSERS = new CType<InternalUser>("internalusers", 0, InternalUser.class, InternalUser::parse);
    public static final CType<ActionGroupsV7> ACTIONGROUPS = new CType<ActionGroupsV7>("actiongroups", 1, ActionGroupsV7.class, null);
    public static final CType<ConfigV7> CONFIG = new CType<ConfigV7>("config", 2, ConfigV7.class, null);
    public static final CType<RoleV7> ROLES = new CType<RoleV7>("roles", 3, RoleV7.class, null);
    public static final CType<RoleMappingsV7> ROLESMAPPING = new CType<RoleMappingsV7>("rolesmapping", 4, RoleMappingsV7.class, null);
    public static final CType<TenantV7> TENANTS = new CType<TenantV7>("tenants", 5, TenantV7.class, null);
    public static final CType<BlocksV7> BLOCKS = new CType<BlocksV7>("blocks", 6, BlocksV7.class, null);

    private static Map<Class<?>, CType<?>> classToEnumMap = new HashMap<>();
    private static Map<String, CType<?>> nameToInstanceMap = new HashMap<>();
    private static Map<Integer, CType<?>> ordToInstanceMap = new HashMap<>();

    private final String name;
    private final Class<T> type;
    private final int ord;

    private final ValidatingFunction<Map<String, Object>, ?> parser;

    CType(String name, int ord, Class<T> type, ValidatingFunction<Map<String, Object>, ?> parser) {
        this.name = name;
        this.type = type;
        this.parser = parser;
        this.ord = ord;
        classToEnumMap.put(type, this);
        nameToInstanceMap.put(name, this);
        ordToInstanceMap.put(ord, this);
    }

    public String name() {
        return name;
    }
    
    public String getName() {
        return name;
    }

    public Class<T> getType() {
        return type;
    }

    public static CType<?> fromString(String value) {
        return nameToInstanceMap.get(value.toLowerCase());
    }

    public static CType<?> valueOf(String value) {
        return fromString(value);
    }
    
    public String toString() {
        return this.name;
    }

    public String toLCString() {
        return this.name;
    }

    public static Set<String> lcStringValues() {
        return new HashSet<>(nameToInstanceMap.keySet());
    }

    public static Set<CType<?>> fromStringValues(String[] strings) {
        return Arrays.stream(strings).map(CType::fromString).collect(Collectors.toSet());
    }
    
    public static Set<CType<?>> of(CType<?> first, CType<?>...rest) {
        return ImmutableSet.of(first, rest);
    }

    public static Set<CType<?>> all() {
        return new HashSet<>(nameToInstanceMap.values());
    }
    
    public static CType<?> getByClass(Class<?> clazz) {
        CType<?> configType = classToEnumMap.get(clazz);

        if (configType != null) {
            return configType;
        } else {
            throw new IllegalArgumentException("Invalid config class " + clazz);
        }
    }
    
    public static CType<?> getByOrd(int ord) {
        return ordToInstanceMap.get(ord);
    }

    public ValidatingFunction<Map<String, Object>, ?> getParser() {
        return parser;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj instanceof CType) {
            return ((CType<?>) obj).name.equals(this.name);
        } else {
            return false;
        }
    }

    public int getOrd() {
        return ord;
    }

}
