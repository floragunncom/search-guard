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

package com.floragunn.searchguard.configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.blocking.Blocks;
import com.floragunn.searchguard.authc.internal_users_db.InternalUser;
import com.floragunn.searchguard.authc.rest.RestAuthcConfig;
import com.floragunn.searchguard.authc.session.FrontendAuthcConfig;
import com.floragunn.searchguard.authz.config.ActionGroup;
import com.floragunn.searchguard.authz.config.AuthorizationConfig;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.authz.config.RoleMapping;
import com.floragunn.searchguard.authz.config.Tenant;
import com.floragunn.searchguard.configuration.variables.ConfigVar;
import com.floragunn.searchguard.license.SearchGuardLicenseKey;

/**
 * Defines the types of configuration that Search Guard can manage.
 * 
 * IMPORTANT: Do not change the ord attribute. It stems from the time when this type was an enum. 
 * The attribute is used for ES transport message serialization. Changing it might make rolling updates impossible.
 * 
 */
public class CType<T> {

    private static Map<Class<?>, CType<?>> classToEnumMap = new HashMap<>();
    private static Map<String, CType<?>> nameToInstanceMap = new HashMap<>();
    private static Map<Integer, CType<?>> ordToInstanceMap = new HashMap<>();
    private static Set<CType<?>> allSet = new HashSet<>();

    public static final CType<InternalUser> INTERNALUSERS = new CType<InternalUser>("internalusers", "Internal User", 0, InternalUser.class,
            InternalUser::parse).replaceLegacyEnvVars();
    public static final CType<ActionGroup> ACTIONGROUPS = new CType<ActionGroup>("actiongroups", "Action Group", 1, ActionGroup.class,
            ActionGroup::parse).replaceLegacyEnvVars();
    public static final CType<Role> ROLES = new CType<Role>("roles", "Role", 3, Role.class, Role::parse).replaceLegacyEnvVars();
    public static final CType<RoleMapping> ROLESMAPPING = new CType<RoleMapping>("rolesmapping", "Role Mapping", 4, RoleMapping.class,
            RoleMapping::parse).replaceLegacyEnvVars();
    public static final CType<Tenant> TENANTS = new CType<Tenant>("tenants", "Tenant", 5, Tenant.class, Tenant::parse).replaceLegacyEnvVars();
    public static final CType<Blocks> BLOCKS = new CType<Blocks>("blocks", "Block", 6, Blocks.class, Blocks::parse, Storage.OPTIONAL);

    public static final CType<ConfigVar> CONFIG_VARS = new CType<ConfigVar>("config_vars", "Config Variable", 7, ConfigVar.class, null,
            Storage.EXTERNAL);

    public static final CType<FrontendAuthcConfig> FRONTEND_AUTHC = new CType<FrontendAuthcConfig>("frontend_authc", "Frontend Authc", 8,
            FrontendAuthcConfig.class, FrontendAuthcConfig::parse, Storage.OPTIONAL);

    public static final CType<RestAuthcConfig> AUTHC = new CType<RestAuthcConfig>("authc", "Authc", 10, RestAuthcConfig.class, RestAuthcConfig::parse,
            Storage.OPTIONAL, Arity.SINGLE);

    public static final CType<AuthorizationConfig> AUTHZ = new CType<AuthorizationConfig>("authz", "Authorization", 20, AuthorizationConfig.class,
            AuthorizationConfig::parse, Storage.OPTIONAL, Arity.SINGLE);

    public static final CType<SearchGuardLicenseKey> LICENSE_KEY = new CType<SearchGuardLicenseKey>("license_key", "License Key", 30,
            SearchGuardLicenseKey.class, SearchGuardLicenseKey::parse, Storage.OPTIONAL, Arity.SINGLE);

    private final String name;
    private final String uiName;
    private final Class<T> type;
    private final int ord;
    private final Storage storage;
    private final Arity arity;
    private boolean replaceLegacyEnvVars = false;

    private final Parser.ReturningValidationResult<T, ConfigurationRepository.Context> parser;

    public CType(String name, String uiName, int ord, Class<T> type, Parser.ReturningValidationResult<T, ConfigurationRepository.Context> parser) {
        this(name, uiName, ord, type, parser, null, Arity.MULTI);
    }

    public CType(String name, String uiName, int ord, Class<T> type, Parser.ReturningValidationResult<T, ConfigurationRepository.Context> parser,
            Arity arity) {
        this(name, uiName, ord, type, parser, null, arity);
    }

    public CType(String name, String uiName, int ord, Class<T> type, Parser.ReturningValidationResult<T, ConfigurationRepository.Context> parser,
            Storage storage) {
        this(name, uiName, ord, type, parser, storage, Arity.MULTI);
    }

    public CType(String name, String uiName, int ord, Class<T> type, Parser.ReturningValidationResult<T, ConfigurationRepository.Context> parser,
            Storage storage, Arity arity) {
        this.name = name;
        this.uiName = uiName;
        this.type = type;
        this.parser = parser;
        this.ord = ord;
        this.storage = storage;
        this.arity = arity;
        classToEnumMap.put(type, this);
        nameToInstanceMap.put(name, this);
        ordToInstanceMap.put(ord, this);

        if (storage != Storage.EXTERNAL) {
            allSet.add(this);
        }
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
        CType<?> result = fromString(value);

        if (result != null) {
            return result;
        } else {
            throw new IllegalArgumentException("Unknown CType: " + value);
        }
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
        return Arrays.stream(strings).map(CType::fromString).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    public static Set<CType<?>> of(CType<?> first, CType<?>... rest) {
        return ImmutableSet.of(first, rest);
    }

    public static Set<CType<?>> all() {
        return new HashSet<>(allSet);
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

    public Parser.ReturningValidationResult<T, ConfigurationRepository.Context> getParser() {
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

    public boolean isOptional() {
        return storage == Storage.OPTIONAL;
    }

    public boolean isExternal() {
        return storage == Storage.EXTERNAL;
    }

    public boolean isRequired() {
        return !isExternal() && !isOptional();
    }

    public String getUiName() {
        return uiName;
    }

    public static enum Storage {
        OPTIONAL, EXTERNAL
    }

    public static enum Arity {
        SINGLE, MULTI
    }

    public Arity getArity() {
        return arity;
    }

    public T createDefaultInstance(ConfigurationRepository.Context context) {
        try {
            T result = (T) parser.parse(DocNode.EMPTY, context).get();
            return result;
        } catch (ConfigValidationException e) {
            throw new RuntimeException("Could not create default config", e);
        }
    }

    CType<T> replaceLegacyEnvVars() {
        this.replaceLegacyEnvVars = true;
        return this;
    }

    protected boolean isReplaceLegacyEnvVars() {
        return replaceLegacyEnvVars;
    }

}
