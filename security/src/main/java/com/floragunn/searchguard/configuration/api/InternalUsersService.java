/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.configuration.api;

import static com.floragunn.searchguard.sgconf.impl.CType.INTERNALUSERS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.bouncycastle.crypto.generators.OpenBSDBCrypt;

import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.searchguard.configuration.ConfigUpdateException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.InternalUserV7;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

class InternalUsersService {

    private final ConfigurationRepository configurationRepository;

    InternalUsersService(ConfigurationRepository configurationRepository) {
        this.configurationRepository = configurationRepository;
    }

    public InternalUserV7 getUser(String userName) throws InternalUserNotFoundException {
        SgDynamicConfiguration<InternalUserV7> users = getAllUsers();
        if (!users.exists(userName)) {
            throw new InternalUserNotFoundException(userName);
        }

        return users.getCEntry(userName);
    }

    public void addUser(String userName, Map<String, Object> newUserConfig)
            throws ConfigUpdateException, ConfigValidationException, InternalUserAlreadyExistsException {
        SgDynamicConfiguration<InternalUserV7> users = getAllUsers();
        if (users.exists(userName)) {
            throw new InternalUserAlreadyExistsException(userName);
        }

        InternalUserV7 user = new InternalUserV7();

        ValidationErrors validationErrors = new ValidationErrors();
        Object searchGuardRoles = ofNullable(newUserConfig.get("search_guard_roles")).orElse(emptyList());
        Object backendRoles = ofNullable(newUserConfig.get("backend_roles")).orElse(emptyList());
        Object attributes = ofNullable(newUserConfig.get("attributes")).orElse(emptyMap());
        Object password = newUserConfig.get("password");

        setSearchGuardRoles(validationErrors, user, searchGuardRoles);
        setBackendRoles(validationErrors, user, backendRoles);
        setAttributes(validationErrors, user, attributes);

        if (password != null) {
            setPassword(validationErrors, user, password);
        } else {
            validationErrors.add(new InvalidAttributeValue("password", null, "Password"));
        }

        validationErrors.throwExceptionForPresentErrors();

        users.putCEntry(userName, user);
        configurationRepository.update(INTERNALUSERS, users);
    }

    public void updateUser(String userName, Map<String, Object> updates)
            throws ConfigUpdateException, ConfigValidationException, InternalUserNotFoundException {
        SgDynamicConfiguration<InternalUserV7> users = getAllUsers();
        if (!users.exists(userName)) {
            throw new InternalUserNotFoundException(userName);
        }

        ValidationErrors validationErrors = new ValidationErrors();

        InternalUserV7 user = users.getCEntry(userName);
        updates.forEach((key, value) -> {
            switch (key) {
            case "password": {
                setPassword(validationErrors, user, value);
                break;
            }
            case "search_guard_roles": {
                setSearchGuardRoles(validationErrors, user, value);
                break;
            }
            case "backend_roles": {
                setBackendRoles(validationErrors, user, value);
                break;
            }
            case "attributes": {
                setAttributes(validationErrors, user, value);
                break;
            }
            }
        });

        validationErrors.throwExceptionForPresentErrors();

        users.putCEntry(userName, user);
        configurationRepository.update(INTERNALUSERS, users);
    }

    public void deleteUser(String userName) throws ConfigUpdateException, ConfigValidationException, InternalUserNotFoundException {
        SgDynamicConfiguration<InternalUserV7> users = getAllUsers();
        if (!users.exists(userName)) {
            throw new InternalUserNotFoundException(userName);
        }
        users.remove(userName);
        configurationRepository.update(INTERNALUSERS, users);
    }

    private SgDynamicConfiguration<InternalUserV7> getAllUsers() {
        return (SgDynamicConfiguration<InternalUserV7>) configurationRepository.getConfigurationFromIndex(INTERNALUSERS, true);
    }

    private void setPassword(ValidationErrors validationErrors, InternalUserV7 user, Object password) {
        if (password instanceof String) {
            String passwordString = (String) password;
            if (passwordString.isEmpty()) {
                validationErrors.add(new InvalidAttributeValue("password", password, "Password cannot be empty"));
            }
            user.setHash(hash(passwordString.trim().toCharArray()));
        } else {
            validationErrors.add(new InvalidAttributeValue("password", password, "Password"));
        }
    }

    private void setAttributes(ValidationErrors validationErrors, InternalUserV7 user, Object attributesToSet) {
        if (attributesToSet instanceof Map) {
            user.setAttributes(ImmutableMap.copyOf((Map)attributesToSet));
        } else {
            validationErrors.add(new InvalidAttributeValue("attributes", attributesToSet, "Map of attributes"));
        }
    }

    private void setBackendRoles(ValidationErrors validationErrors, InternalUserV7 user, Object backendRolesToSet) {
        if (backendRolesToSet instanceof List) {
            user.setBackend_roles(ImmutableList.copyOf((List<String>)backendRolesToSet));
        } else {
            validationErrors.add(new InvalidAttributeValue("backend_roles", backendRolesToSet, "Collection of backend roles (String)"));
        }
    }

    private void setSearchGuardRoles(ValidationErrors validationErrors, InternalUserV7 user, Object searchGuardRolesToSet) {
        if (searchGuardRolesToSet instanceof List) {
            user.setSearch_guard_roles(ImmutableList.copyOf((List<String>) searchGuardRolesToSet));
        } else {
            validationErrors.add(new InvalidAttributeValue("search_guard_roles", searchGuardRolesToSet, "Collection of roles (String)"));
        }
    }

    public static String hash(char[] clearTextPassword) {
        final byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        final String hash = OpenBSDBCrypt.generate((Objects.requireNonNull(clearTextPassword)), salt, 12);
        Arrays.fill(salt, (byte) 0);
        Arrays.fill(clearTextPassword, '\0');
        return hash;
    }
}
