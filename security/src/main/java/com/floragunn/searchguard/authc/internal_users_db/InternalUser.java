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

package com.floragunn.searchguard.authc.internal_users_db;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bouncycastle.crypto.generators.OpenBSDBCrypt;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.RedactableDocument;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.sgconf.Hideable;

public class InternalUser implements PatchableDocument<InternalUser>, RedactableDocument, Hideable {

    private final static SecureRandom RANDOM = new SecureRandom();

    private final Map<String, Object> rawDocument;
    private final String description;
    private final String passwordHash;
    private final boolean reserved;
    private final boolean hidden;

    private final ImmutableSet<String> backendRoles;
    private final ImmutableSet<String> searchGuardRoles;
    private final ImmutableMap<String, Object> attributes;

    private InternalUser(Map<String, Object> rawDocument, String description, String passwordHash, boolean reserved, boolean hidden,
            List<String> backendRoles, List<String> searchGuardRoles, Map<String, Object> attributes) {
        super();
        this.rawDocument = rawDocument;
        this.description = description;
        this.passwordHash = passwordHash;
        this.reserved = reserved;
        this.hidden = hidden;
        this.backendRoles = ImmutableSet.of(backendRoles);
        this.searchGuardRoles = ImmutableSet.of(searchGuardRoles);
        this.attributes = ImmutableMap.of(attributes);
    }

    @Override
    public Object toBasicObject() {
        if (rawDocument != null) {
            return rawDocument;
        } else {
            return ImmutableMap.of("description", description, "hash", passwordHash, "backend_roles", backendRoles, "search_guard_roles",
                    searchGuardRoles, "attributes", attributes);
        }
    }

    @Override
    public ImmutableMap<String, Object> toRedactedBasicObject() {
        if (rawDocument != null) {
            return ImmutableMap.without(rawDocument, "hash");
        } else {
            return ImmutableMap.ofNonNull("description", description, "backend_roles", backendRoles, "search_guard_roles", searchGuardRoles,
                    "attributes", attributes);
        }
    }

    public String getDescription() {
        return description;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public boolean isReserved() {
        return reserved;
    }

    public boolean isHidden() {
        return hidden;
    }

    public ImmutableSet<String> getBackendRoles() {
        return backendRoles;
    }

    public ImmutableSet<String> getSearchGuardRoles() {
        return searchGuardRoles;
    }

    public ImmutableMap<String, Object> getAttributes() {
        return attributes;
    }

    public static ValidationResult<InternalUser> parse(Object parsedJson, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(DocNode.wrap(parsedJson), validationErrors, context);

        String description = vNode.get("description").asString();
        String passwordHash = null;

        if (vNode.hasNonNull("password")) {
            if (vNode.get("password").asString().length() == 0) {
                validationErrors.add(new InvalidAttributeValue("password", "", "A non-empty password"));
            }

            passwordHash = hash(vNode.get("password").asString().trim().toCharArray());
        } else if (vNode.hasNonNull("hash")) {
            passwordHash = vNode.get("hash").asString();
        }

        boolean reserved = vNode.get("reserved").withDefault(false).asBoolean();
        boolean hidden = vNode.get("hidden").withDefault(false).asBoolean();

        List<String> backendRoles = vNode.get("backend_roles").asList().withEmptyListAsDefault().ofStrings();
        List<String> searchGuardRoles = vNode.get("search_guard_roles").asList().withEmptyListAsDefault().ofStrings();
        Map<String, Object> attributes = vNode.get("attributes").asMap();

        attributes = attributes != null ? Collections.unmodifiableMap(new LinkedHashMap<>(attributes)) : Collections.emptyMap();

        Map<String, Object> rawDocument = new LinkedHashMap<>(DocNode.wrap(parsedJson));

        rawDocument.remove("password");

        if (passwordHash != null) {
            rawDocument.put("hash", passwordHash);
        }

        if (validationErrors.hasErrors()) {
            return new ValidationResult<InternalUser>(validationErrors);
        } else {
            return new ValidationResult<InternalUser>(
                    new InternalUser(rawDocument, description, passwordHash, reserved, hidden, backendRoles, searchGuardRoles, attributes));
        }
    }

    public static Document<InternalUser> check(Map<String, Object> parsedJson) throws ConfigValidationException {
        return Document.assertedType(parsedJson, InternalUser.class);
    }

    private static String hash(char[] clearTextPassword) {
        final byte[] salt = new byte[16];
        RANDOM.nextBytes(salt);
        final String hash = OpenBSDBCrypt.generate(clearTextPassword, salt, 12);
        Arrays.fill(salt, (byte) 0);
        Arrays.fill(clearTextPassword, '\0');
        return hash;
    }

    @Override
    public InternalUser parseI(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        return InternalUser.parse(docNode.toMap(), context).get();
    }

}
