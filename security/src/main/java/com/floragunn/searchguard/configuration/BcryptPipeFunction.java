/*
 * Copyright 2024 floragunn GmbH
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

import com.floragunn.codova.config.templates.PipeExpression;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;

import java.security.SecureRandom;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class BcryptPipeFunction implements PipeExpression.PipeFunction {

    public static final String NAME = "bcrypt";

    private final int cost;
    private final Supplier<byte[]> saltSupplier;

    BcryptPipeFunction(int cost, Supplier<byte[]> saltSupplier) {
        this.cost = cost;
        this.saltSupplier = Objects.requireNonNull(saltSupplier, "Salt supplier is required");
    }

    public BcryptPipeFunction() {
        this(12, () -> {
            final byte[] salt = new byte[16];
            new SecureRandom().nextBytes(salt);
            return salt;
        });
    }

    @Override
    public Object apply(Object value) {
        if (value instanceof String) {
            String stringValue = (String) value;
            return bcrypt(stringValue);
        } else if (value instanceof Collection) {
            return ((Collection<?>) value).stream().map((v) -> apply(v)).collect(Collectors.toList());
        } else if (value == null) {
            return null;
        } else {
            return apply(value.toString());
        }
    }

    private String bcrypt(String string) {
        final byte[] salt = saltSupplier.get();
        return OpenBSDBCrypt.generate((string.toCharArray()), salt, cost);
    }
}
