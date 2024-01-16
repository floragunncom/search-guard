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
