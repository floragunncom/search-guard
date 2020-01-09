package com.floragunn.searchsupport.jobs.config.validation;

@FunctionalInterface
public interface ValidatingFunction<T, R> {
    R apply(T t) throws ConfigValidationException;

    public static <T, R> ValidatingFunction<T, R> from(java.util.function.Function<T, R> f) {
        return (t) -> f.apply(t);
    }
}
