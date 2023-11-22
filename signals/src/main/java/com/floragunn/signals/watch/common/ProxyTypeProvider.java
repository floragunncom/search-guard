package com.floragunn.signals.watch.common;

import com.floragunn.fluent.collections.ImmutableSet;
import org.elasticsearch.common.Strings;

public class ProxyTypeProvider {

    public static final String DEFAULT_PROXY_KEYWORD = "default";
    public static final String NONE_PROXY_KEYWORD = "none";
    public static final ImmutableSet<String> INLINE_PROXY_PREFIXES = ImmutableSet.of("http:", "https:");

    public static Type determineTypeBasedOnValue(String value) {
        if (Strings.isNullOrEmpty(value) || DEFAULT_PROXY_KEYWORD.equalsIgnoreCase(value)) {
            return Type.USE_DEFAULT_PROXY;
        } else if (NONE_PROXY_KEYWORD.equalsIgnoreCase(value)) {
            return Type.USE_NO_PROXY;
        } else if (INLINE_PROXY_PREFIXES.stream().anyMatch(prefix -> value.toLowerCase().startsWith(prefix))) {
            return Type.USE_INLINE_PROXY;
        } else {
            return Type.USE_STORED_PROXY;
        }
    }

    public enum Type {
        USE_INLINE_PROXY, USE_STORED_PROXY, USE_DEFAULT_PROXY, USE_NO_PROXY
    }

}
