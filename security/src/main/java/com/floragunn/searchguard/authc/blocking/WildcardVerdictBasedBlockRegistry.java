package com.floragunn.searchguard.authc.blocking;

import com.floragunn.searchguard.support.WildcardMatcher;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class WildcardVerdictBasedBlockRegistry extends VerdictBasedBlockRegistry<String> {

    public WildcardVerdictBasedBlockRegistry(Set<String> allows, Set<String> disallows) {
        super(String.class, allows, disallows);
    }

    @Override
    protected Function<String, Predicate<Collection<String>>> check() {
        return a -> as -> WildcardMatcher.matchAny(as, a);
    }
}
