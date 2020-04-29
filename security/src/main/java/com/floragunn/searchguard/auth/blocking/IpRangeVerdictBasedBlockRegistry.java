package com.floragunn.searchguard.auth.blocking;

import inet.ipaddr.IPAddressString;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class IpRangeVerdictBasedBlockRegistry extends VerdictBasedBlockRegistry<IPAddressString> {

    public IpRangeVerdictBasedBlockRegistry(Set<IPAddressString> allows, Set<IPAddressString> disallows) {
        super(IPAddressString.class, allows, disallows);
    }

    @Override
    protected Function<IPAddressString, Predicate<Collection<IPAddressString>>> check() {
        return ip -> nets -> nets.stream().anyMatch(net -> net.contains(ip));
    }
}
