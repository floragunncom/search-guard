package com.floragunn.searchguard.authc.blocking;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import inet.ipaddr.IPAddress;

public class IpRangeVerdictBasedBlockRegistry extends VerdictBasedBlockRegistry<IPAddress> {

    public IpRangeVerdictBasedBlockRegistry(Set<IPAddress> allows, Set<IPAddress> disallows) {
        super(IPAddress.class, allows, disallows);
    }

    @Override
    protected Function<IPAddress, Predicate<Collection<IPAddress>>> check() {
        return ip -> nets -> nets.stream().anyMatch(net -> net.contains(ip));
    }
}
