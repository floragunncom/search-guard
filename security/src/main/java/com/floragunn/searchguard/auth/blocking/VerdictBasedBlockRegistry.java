package com.floragunn.searchguard.auth.blocking;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class VerdictBasedBlockRegistry<ClientIdType> implements ClientBlockRegistry<ClientIdType> {

    private final Class<ClientIdType> registryType;
    private final Set<ClientIdType> allows;
    private final Set<ClientIdType> disallows;

    public VerdictBasedBlockRegistry(Class<ClientIdType> registryType, Set<ClientIdType> allows, Set<ClientIdType> disallows) {
        this.registryType = registryType;
        this.allows = allows;
        this.disallows = disallows;
    }

    @Override
    public boolean isBlocked(ClientIdType clientId) {
        Predicate<Collection<ClientIdType>> p = check().apply(clientId);

        if (allows.isEmpty()) {
            return p.test(disallows);
        }

        boolean isAllowed = p.test(allows);
        boolean disAllowed = p.test(disallows);
        return !isAllowed || disAllowed;
    }

    @Override
    public void block(ClientIdType clientId) {
        throw new UnsupportedOperationException("This class doesn't allow to add new block entries after object construction.");
    }

    @Override
    public Class<ClientIdType> getClientIdType() {
        return registryType;
    }

    protected Function<ClientIdType, Predicate<Collection<ClientIdType>>> check() {
        return a -> as -> as.contains(a);
    }

}
