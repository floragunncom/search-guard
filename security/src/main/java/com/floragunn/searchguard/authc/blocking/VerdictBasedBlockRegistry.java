/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.searchguard.authc.blocking;

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

    @Override
    public String toString() {
        return "[allows=" + allows + ", disallows=" + disallows + "]";
    }

}
