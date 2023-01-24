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

import inet.ipaddr.IPAddress;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class IpRangeVerdictBasedBlockRegistry extends VerdictBasedBlockRegistry<IPAddress> {

    public IpRangeVerdictBasedBlockRegistry(Set<IPAddress> allows, Set<IPAddress> disallows) {
        super(IPAddress.class, allows, disallows);
    }

    @Override
    protected Function<IPAddress, Predicate<Collection<IPAddress>>> check() {
        return ip -> nets -> nets.stream().anyMatch(net -> net.contains(ip));
    }
}
