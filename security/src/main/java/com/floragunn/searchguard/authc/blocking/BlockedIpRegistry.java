/*
 * Copyright 2015-2022 floragunn GmbH
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;

import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.google.common.collect.ImmutableList;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;

public class BlockedIpRegistry {
    protected static final Logger log = LogManager.getLogger(BlockedIpRegistry.class);

    private volatile List<ClientBlockRegistry<IPAddress>> blockedNetmasks;
    private volatile List<ClientBlockRegistry<InetAddress>> ipClientBlockRegistries;

    public BlockedIpRegistry(ConfigurationRepository configurationRepository) {
        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<BlocksV7> blocks = configMap.get(CType.BLOCKS);

                if (blocks != null) {
                    blockedNetmasks = ImmutableList.of(reloadBlockedNetmasks(blocks));
                    ipClientBlockRegistries = ImmutableList.of(reloadBlockedIpAddresses(blocks));
                    
                    if (log.isDebugEnabled()) {
                        log.debug("Updated confiuration: " + blocks + "\nBlockedNetmasks: " + blockedNetmasks + "; ips: " + ipClientBlockRegistries);
                    }
                } 
            }
        });
    }

    public boolean isIpBlocked(IPAddress address) {
        if (address == null) {
            return false;
        }

        if ((this.ipClientBlockRegistries == null || this.ipClientBlockRegistries.isEmpty())
                && (this.blockedNetmasks == null || this.blockedNetmasks.isEmpty())) {
            return false;
        }

        InetAddress inetAddress = address.toInetAddress();

        if (ipClientBlockRegistries != null) {
            for (ClientBlockRegistry<InetAddress> clientBlockRegistry : ipClientBlockRegistries) {
                if (clientBlockRegistry.isBlocked(inetAddress)) {
                    return true;
                }
            }
        }

        if (blockedNetmasks != null) {
            for (ClientBlockRegistry<IPAddress> registry : blockedNetmasks) {
                if (registry.isBlocked(address)) {
                    return true;
                }
            }
        }

        return false;
    }
    
    
    private ClientBlockRegistry<IPAddress> reloadBlockedNetmasks(SgDynamicConfiguration<BlocksV7> blocks) {
        Function<String, Optional<IPAddress>> parsedIp = s -> {
            IPAddressString ipAddressString = new IPAddressString(s);
            try {
                ipAddressString.validate();
                return Optional.of(ipAddressString.toAddress());
            } catch (AddressStringException e) {
                log.error("Reloading blocked IP addresses failed ", e);
                return Optional.empty();
            }
        };

        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.net_mask);
        Set<IPAddress> allows = b.v1().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());
        Set<IPAddress> disallows = b.v2().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());

        return new IpRangeVerdictBasedBlockRegistry(allows, disallows);
    }

    private ClientBlockRegistry<InetAddress> reloadBlockedIpAddresses(SgDynamicConfiguration<BlocksV7> blocks) {
        Function<String, Optional<InetAddress>> parsedIp = s -> {
            try {
                return Optional.of(InetAddress.getByName(s));
            } catch (UnknownHostException e) {
                log.error("Reloading blocked IP addresses failed", e);
                return Optional.empty();
            }
        };

        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.ip);
        Set<InetAddress> allows = b.v1().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty)).collect(Collectors.toSet());
        Set<InetAddress> disallows = b.v2().stream().map(parsedIp).flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toSet());

        return new VerdictBasedBlockRegistry<>(InetAddress.class, allows, disallows);
    }

    private Tuple<Set<String>, Set<String>> readBlocks(SgDynamicConfiguration<BlocksV7> blocks, BlocksV7.Type type) {
        Set<String> allows = new HashSet<>();
        Set<String> disallows = new HashSet<>();

        List<BlocksV7> blocksV7s = blocks.getCEntries().values().stream().filter(b -> b.getType() == type).collect(Collectors.toList());

        for (BlocksV7 blocksV7 : blocksV7s) {
            if (blocksV7.getVerdict() == null) {
                log.error("No verdict type found in blocks");
                continue;
            }
            if (blocksV7.getVerdict() == BlocksV7.Verdict.disallow) {
                disallows.addAll(blocksV7.getValue());
            } else if (blocksV7.getVerdict() == BlocksV7.Verdict.allow) {
                allows.addAll(blocksV7.getValue());
            } else {
                log.error("Found unknown verdict type: " + blocksV7.getVerdict());
            }
        }
        return new Tuple<>(allows, disallows);
    }


}
