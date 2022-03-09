/*
 * Copyright 2015-2018 floragunn GmbH
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

package com.floragunn.searchguard.sgconf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;

import com.floragunn.searchguard.authc.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.authc.blocking.IpRangeVerdictBasedBlockRegistry;
import com.floragunn.searchguard.authc.blocking.VerdictBasedBlockRegistry;
import com.floragunn.searchguard.authc.blocking.WildcardVerdictBasedBlockRegistry;
import com.floragunn.searchguard.authz.RoleMapping;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;

public class ConfigModelV7 extends ConfigModel {

    private static final Logger log = LogManager.getLogger(ConfigModelV7.class);

    private ConfigConstants.RolesMappingResolution rolesMappingResolution;
    private ActionGroups actionGroups;
    private SgRoles sgRoles;
    private RoleMapping.InvertedIndex invertedRoleMappings;
    private SgDynamicConfiguration<TenantV7> tenants;
    private ClientBlockRegistry<InetAddress> blockedIpAddresses;
    private ClientBlockRegistry<String> blockedUsers;
    private ClientBlockRegistry<IPAddress> blockeNetmasks;

    public ConfigModelV7(SgDynamicConfiguration<BlocksV7> blocks, Settings esSettings) {

        try {
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.valueOf(esSettings
                    .get(ConfigConstants.SEARCHGUARD_ROLES_MAPPING_RESOLUTION, ConfigConstants.RolesMappingResolution.MAPPING_ONLY.toString())
                    .toUpperCase());
        } catch (Exception e) {
            log.error("Cannot apply roles mapping resolution", e);
            rolesMappingResolution = ConfigConstants.RolesMappingResolution.MAPPING_ONLY;
        }

        blockedIpAddresses = reloadBlockedIpAddresses(blocks);
        blockedUsers = reloadBlockedUsers(blocks);
        blockeNetmasks = reloadBlockedNetmasks(blocks);
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

    private ClientBlockRegistry<String> reloadBlockedUsers(SgDynamicConfiguration<BlocksV7> blocks) {
        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.name);
        return new WildcardVerdictBasedBlockRegistry(b.v1(), b.v2());
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

    public Set<String> getAllConfiguredTenantNames() {
        return Collections.unmodifiableSet(tenants.getCEntries().keySet());
    }

    public SgRoles getSgRoles() {
        return sgRoles;
    }

    public ActionGroups getActionGroups() {
        return actionGroups;
    }

    @Override
    public List<ClientBlockRegistry<InetAddress>> getBlockIpAddresses() {
        return Collections.singletonList(blockedIpAddresses);
    }

    @Override
    public List<ClientBlockRegistry<String>> getBlockedUsers() {
        return Collections.singletonList(blockedUsers);
    }

    @Override
    public List<ClientBlockRegistry<IPAddress>> getBlockedNetmasks() {
        return Collections.singletonList(blockeNetmasks);
    }

    @Override
    public boolean isTenantValid(String requestedTenant) {

        if ("SGS_GLOBAL_TENANT".equals(requestedTenant) || ConfigModel.USER_TENANT.equals(requestedTenant)) {
            return true;
        }

        return getAllConfiguredTenantNames().contains(requestedTenant);
    }
  
    @Override
    public Set<String> mapSgRoles(User user, TransportAddress caller) {
        return invertedRoleMappings.evaluate(user, caller, rolesMappingResolution);
    }


}
