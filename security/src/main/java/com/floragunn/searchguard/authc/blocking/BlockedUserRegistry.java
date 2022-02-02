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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;

import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.BlocksV7;

public class BlockedUserRegistry {
    protected static final Logger log = LogManager.getLogger(BlockedIpRegistry.class);

    private volatile ClientBlockRegistry<String> blockedUsers;
    
    public BlockedUserRegistry(ConfigurationRepository configurationRepository) {
        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<BlocksV7> blocks = configMap.get(CType.BLOCKS);

                if (blocks != null) {
                    blockedUsers = reloadBlockedUsers(blocks);
                    
                    if (log.isDebugEnabled()) {
                        log.debug("Updated confiuration: " + blocks + "\nBlockedUsers: " + blockedUsers);
                    }
                } 
            }
        });
    }
    

    public boolean isUserBlocked(String userName) {
        if (userName == null) {
            return false;
        }

        if (blockedUsers == null) {
            return false;
        }
        
        return blockedUsers.isBlocked(userName);
    }
    
    

    private ClientBlockRegistry<String> reloadBlockedUsers(SgDynamicConfiguration<BlocksV7> blocks) {
        Tuple<Set<String>, Set<String>> b = readBlocks(blocks, BlocksV7.Type.name);
        return new WildcardVerdictBasedBlockRegistry(b.v1(), b.v2());
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
