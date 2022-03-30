/*
 * Copyright 2021 floragunn GmbH
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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;

public class ActionGroups {
    private static final Logger log = LogManager.getLogger(ActionGroups.class);

    private final ImmutableMap<String, Set<String>> resolvedActionGroups;

    public ActionGroups(SgDynamicConfiguration<ActionGroupsV7> actionGroups) {

        Map<String, Set<String>> resolved = new HashMap<>(actionGroups.getCEntries().size());
        Map<String, Set<String>> needsResolution = new HashMap<>(actionGroups.getCEntries().size());

        for (Map.Entry<String, ActionGroupsV7> entry : actionGroups.getCEntries().entrySet()) {
            String key = entry.getKey();

            Set<String> actions = resolved.computeIfAbsent(key, (k) -> new HashSet<>());

            for (String action : entry.getValue().getAllowed_actions()) {
                actions.add(action);

                if (actionGroups.getCEntries().containsKey(action) && !action.equals(key)) {
                    needsResolution.computeIfAbsent(key, (k) -> new HashSet<>()).add(action);
                }
            }
        }

        boolean settled = false;

        for (int i = 0; !settled; i++) {
            boolean changed = false;

            for (Map.Entry<String, Set<String>> entry : needsResolution.entrySet()) {
                String key = entry.getKey();
                Set<String> resolvedActions = resolved.get(key);

                for (String action : entry.getValue()) {
                    Set<String> mappedActions = resolved.get(action);

                    changed |= resolvedActions.addAll(mappedActions);
                }
            }

            if (!changed) {
                settled = true;
                if (log.isDebugEnabled()) {
                    log.debug("Action groups settled after " + i + " loops.\nResolved: " + resolved);
                }
            }

            if (i > 100) {
                log.error("Found too deeply nested action groups. Aborting resolution.\nResolved so far: " + resolved);
                break;
            }
        }

        this.resolvedActionGroups = ImmutableMap.of(resolved);
    }

    @Override
    public String toString() {
        return resolvedActionGroups.toString();
    }
    
    public ImmutableSet<String> resolve(Collection<String> actions) {
        ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>();
        
        for (String action : actions) {
            if (action == null) {
                continue;
            }
            
            result.add(action);
            
            Set<String> mappedActions = this.resolvedActionGroups.get(action);            
            if (mappedActions != null) {
                result.addAll(mappedActions);
            }
        }
        
        return result.build();
    }
}
