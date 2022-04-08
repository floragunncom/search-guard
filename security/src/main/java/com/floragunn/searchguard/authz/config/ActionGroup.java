/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authz.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.sgconf.StaticDefinable;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class ActionGroup implements Document<ActionGroup>, Hideable, StaticDefinable {

    public static ValidationResult<ActionGroup> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        boolean reserved = vNode.get("reserved").withDefault(false).asBoolean();
        boolean hidden = vNode.get("hidden").withDefault(false).asBoolean();
        boolean isStatic = vNode.get("static").withDefault(false).asBoolean();

        String description = vNode.get("description").asString();
        String type = vNode.get("type").asString();
        ImmutableList<String> allowedActions = ImmutableList.of(vNode.get("allowed_actions").required().asListOfStrings());

        vNode.checkForUnusedAttributes();

        return new ValidationResult<ActionGroup>(new ActionGroup(docNode, reserved, hidden, isStatic, description, type, allowedActions),
                validationErrors);
    }

    private final DocNode source;
    private final boolean reserved;
    private final boolean hidden;
    private final boolean isStatic;

    private final String description;
    private final String type;
    private final ImmutableList<String> allowedActions;

    public ActionGroup(DocNode source, boolean reserved, boolean hidden, boolean isStatic, String description, String type,
            ImmutableList<String> allowedActions) {
        this.source = source;
        this.reserved = reserved;
        this.hidden = hidden;
        this.isStatic = isStatic;
        this.description = description;
        this.type = type;
        this.allowedActions = allowedActions;
    }

    public boolean isReserved() {
        return reserved;
    }

    public boolean isHidden() {
        return hidden;
    }

    public boolean isStatic() {
        return isStatic;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    public ImmutableList<String> getAllowedActions() {
        return allowedActions;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }
    
    public static class FlattenedIndex {
        private static final Logger log = LogManager.getLogger(FlattenedIndex.class);

        private final ImmutableMap<String, Set<String>> resolvedActionGroups;

        public FlattenedIndex(SgDynamicConfiguration<ActionGroup> actionGroups) {

            Map<String, Set<String>> resolved = new HashMap<>(actionGroups.getCEntries().size());
            Map<String, Set<String>> needsResolution = new HashMap<>(actionGroups.getCEntries().size());

            for (Map.Entry<String, ActionGroup> entry : actionGroups.getCEntries().entrySet()) {
                String key = entry.getKey();

                Set<String> actions = resolved.computeIfAbsent(key, (k) -> new HashSet<>());

                for (String action : entry.getValue().getAllowedActions()) {
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
}
