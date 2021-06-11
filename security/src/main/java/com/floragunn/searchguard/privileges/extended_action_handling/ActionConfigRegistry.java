package com.floragunn.searchguard.privileges.extended_action_handling;

import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.xContentInstantFromMillis;
import static com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors.objectAttr;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Scope.*;

public class ActionConfigRegistry {

    public static final ActionConfigRegistry INSTANCE = new ActionConfigRegistry(
            ActionConfig.of("indices:data/read/async_search/submit").scope(CLUSTER)
                    .createsResource("async_search", objectAttr("id"), xContentInstantFromMillis("expiration_time_in_millis")).build(),
            ActionConfig.of("indices:data/read/async_search/get").scope(CLUSTER).usesResource("async_search", objectAttr("id")).build(),
            ActionConfig.of("indices:data/read/async_search/delete").scope(CLUSTER).deletesResource("async_search", objectAttr("id")).build(),
            ActionConfig.of("indices:data/read/sql").scope(CLUSTER).build(),
            ActionConfig.of("indices:data/read/sql/translate").scope(CLUSTER).build(),
            ActionConfig.of("indices:data/read/sql/close_cursor").scope(CLUSTER).build());

    private Map<String, ActionConfig> actionsByNameMap;
    private Set<String> clusterActions;
    private Set<String> tenantActions;

    public ActionConfigRegistry(ActionConfig... actionConfigs) {
        Map<String, ActionConfig> actionsByNameMap = new HashMap<>();
        Set<String> clusterActions = new HashSet<>();
        Set<String> tenantActions = new HashSet<>();

        for (ActionConfig actionConfig : actionConfigs) {
            actionsByNameMap.put(actionConfig.getActionName(), actionConfig);

            if (actionConfig.getScope() == ActionConfig.Scope.CLUSTER) {
                clusterActions.add(actionConfig.getActionName());
            } else if (actionConfig.getScope() == ActionConfig.Scope.TENANT) {
                tenantActions.add(actionConfig.getActionName());
            }
        }

        this.actionsByNameMap = Collections.unmodifiableMap(actionsByNameMap);
        this.clusterActions = Collections.unmodifiableSet(clusterActions);
        this.tenantActions = Collections.unmodifiableSet(tenantActions);
    }

    public ActionConfig get(String actionName) {
        return actionsByNameMap.get(actionName);
    }

    public boolean isClusterAction(String actionName) {
        return clusterActions.contains(actionName);
    }

    public boolean isTenantAction(String actionName) {
        return tenantActions.contains(actionName);
    }
}
