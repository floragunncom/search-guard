package com.floragunn.searchguard.authz;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.action.ActionRequest;

import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.AdditionalPrivileges;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.NewResource;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Resource;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Scope;
import com.floragunn.searchsupport.util.ImmutableSet;

public interface Action {
    
    String name();
    boolean isClusterPrivilege();
    boolean isTenantPrivilege();

    public static class WellKnownAction<RequestType extends ActionRequest, RequestItem, RequestItemType> implements Action {
        private final String actionName;

        private final Scope scope;

        private final Class<RequestType> requestType;
        private final String requestTypeName;

        private final List<AdditionalPrivileges<RequestType, RequestItem>> additionalPrivileges;

        private final RequestItems<RequestType, RequestItem, RequestItemType> requestItems;

        private final Resources resources;

        public WellKnownAction(String actionName, Scope scope, Class<RequestType> requestType, String requestTypeName,
                List<AdditionalPrivileges<RequestType, RequestItem>> additionalPrivileges,
                RequestItems<RequestType, RequestItem, RequestItemType> requestItems, Resources resources) {
            this.actionName = actionName;
            this.scope = scope;
            this.requestType = requestType;
            this.requestTypeName = requestTypeName;
            this.additionalPrivileges = additionalPrivileges;
            this.requestItems = requestItems;
            this.resources = resources;
        }
        
        @Override 
        public String name() {
            return actionName;
        }
        
        @Override
        public boolean isClusterPrivilege() {
            return scope == Scope.CLUSTER;
        }
        
        @Override 
        public boolean isTenantPrivilege() {
            return scope == Scope.TENANT;
        }

        public static class RequestItems<RequestType extends ActionRequest, RequestItem, RequestItemType> {
            private final Function<RequestItem, RequestItemType> requestItemTypeFunction;
            private final Function<RequestType, Collection<RequestItem>> requestItemFunction;
            private final Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType;
            private final Class<Enum<?>> requestItemTypeEnum;

            public RequestItems(Function<RequestItem, RequestItemType> requestItemTypeFunction,
                    Function<RequestType, Collection<RequestItem>> requestItemFunction,
                    Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType, Class<Enum<?>> requestItemTypeEnum) {
                this.requestItemTypeFunction = requestItemTypeFunction;
                this.requestItemFunction = requestItemFunction;
                this.additionalPrivilegesByItemType = additionalPrivilegesByItemType;
                this.requestItemTypeEnum = requestItemTypeEnum;
            }
        }

        public static class Resources {
            private final NewResource createsResource;
            private final List<Resource> usesResources;

            public Resources(NewResource createsResource, List<Resource> usesResources) {
                super();
                this.createsResource = createsResource;
                this.usesResources = usesResources;
            }

        }
    }

    public static class OtherAction implements Action {
        private final String actionName;
        private final Scope scope;

        public OtherAction(String actionName, Scope scope) {
            this.actionName = actionName;
            this.scope = scope;
        }

        @Override 
        public String name() {
            return actionName;
        }


        public Scope getScope() {
            return scope;
        }

        @Override
        public boolean isClusterPrivilege() {
            return scope == Scope.CLUSTER;
        }
        
        @Override 
        public boolean isTenantPrivilege() {
            return scope == Scope.TENANT;
        }

    }
}
