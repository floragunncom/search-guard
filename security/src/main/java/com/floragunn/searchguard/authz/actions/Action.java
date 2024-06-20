/*
 * Copyright 2022-2024 floragunn GmbH
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

package com.floragunn.searchguard.authz.actions;

import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.ActionAuthorization.AliasDataStreamHandling;
import com.floragunn.searchguard.authz.actions.Actions.Scope;

public interface Action {

    String name();

    boolean isIndexPrivilege();

    boolean isClusterPrivilege();

    boolean isTenantPrivilege();
    
    boolean isDataStreamPrivilege();

    boolean isOpen();

    ImmutableSet<Action> getAdditionalPrivileges(ActionRequest request);

    /**
     * Same as getAdditionalPrivileges(), with the difference that the original action is included in the result
     */
    ImmutableSet<Action> expandPrivileges(ActionRequest request);

    boolean requiresSpecialProcessing();

    AliasDataStreamHandling aliasDataStreamHandling();
    
    default <RequestType extends ActionRequest> WellKnownAction<RequestType, ?, ?> wellKnown(RequestType request) {
        if (this instanceof WellKnownAction) {
            @SuppressWarnings("unchecked")
            WellKnownAction<RequestType, ?, ?> result = (WellKnownAction<RequestType, ?, ?>) this;
            result.cast(request);
            return result;
        } else {
            return null;
        }
    }

    public static class WellKnownAction<RequestType extends ActionRequest, RequestItem, RequestItemType> implements Action {
        private final String actionName;
        private final Scope scope;
        private final Class<RequestType> requestType;
        private final String requestTypeName;
        private final ImmutableList<AdditionalPrivileges<RequestType, RequestItem>> additionalPrivileges;
        private final RequestItems<RequestType, RequestItem, RequestItemType> requestItems;
        private final Resources resources;
        private final ImmutableSet<Action> asImmutableSet;
        private final Actions actions;
        private final int hashCode;
        private final AliasDataStreamHandling aliasDataStreamHandling;

        public WellKnownAction(String actionName, Scope scope, Class<RequestType> requestType, String requestTypeName,
                ImmutableList<AdditionalPrivileges<RequestType, RequestItem>> additionalPrivileges,
                ImmutableMap<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType,
                RequestItems<RequestType, RequestItem, RequestItemType> requestItems, Resources resources, AliasDataStreamHandling aliasDataStreamHandling, Actions actions) {
            this.actionName = actionName;
            this.scope = scope;
            this.requestType = requestType;
            this.requestTypeName = requestTypeName;
            this.additionalPrivileges = additionalPrivileges;
            this.requestItems = requestItems;
            this.resources = resources;
            this.actions = actions;
            this.asImmutableSet = ImmutableSet.of(this);
            this.hashCode = actionName.hashCode();
            this.aliasDataStreamHandling = aliasDataStreamHandling;
        }

        @Override
        public String toString() {
            return actionName;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof WellKnownAction)) {
                return false;
            }
            WellKnownAction<?, ?, ?> other = (WellKnownAction<?, ?, ?>) obj;

            return other.hashCode == this.hashCode && other.actionName.equals(this.actionName);
        }

        @Override
        public String name() {
            return actionName;
        }

        @Override
        public boolean isIndexPrivilege() {
            return scope == Scope.INDEX;
        }
        
        @Override
        public boolean isDataStreamPrivilege() {
            return scope == Scope.DATA_STREAM;
        }

        @Override
        public boolean isClusterPrivilege() {
            return scope == Scope.CLUSTER;
        }

        @Override
        public boolean isTenantPrivilege() {
            return scope == Scope.TENANT;
        }

        @Override
        public boolean isOpen() {
            return scope == Scope.OPEN;
        }

        @Override
        public boolean requiresSpecialProcessing() {
            return resources != null && (resources.createsResource != null || !resources.usesResources.isEmpty());
        }

        @Override
        public ImmutableSet<Action> getAdditionalPrivileges(ActionRequest request) {
            RequestType typedRequest = cast(request);

            ImmutableSet<Action> result = ImmutableSet.empty();

            for (AdditionalPrivileges<RequestType, RequestItem> additionalPrivilegeSpec : this.additionalPrivileges) {
                if (additionalPrivilegeSpec.getCondition().test(typedRequest)) {
                    result = result.with(additionalPrivilegeSpec.getPrivilegesAsActions(actions));
                }
            }

            if (requestItems != null) {
                result = result.with(requestItems.evaluateItemTypePrivileges(typedRequest));
            }

            return result;
        }

        @Override
        public ImmutableSet<Action> expandPrivileges(ActionRequest request) {
            RequestType typedRequest = cast(request);

            ImmutableSet<Action> result = asImmutableSet;

            for (AdditionalPrivileges<RequestType, RequestItem> additionalPrivilegeSpec : this.additionalPrivileges) {
                if (additionalPrivilegeSpec.getCondition().test(typedRequest)) {
                    result = result.with(additionalPrivilegeSpec.getPrivilegesAsActions(actions));
                }
            }

            if (requestItems != null) {
                result = result.with(requestItems.evaluateItemTypePrivileges(typedRequest));
            }

            return result;
        }

        public RequestType cast(ActionRequest request) {
            if (requestType != null) {
                return requestType.cast(request);
            } else {
                @SuppressWarnings("unchecked")
                RequestType result = (RequestType) request;
                return result;
            }
        }

        public static class RequestItems<RequestType extends ActionRequest, RequestItem, RequestItemType> {
            private final Function<RequestItem, RequestItemType> requestItemTypeFunction;
            private final Function<RequestType, Collection<RequestItem>> requestItemFunction;
            private final Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType;
            private final Class<Enum<?>> requestItemTypeEnum;
            private final Actions actions;

            public RequestItems(Function<RequestItem, RequestItemType> requestItemTypeFunction,
                    Function<RequestType, Collection<RequestItem>> requestItemFunction,
                    Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType, Class<Enum<?>> requestItemTypeEnum, Actions actions) {
                this.requestItemTypeFunction = requestItemTypeFunction;
                this.requestItemFunction = requestItemFunction;
                this.additionalPrivilegesByItemType = additionalPrivilegesByItemType;
                this.requestItemTypeEnum = requestItemTypeEnum;
                this.actions = actions;
            }

            @SuppressWarnings({ "unchecked", "rawtypes" })
            Set<RequestItemType> createItemTypeSet() {
                if (requestItemTypeEnum != null) {
                    return (Set<RequestItemType>) EnumSet.noneOf((Class) requestItemTypeEnum);
                } else {
                    return new HashSet<>();
                }
            }

            ImmutableSet<Action> evaluateItemTypePrivileges(RequestType request) {
                if (additionalPrivilegesByItemType == null || additionalPrivilegesByItemType.isEmpty()) {
                    return ImmutableSet.empty();
                }

                ImmutableSet<Action> result = ImmutableSet.empty();

                int itemTypeCount = additionalPrivilegesByItemType.size();
                Set<RequestItemType> seenItemTypes = null;

                if (itemTypeCount > 1) {
                    seenItemTypes = createItemTypeSet();
                }

                for (RequestItem requestItem : requestItemFunction.apply(request)) {
                    RequestItemType requestItemType = requestItemTypeFunction.apply(requestItem);
                    ImmutableSet<String> additionalPrivileges = additionalPrivilegesByItemType.get(requestItemType);

                    if (additionalPrivileges == null) {
                        continue;
                    }

                    result = result.with(additionalPrivileges.map((a) -> actions.get(a)));

                    if (itemTypeCount == 1) {
                        break;
                    } else {
                        seenItemTypes.add(requestItemType);

                        if (seenItemTypes.size() == additionalPrivilegesByItemType.size()) {
                            break;
                        }
                    }
                }

                return result;
            }
        }

        public static class Resources {
            private final NewResource createsResource;
            private final ImmutableList<Resource> usesResources;

            public Resources(NewResource createsResource, List<Resource> usesResources) {
                super();
                this.createsResource = createsResource;
                this.usesResources = ImmutableList.of(usesResources);
            }

            public NewResource getCreatesResource() {
                return createsResource;
            }

            public ImmutableList<Resource> getUsesResources() {
                return usesResources;
            }
        }

        public static class AdditionalPrivileges<RequestType extends ActionRequest, RequestItemType> {
            private final ImmutableSet<String> privileges;
            private final Predicate<RequestType> condition;
            private ImmutableSet<Action> privilegesAsActions;

            public AdditionalPrivileges(ImmutableSet<String> privileges, Predicate<RequestType> condition) {
                this.privileges = privileges;
                this.condition = condition;
            }

            public ImmutableSet<String> getPrivileges() {
                return privileges;
            }

            public ImmutableSet<Action> getPrivilegesAsActions(Actions actions) {
                ImmutableSet<Action> result = this.privilegesAsActions;

                if (result == null) {
                    this.privilegesAsActions = result = privileges.map((a) -> actions.get(a));
                }

                return result;
            }

            public Predicate<RequestType> getCondition() {
                return condition;
            }

        }

        public static class Resource {
            private final String type;
            private final Function<ActionRequest, Object> id;
            private final boolean deleteAction;
            private final String ownerCheckBypassPermission;

            public Resource(String type, Function<ActionRequest, Object> id) {
                this(type, id, false, null);
            }
            
            public Resource(String type, Function<ActionRequest, Object> id, boolean deleteAction, String ownerCheckBypassPermission) {
                this.type = type;
                this.id = id;
                this.deleteAction = deleteAction;
                this.ownerCheckBypassPermission = ownerCheckBypassPermission;
            }

            public String getType() {
                return type;
            }

            public Function<ActionRequest, Object> getId() {
                return id;
            }

            public boolean isDeleteAction() {
                return deleteAction;
            }
            
            public Resource deleteAction(boolean deleteAction) {
                return new Resource(this.type, this.id, deleteAction, this.ownerCheckBypassPermission);
            }
            
            public Resource ownerCheckBypassPermission(String permission) {
                return new Resource(this.type, this.id, this.deleteAction, permission);
            }

            public String getOwnerCheckBypassPermission() {
                return ownerCheckBypassPermission;
            }
        }

        public static class NewResource {
            private final String type;
            private final Function<ActionResponse, Object> id;
            private final Function<ActionResponse, Instant> expiresAfter;

            public NewResource(String type, Function<ActionResponse, Object> id, Function<ActionResponse, Instant> expiresAfter) {
                this.type = type;
                this.id = id;
                this.expiresAfter = expiresAfter;
            }

            public String getType() {
                return type;
            }

            public Function<ActionResponse, Object> getId() {
                return id;
            }

            public Function<ActionResponse, Instant> getExpiresAfter() {
                return expiresAfter;
            }
        }

        public static class RequestPropertyModifier<PropertyType> {
            private final Function<ActionRequest, PropertyType> attrGetter;
            private final BiFunction<ActionRequest, PropertyType, ?> attrSetter;
            private final Class<PropertyType> type;
            private final Function<PropertyType, PropertyType> function;

            public RequestPropertyModifier(Function<ActionRequest, PropertyType> attrGetter, BiFunction<ActionRequest, PropertyType, ?> attrSetter,
                    Class<PropertyType> type, Function<PropertyType, PropertyType> function) {
                this.function = function;
                this.type = type;
                this.attrGetter = attrGetter;
                this.attrSetter = attrSetter;
            }

            public Class<PropertyType> getType() {
                return type;
            }

            public Function<PropertyType, PropertyType> getFunction() {
                return function;
            }

            public Function<ActionRequest, PropertyType> getAttrGetter() {
                return attrGetter;
            }

            public BiFunction<ActionRequest, PropertyType, ?> getAttrSetter() {
                return attrSetter;
            }

            public void apply(ActionRequest actionRequest) {
                PropertyType value = attrGetter.apply(actionRequest);
                PropertyType newValue = function.apply(value);
                attrSetter.apply(actionRequest, newValue);
            }

        }

        public Resources getResources() {
            return resources;
        }

        public String getRequestTypeName() {
            return requestTypeName;
        }

        @Override
        public AliasDataStreamHandling aliasDataStreamHandling() {
            return aliasDataStreamHandling;
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
        public boolean isIndexPrivilege() {
            return scope == Scope.INDEX;
        }
        
        @Override
        public boolean isDataStreamPrivilege() {
            return scope == Scope.DATA_STREAM;
        }
        
        @Override
        public boolean isClusterPrivilege() {
            return scope == Scope.CLUSTER;
        }

        @Override
        public boolean isTenantPrivilege() {
            return scope == Scope.TENANT;
        }

        @Override
        public boolean isOpen() {
            return scope == Scope.OPEN;
        }

        @Override
        public ImmutableSet<Action> getAdditionalPrivileges(ActionRequest request) {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<Action> expandPrivileges(ActionRequest request) {
            return ImmutableSet.of(this);
        }

        @Override
        public boolean requiresSpecialProcessing() {
            return false;
        }

        @Override
        public String toString() {
            return actionName;
        }

        @Override
        public int hashCode() {
            return actionName.hashCode();
        }

        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }

            if (!(other instanceof OtherAction)) {
                return false;
            }

            OtherAction otherAction = (OtherAction) other;

            return actionName.equals(otherAction.actionName);
        }

        @Override
        public AliasDataStreamHandling aliasDataStreamHandling() {
            return AliasDataStreamHandling.RESOLVE_IF_NECESSARY;
        }
    }
 
}
