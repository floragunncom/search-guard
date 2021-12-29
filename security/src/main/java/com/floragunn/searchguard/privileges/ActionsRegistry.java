package com.floragunn.searchguard.privileges;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;

import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.AdditionalPrivileges;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.NewResource;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.RequestPropertyModifier;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Resource;
import com.floragunn.searchguard.privileges.extended_action_handling.ActionConfig.Scope;
import com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors;
import com.floragunn.searchsupport.util.ImmutableMap;
import com.floragunn.searchsupport.util.ImmutableSet;

class ActionsRegistry {
    //private final ImmutableMap<String, Action> actionMap;

    static class Builder {

        private Map<String, ActionBuilder<?, ?, ?>> builders = new HashMap<>(300);

        <RequestType extends ActionRequest> ActionBuilder<RequestType, Void, Void> cluster(ActionType<?> actionType) {
            ActionBuilder<RequestType, Void, Void> builder = new ActionBuilder<RequestType, Void, Void>(actionType.name(), Scope.CLUSTER);
            builders.put(actionType.name(), builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> cluster(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.CLUSTER);
            builders.put(action, builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> index(ActionType<?> actionType) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(actionType.name(), Scope.INDEX);
            builders.put(actionType.name(), builder);
            return builder;
        }

        ActionBuilder<?, ?, ?> index(String action) {
            ActionBuilder<ActionRequest, ?, ?> builder = new ActionBuilder<ActionRequest, Void, Void>(action, Scope.INDEX);
            builders.put(action, builder);
            return builder;
        }

        ImmutableMap<String, Action> build() {
            ImmutableMap.Builder<String, Action> result = new ImmutableMap.Builder<>(builders.size());

            for (ActionBuilder<?, ?, ?> builder : builders.values()) {
                result.with(builder.actionName, builder.build());
            }

            builders = null;

            return result.build();
        }

        static class ActionBuilder<RequestType extends ActionRequest, RequestItem, RequestItemType> {

            private Class<RequestType> requestType;
            private String requestTypeName;

            private String actionName;
            private Class<Enum<?>> requestItemTypeEnum;
            private boolean resolveIndices = true;
            private NewResource createsResource;
            private List<Resource> usesResources = new ArrayList<>();
            private List<RequestPropertyModifier<?>> requestProperyModifiers = new ArrayList<>();
            private List<AdditionalPrivileges<RequestType, RequestItem>> additionalPrivileges = new ArrayList<>();
            private Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType;
            private Scope scope;
            private Function<RequestType, Collection<RequestItem>> requestItemFunction;
            private Function<RequestItem, RequestItemType> requestItemTypeFunction;

            ActionBuilder(String actionName, Scope scope) {
                this.actionName = actionName;
                this.scope = scope;
            }

            <NewRequestType extends ActionRequest> ActionBuilder<NewRequestType, RequestItem, RequestItemType> requestType(
                    Class<NewRequestType> requestType) {
                if (this.requestType != null && !this.requestType.equals(requestType)) {
                    throw new IllegalStateException("Request type was already set: " + requestType + " vs " + this.requestType);
                }

                @SuppressWarnings("unchecked")
                ActionBuilder<NewRequestType, RequestItem, RequestItemType> newRequestTypeBuilder = (ActionBuilder<NewRequestType, RequestItem, RequestItemType>) this;
                newRequestTypeBuilder.requestType = requestType;
                return newRequestTypeBuilder;
            }

            @SuppressWarnings("unchecked")
            ActionBuilder<RequestType, RequestItem, RequestItemType> requestType(String requestType) {
                try {
                    this.requestType = (Class<RequestType>) Class.forName(requestType);
                } catch (ClassNotFoundException e) {
                    requestTypeName = requestType;
                }
                return this;
            }

            <NewRequestItem> ActionBuilder<RequestType, NewRequestItem, RequestItemType> requestItems(
                    Function<RequestType, Collection<NewRequestItem>> function) {
                @SuppressWarnings("unchecked")
                ActionBuilder<RequestType, NewRequestItem, RequestItemType> newRequestTypeBuilder = (ActionBuilder<RequestType, NewRequestItem, RequestItemType>) this;

                newRequestTypeBuilder.requestItemFunction = function;

                return newRequestTypeBuilder;
            }

            <NewRequestItem, NewRequestItemType> ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> requestItems(
                    Function<RequestType, Collection<NewRequestItem>> function,
                    Function<NewRequestItem, NewRequestItemType> requestItemTypeFunction) {
                @SuppressWarnings("unchecked")
                ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> newRequestTypeBuilder = (ActionBuilder<RequestType, NewRequestItem, NewRequestItemType>) this;

                newRequestTypeBuilder.requestItemFunction = function;
                newRequestTypeBuilder.requestItemTypeFunction = requestItemTypeFunction;

                return newRequestTypeBuilder;
            }

            <NewRequestItem, NewRequestItemType> ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> requestItemsA(
                    Function<RequestType, NewRequestItem[]> function, Function<NewRequestItem, NewRequestItemType> requestItemTypeFunction) {
                @SuppressWarnings("unchecked")
                ActionBuilder<RequestType, NewRequestItem, NewRequestItemType> newRequestTypeBuilder = (ActionBuilder<RequestType, NewRequestItem, NewRequestItemType>) this;

                newRequestTypeBuilder.requestItemFunction = (r) -> Arrays.asList(function.apply(r));
                newRequestTypeBuilder.requestItemTypeFunction = requestItemTypeFunction;

                return newRequestTypeBuilder;
            }

            ActionBuilder<RequestType, RequestItem, RequestItemType> requiresAdditionalPrivileges(Predicate<RequestType> condition, String privilege,
                    String... morePrivileges) {
                additionalPrivileges.add(new AdditionalPrivileges<RequestType, RequestItem>(ImmutableSet.of(privilege, morePrivileges), condition));

                return this;
            }

            ActionBuilder<RequestType, RequestItem, RequestItemType> requiresAdditionalPrivilegesForItemType(RequestItemType requestItemType,
                    String privilege, String... morePrivileges) {
                ImmutableSet<String> privileges = ImmutableSet.of(privilege, morePrivileges);

                if (additionalPrivilegesByItemType == null) {
                    if (Enum.class.isAssignableFrom(requestItemType.getClass())) {
                        @SuppressWarnings("unchecked")
                        Class<RequestItemType> requestItemTypeClass = (Class<RequestItemType>) requestItemType.getClass();
                        @SuppressWarnings({ "rawtypes", "unchecked" })
                        Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType = new EnumMap(requestItemTypeClass);
                        this.additionalPrivilegesByItemType = additionalPrivilegesByItemType;
                        @SuppressWarnings("unchecked")
                        Class<Enum<?>> enumClass = (Class<Enum<?>>) requestItemType.getClass();
                        requestItemTypeEnum = enumClass;
                    } else {
                        additionalPrivilegesByItemType = new HashMap<>();
                    }
                }

                ImmutableSet<String> existingPrivileges = additionalPrivilegesByItemType.get(requestItemType);

                if (existingPrivileges == null) {
                    additionalPrivilegesByItemType.put(requestItemType, privileges);
                } else {
                    additionalPrivilegesByItemType.put(requestItemType, privileges.with(existingPrivileges));
                }

                return this;
            }

            ActionBuilder<RequestType, RequestItem, RequestItemType> noIndexResolution() {
                resolveIndices = false;
                return this;
            }

            ActionBuilder<RequestType, RequestItem, RequestItemType> createsResource(String type, Function<ActionResponse, Object> id,
                    Function<ActionResponse, Instant> expiresAfter) {
                createsResource = new NewResource(type, id, expiresAfter);
                return this;
            }

            ActionBuilder<RequestType, RequestItem, RequestItemType> usesResource(String type, Function<ActionRequest, Object> id) {
                usesResources.add(new Resource(type, id, false));
                return this;
            }

            ActionBuilder<RequestType, RequestItem, RequestItemType> deletesResource(String type, Function<ActionRequest, Object> id) {
                usesResources.add(new Resource(type, id, true));
                return this;
            }

            <PropertyType> ActionBuilder<RequestType, RequestItem, RequestItemType> setRequestProperty(String name, Class<PropertyType> type,
                    Function<PropertyType, PropertyType> function) {
                requestProperyModifiers.add(new RequestPropertyModifier<>(ReflectiveAttributeAccessors.objectAttr(name, type),
                        ReflectiveAttributeAccessors.setObjectAttr(name, type), type, function));
                return this;
            }

            Action build() {
                Action.WellKnownAction.Resources resources = null;

                if (createsResource != null || !usesResources.isEmpty()) {
                    resources = new Action.WellKnownAction.Resources(createsResource, usesResources);
                }

                Action.WellKnownAction.RequestItems<RequestType, RequestItem, RequestItemType> requestItems = null;

                if (requestItemTypeFunction != null || requestItemTypeEnum != null || requestItemTypeFunction != null) {
                    requestItems = new Action.WellKnownAction.RequestItems<RequestType, RequestItem, RequestItemType>(requestItemTypeFunction,
                            requestItemFunction, additionalPrivilegesByItemType, requestItemTypeEnum);
                }

                return new Action.WellKnownAction<RequestType, RequestItem, RequestItemType>(actionName, scope, requestType, requestTypeName,
                        additionalPrivileges, requestItems, resources);
            }

        }

    }
}
