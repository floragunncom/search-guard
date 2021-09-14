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

package com.floragunn.searchguard.privileges.extended_action_handling;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContent;

import com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors;
import com.floragunn.searchsupport.util.ImmutableSet;
import com.floragunn.searchsupport.xcontent.AttributeValueFromXContent;

public class ActionConfig<RequestType extends ActionRequest, RequestItem, RequestItemType> {

    private String actionName;
    private Class<RequestType> requestType;
    private Class<Enum<?>> requestItemTypeEnum;
    private String requestTypeName;
    private boolean resolveIndices = true;
    private NewResource createsResource;
    private List<Resource> usesResources = new ArrayList<>();
    private List<RequestPropertyModifier<?>> requestProperyModifiers = new ArrayList<>();
    private List<AdditionalPrivileges<RequestType, RequestItem>> additionalPrivileges = new ArrayList<>();
    private Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType;
    private Scope scope = Scope.INDEX;
    private Function<RequestType, Collection<RequestItem>> requestItemFunction;
    private Function<RequestItem, RequestItemType> requestItemTypeFunction;

    public static Builder<ActionRequest, Void, Void> of(String actionName) {
        return new Builder<ActionRequest, Void, Void>(actionName);
    }

    public ActionConfig() {

    }

    public ActionConfig(String actionName) {
        this.actionName = actionName;
    }

    public ImmutableSet<String> evaluateAdditionalPrivileges(RequestType request) {
        if (additionalPrivileges.isEmpty() && additionalPrivilegesByItemType == null) {
            return ImmutableSet.empty();
        }

        ImmutableSet<String> result = ImmutableSet.empty();

        for (AdditionalPrivileges<RequestType, RequestItem> additionalPrivilegeSpec : this.additionalPrivileges) {
            if (additionalPrivilegeSpec.condition.test(request)) {
                result = result.with(additionalPrivilegeSpec.getPrivileges());
            }
        }

        if (additionalPrivilegesByItemType != null && !additionalPrivilegesByItemType.isEmpty()) {
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

                result = result.with(additionalPrivileges);

                if (itemTypeCount == 1) {
                    break;
                } else {
                    seenItemTypes.add(requestItemType);

                    if (seenItemTypes.size() == additionalPrivilegesByItemType.size()) {
                        break;
                    }
                }
            }
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Set<RequestItemType> createItemTypeSet() {
        if (requestItemTypeEnum != null) {
            return (Set<RequestItemType>) EnumSet.noneOf((Class) requestItemTypeEnum);
        } else {
            return new HashSet<>();
        }
    }

    public static class AdditionalPrivileges<RequestType extends ActionRequest, RequestItemType> {
        private final ImmutableSet<String> privileges;
        private final Predicate<RequestType> condition;

        public AdditionalPrivileges(ImmutableSet<String> privileges, Predicate<RequestType> condition) {
            this.privileges = privileges;
            this.condition = condition;
        }

        public ImmutableSet<String> getPrivileges() {
            return privileges;
        }

        public Predicate<RequestType> getCondition() {
            return condition;
        }

    }

    public static class Resource {
        private final String type;
        private final Function<ActionRequest, Object> id;
        private final boolean deleteAction;

        public Resource(String type, Function<ActionRequest, Object> id, boolean deleteAction) {
            this.type = type;
            this.id = id;
            this.deleteAction = deleteAction;
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

    public static class Builder<RequestType extends ActionRequest, RequestItem, RequestItemType> {
        private ActionConfig<RequestType, RequestItem, RequestItemType> result;

        Builder(String actionName) {
            result = new ActionConfig<RequestType, RequestItem, RequestItemType>(actionName);
        }

        public <NewRequestType extends ActionRequest> Builder<NewRequestType, RequestItem, RequestItemType> requestType(
                Class<NewRequestType> requestType) {
            if (result.requestType != null && !result.requestType.equals(requestType)) {
                throw new IllegalStateException("Request type was already set: " + requestType + " vs " + result.requestType);
            }

            @SuppressWarnings("unchecked")
            Builder<NewRequestType, RequestItem, RequestItemType> newRequestTypeBuilder = (Builder<NewRequestType, RequestItem, RequestItemType>) this;
            newRequestTypeBuilder.result.requestType = requestType;
            return newRequestTypeBuilder;
        }

        @SuppressWarnings("unchecked")
        public Builder<RequestType, RequestItem, RequestItemType> requestType(String requestType) {
            try {
                result.requestType = (Class<RequestType>) Class.forName(requestType);
            } catch (ClassNotFoundException e) {
                result.requestTypeName = requestType;
            }
            return this;
        }

        public <NewRequestItem> Builder<RequestType, NewRequestItem, RequestItemType> requestItems(
                Function<RequestType, Collection<NewRequestItem>> function) {
            @SuppressWarnings("unchecked")
            Builder<RequestType, NewRequestItem, RequestItemType> newRequestTypeBuilder = (Builder<RequestType, NewRequestItem, RequestItemType>) this;

            newRequestTypeBuilder.result.requestItemFunction = function;

            return newRequestTypeBuilder;
        }

        public <NewRequestItem, NewRequestItemType> Builder<RequestType, NewRequestItem, NewRequestItemType> requestItems(
                Function<RequestType, Collection<NewRequestItem>> function, Function<NewRequestItem, NewRequestItemType> requestItemTypeFunction) {
            @SuppressWarnings("unchecked")
            Builder<RequestType, NewRequestItem, NewRequestItemType> newRequestTypeBuilder = (Builder<RequestType, NewRequestItem, NewRequestItemType>) this;

            newRequestTypeBuilder.result.requestItemFunction = function;
            newRequestTypeBuilder.result.requestItemTypeFunction = requestItemTypeFunction;

            return newRequestTypeBuilder;
        }

        public <NewRequestItem, NewRequestItemType> Builder<RequestType, NewRequestItem, NewRequestItemType> requestItemsA(
                Function<RequestType, NewRequestItem[]> function, Function<NewRequestItem, NewRequestItemType> requestItemTypeFunction) {
            @SuppressWarnings("unchecked")
            Builder<RequestType, NewRequestItem, NewRequestItemType> newRequestTypeBuilder = (Builder<RequestType, NewRequestItem, NewRequestItemType>) this;

            newRequestTypeBuilder.result.requestItemFunction = (r) -> Arrays.asList(function.apply(r));
            newRequestTypeBuilder.result.requestItemTypeFunction = requestItemTypeFunction;

            return newRequestTypeBuilder;
        }

        public Builder<RequestType, RequestItem, RequestItemType> requiresAdditionalPrivileges(Predicate<RequestType> condition, String privilege,
                String... morePrivileges) {
            result.additionalPrivileges
                    .add(new AdditionalPrivileges<RequestType, RequestItem>(ImmutableSet.of(privilege, morePrivileges), condition));

            return this;
        }

        public Builder<RequestType, RequestItem, RequestItemType> requiresAdditionalPrivilegesForItemType(RequestItemType requestItemType,
                String privilege, String... morePrivileges) {
            ImmutableSet<String> privileges = ImmutableSet.of(privilege, morePrivileges);

            if (result.additionalPrivilegesByItemType == null) {
                if (Enum.class.isAssignableFrom(requestItemType.getClass())) {
                    @SuppressWarnings("unchecked")
                    Class<RequestItemType> requestItemTypeClass = (Class<RequestItemType>) requestItemType.getClass();
                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    Map<RequestItemType, ImmutableSet<String>> additionalPrivilegesByItemType = new EnumMap(requestItemTypeClass);
                    result.additionalPrivilegesByItemType = additionalPrivilegesByItemType;
                    @SuppressWarnings("unchecked")
                    Class<Enum<?>> enumClass = (Class<Enum<?>>) requestItemType.getClass();
                    result.requestItemTypeEnum = enumClass;
                } else {
                    result.additionalPrivilegesByItemType = new HashMap<>();
                }
            }

            ImmutableSet<String> existingPrivileges = result.additionalPrivilegesByItemType.get(requestItemType);

            if (existingPrivileges == null) {
                result.additionalPrivilegesByItemType.put(requestItemType, privileges);
            } else {
                result.additionalPrivilegesByItemType.put(requestItemType, privileges.with(existingPrivileges));
            }

            return this;
        }

        public Builder<RequestType, RequestItem, RequestItemType> noIndexResolution() {
            result.resolveIndices = false;
            return this;
        }

        public Builder<RequestType, RequestItem, RequestItemType> createsResource(String type, Function<ActionResponse, Object> id,
                Function<ActionResponse, Instant> expiresAfter) {
            result.createsResource = new NewResource(type, id, expiresAfter);
            return this;
        }

        public Builder<RequestType, RequestItem, RequestItemType> usesResource(String type, Function<ActionRequest, Object> id) {
            result.usesResources.add(new Resource(type, id, false));
            return this;
        }

        public Builder<RequestType, RequestItem, RequestItemType> deletesResource(String type, Function<ActionRequest, Object> id) {
            result.usesResources.add(new Resource(type, id, true));
            return this;
        }

        public <PropertyType> Builder<RequestType, RequestItem, RequestItemType> setRequestProperty(String name, Class<PropertyType> type,
                Function<PropertyType, PropertyType> function) {
            result.requestProperyModifiers.add(new RequestPropertyModifier<>(ReflectiveAttributeAccessors.objectAttr(name, type),
                    ReflectiveAttributeAccessors.setObjectAttr(name, type), type, function));
            return this;
        }

        public Builder<RequestType, RequestItem, RequestItemType> scope(Scope scope) {
            result.scope = scope;
            return this;
        }

        public ActionConfig<RequestType, RequestItem, RequestItemType> build() {
            result.usesResources = Collections.unmodifiableList(result.usesResources);
            result.requestProperyModifiers = Collections.unmodifiableList(result.requestProperyModifiers);

            return result;
        }

    }

    public String getActionName() {
        return actionName;
    }

    public Class<?> getRequestType() {
        return requestType;
    }

    public boolean isResolveIndices() {
        return resolveIndices;
    }

    public NewResource getCreatesResource() {
        return createsResource;
    }

    public List<Resource> getUsesResources() {
        return usesResources;
    }

    public String getRequestTypeName() {
        return requestTypeName;
    }

    public static <O> Function<O, Object> xContentAttr(String name) {
        return (actionResponse) -> AttributeValueFromXContent.get((ToXContent) actionResponse, name);
    }

    public static <O> Function<O, Instant> xContentInstantFromMillis(String name) {
        return (actionResponse) -> {
            Object value = AttributeValueFromXContent.get((ToXContent) actionResponse, name);

            if (value instanceof Number) {
                return Instant.ofEpochMilli(((Number) value).longValue());
            } else if (value == null) {
                return null;
            } else {
                throw new RuntimeException("Unexpected value " + value + " for attribute " + name);
            }
        };
    }

    public static <T> Predicate<T> ifNotEmpty(Function<T, Collection<?>> itemFunction) {
        return (t) -> {
            Collection<?> items = itemFunction.apply(t);

            return items != null && !items.isEmpty();
        };
    }

    public static enum Scope {
        INDEX, CLUSTER, TENANT;
    }

    public Scope getScope() {
        return scope;
    }

    public List<RequestPropertyModifier<?>> getRequestProperyModifiers() {
        return requestProperyModifiers;
    }

    public static Predicate<? extends ActionRequest> ALWAYS = (request) -> true;

    public static <Request extends ActionRequest> Predicate<Request> always() {
        return (request) -> true;
    }
}
