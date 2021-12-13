package com.floragunn.searchguard.privileges.extended_action_handling;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.xcontent.ToXContent;

import com.floragunn.searchsupport.reflection.ReflectiveAttributeAccessors;
import com.floragunn.searchsupport.xcontent.AttributeValueFromXContent;

public class ActionConfig {

    private String actionName;
    private Class<?> requestType;
    private String requestTypeName;
    private boolean resolveIndices = true;
    private NewResource createsResource;
    private List<Resource> usesResources = new ArrayList<>();
    private List<RequestPropertyModifier<?>> requestProperyModifiers = new ArrayList<>();
    private Scope scope = Scope.INDEX;

    public static Builder of(String actionName) {
        return new Builder(actionName);
    }

    public ActionConfig() {

    }

    public ActionConfig(String actionName) {
        this.actionName = actionName;
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

    public static class Builder {
        private ActionConfig result;

        Builder(String actionName) {
            result = new ActionConfig(actionName);
        }

        public Builder requestType(Class<?> requestType) {
            result.requestType = requestType;
            return this;
        }

        public Builder requestType(String requestType) {
            try {
                result.requestType = Class.forName(requestType);
            } catch (ClassNotFoundException e) {
                result.requestTypeName = requestType;
            }
            return this;
        }

        public Builder noIndexResolution() {
            result.resolveIndices = false;
            return this;
        }

        public Builder createsResource(String type, Function<ActionResponse, Object> id, Function<ActionResponse, Instant> expiresAfter) {
            result.createsResource = new NewResource(type, id, expiresAfter);
            return this;
        }

        public Builder usesResource(String type, Function<ActionRequest, Object> id) {
            result.usesResources.add(new Resource(type, id, false));
            return this;
        }

        public Builder deletesResource(String type, Function<ActionRequest, Object> id) {
            result.usesResources.add(new Resource(type, id, true));
            return this;
        }

        public <PropertyType> Builder setRequestProperty(String name, Class<PropertyType> type, Function<PropertyType, PropertyType> function) {
            result.requestProperyModifiers.add(new RequestPropertyModifier<>(ReflectiveAttributeAccessors.objectAttr(name, type),
                    ReflectiveAttributeAccessors.setObjectAttr(name, type), type, function));
            return this;
        }

        public Builder scope(Scope scope) {
            result.scope = scope;
            return this;
        }

        public ActionConfig build() {
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

    public static enum Scope {
        INDEX, CLUSTER, TENANT;
    }

    public Scope getScope() {
        return scope;
    }

    public List<RequestPropertyModifier<?>> getRequestProperyModifiers() {
        return requestProperyModifiers;
    }
}
