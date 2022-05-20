package com.floragunn.signals.watch.action.handlers;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.opensearch.common.Strings;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.action.handlers.email.EmailAction;
import com.floragunn.signals.watch.action.handlers.slack.SlackAction;
import com.floragunn.signals.watch.common.WatchElement;
import com.floragunn.signals.watch.init.WatchInitializationService;

public abstract class ActionHandler extends WatchElement {

    protected ActionHandler() {

    }

    public abstract ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException;

    public abstract String getType();

    public String toJson() {
        return Strings.toString(this);
    }

    public static ActionHandler create(WatchInitializationService watchInitService, ValidatingDocNode jsonNode) throws ConfigValidationException {

        String type = null;

        if (jsonNode.hasNonNull("type")) {
            type = jsonNode.get("type").asString();
        } else {
            throw new ConfigValidationException(new MissingAttribute("type", jsonNode));
        }

        Factory<?> factory = factoryRegistry.get(type);

        if (factory != null) {
            return factory.create(watchInitService, jsonNode);
        } else {
            throw new ConfigValidationException(new InvalidAttributeValue("type", type, factoryRegistry.getFactoryNames(), jsonNode));
        }
    }

    public static ActionHandler create(WatchInitializationService watchInitService, DocNode jsonNode) throws ConfigValidationException {

        String type = null;

        if (jsonNode.hasNonNull("type")) {
            type = jsonNode.getAsString("type");
        } else {
            throw new ConfigValidationException(new MissingAttribute("type", jsonNode));
        }

        Factory<?> factory = factoryRegistry.get(type);

        if (factory != null) {
            return factory.create(watchInitService, jsonNode);
        } else {
            throw new ConfigValidationException(new InvalidAttributeValue("type", type, factoryRegistry.getFactoryNames(), jsonNode));
        }
    }

    public static ActionHandler parseJson(WatchInitializationService ctx, String json) throws ConfigValidationException {
        DocNode jsonNode = DocNode.parse(Format.JSON).from(json);

        return create(ctx, jsonNode);
    }

    public static abstract class Factory<A extends ActionHandler> {
        private final String type;

        protected Factory(String type) {
            this.type = type;
        }

        public final A create(WatchInitializationService watchInitService, DocNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

            A result = create(watchInitService, vJsonNode, validationErrors);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        public final A create(WatchInitializationService watchInitService, ValidatingDocNode vJsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            vJsonNode = new ValidatingDocNode(vJsonNode, validationErrors);

            A result = create(watchInitService, vJsonNode, validationErrors);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        protected abstract A create(WatchInitializationService watchInitService, ValidatingDocNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException;

        public String getType() {
            return type;
        }
    }

    public static final class FactoryRegistry {
        private final Map<String, Factory<?>> factories = new HashMap<>();
        private String factoryNames;

        FactoryRegistry(ActionHandler.Factory<?>... factories) {
            add(factories);
        }

        private void internalAddFactory(ActionHandler.Factory<?> factory) {
            if (factory.getType() == null) {
                throw new IllegalArgumentException("type of factory is null: " + factory);
            }

            if (factories.containsKey(factory.getType())) {
                throw new IllegalStateException("Factory of type " + factory.getType() + " (" + factory + ") was already installed: " + factories);
            }

            factories.put(factory.getType().toLowerCase(), factory);
        }

        public void add(ActionHandler.Factory<?>... factories) {
            for (Factory<?> factory : factories) {
                internalAddFactory(factory);
            }

            factoryNames = String.join("|", new TreeSet<>(this.factories.keySet()));
        }

        public Factory<?> get(String type) {
            return factories.get(type.toLowerCase());
        }

        String getFactoryNames() {
            return factoryNames;
        }
    }

    public static final FactoryRegistry factoryRegistry = new FactoryRegistry(new IndexAction.Factory(), new WebhookAction.Factory(),
            new EmailAction.Factory(), new SlackAction.Factory());

}
