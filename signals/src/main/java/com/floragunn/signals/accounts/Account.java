package com.floragunn.signals.accounts;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidatingJsonParser;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;

public abstract class Account implements ToXContentObject {

    private String id;

    public boolean isInUse(Client client, SignalsSettings settings) {
        long hits = client.search(new SearchRequest(settings.getStaticSettings().getIndexNames().getWatches()).source(getReferencingWatchesQuery()))
                .actionGet().getHits().getTotalHits().value;

        return hits > 0;
    }

    public void isInUse(Client client, SignalsSettings settings, ActionListener<Boolean> actionListener) {
        client.search(new SearchRequest(settings.getStaticSettings().getIndexNames().getWatches()).source(getReferencingWatchesQuery())
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN), new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {
                        if (response.getHits().getTotalHits().value > 0) {
                            actionListener.onResponse(Boolean.TRUE);
                        } else {
                            actionListener.onResponse(Boolean.FALSE);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        actionListener.onFailure(e);
                    }
                });
    }

    public abstract SearchSourceBuilder getReferencingWatchesQuery();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getScopedId() {
        return getType() + "/" + id;
    }

    public abstract String getType();

    public final String toJson() throws JsonProcessingException {
        return Strings.toString(this);
    }

    public static Account parse(String accountType, String id, String string) throws ConfigValidationException {
        return create(accountType, id, ValidatingJsonParser.readTree(string));
    }

    public static Account create(String accountType, String id, JsonNode jsonNode) throws ConfigValidationException {

        Factory<?> factory = factoryRegistry.get(accountType);

        if (factory == null) {
            throw new ConfigValidationException(new InvalidAttributeValue("type", accountType, factoryRegistry.getFactoryNames()));
        }

        return (Account) factory.create(id, jsonNode);
    }

    public static abstract class Factory<A extends Account> {
        private final String type;

        protected Factory(String type) {
            this.type = type;
        }

        public final A create(String id, JsonNode jsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

            vJsonNode.used("type", "_name");

            A result = create(id, vJsonNode, validationErrors);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        public final A create(String id, ValidatingJsonNode vJsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            vJsonNode = new ValidatingJsonNode(vJsonNode, validationErrors);
            
            vJsonNode.used("type", "_name");

            A result = create(id, vJsonNode, validationErrors);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        protected abstract A create(String id, ValidatingJsonNode vJsonNode, ValidationErrors validationErrors) throws ConfigValidationException;

        public abstract Class<A> getImplClass();

        public String getType() {
            return type;
        }
    }

    public static final class FactoryRegistry {
        private final Map<String, Factory<?>> factories = new HashMap<>();
        private String factoryNames;

        FactoryRegistry(Factory<?>... factories) {
            add(factories);
        }

        private void internalAddFactory(Factory<?> factory) {
            if (factory.getType() == null) {
                throw new IllegalArgumentException("type of factory is null: " + factory);
            }

            if (factories.containsKey(factory.getType())) {
                throw new IllegalStateException("Factory of type " + factory.getType() + " (" + factory + ") was already installed: " + factories);
            }

            factories.put(factory.getType().toLowerCase(), factory);
        }

        public void add(Factory<?>... factories) {
            for (Factory<?> factory : factories) {
                internalAddFactory(factory);
            }

            factoryNames = String.join("|", new TreeSet<>(this.factories.keySet()));
        }

        public <A extends Account> Factory<A> get(Class<A> implClass) {
            for (Factory<?> factory : factories.values()) {
                if (factory.getImplClass().equals(implClass)) {
                    @SuppressWarnings("unchecked")
                    Factory<A> result = (Factory<A>) factory;
                    return result;
                }
            }

            return null;
        }

        Factory<?> get(String type) {
            return factories.get(type.toLowerCase());
        }

        String getFactoryNames() {
            return factoryNames;
        }
    }

    public static final FactoryRegistry factoryRegistry = new FactoryRegistry(new EmailAccount.Factory(), new SlackAccount.Factory());

}
