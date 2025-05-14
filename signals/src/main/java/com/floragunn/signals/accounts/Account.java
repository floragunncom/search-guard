/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.signals.accounts;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContentObject;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.watch.action.handlers.email.EmailAccount;
import com.floragunn.signals.watch.action.handlers.slack.SlackAccount;

public abstract class Account implements ToXContentObject {

    private String id;

    public boolean isInUse(Client client, SignalsSettings settings) {
        String indexName = settings.getStaticSettings().getIndexNames().getWatches();
        SearchRequest request = new SearchRequest(indexName).source(getReferencingWatchesQuery());
        SearchResponse searchResponse = client.search(request).actionGet();
        try {
            long hits = searchResponse.getHits().getTotalHits().value();

            return hits > 0;
        } finally {
            searchResponse.decRef();
        }
    }

    public void isInUse(Client client, SignalsSettings settings, ActionListener<Boolean> actionListener) {
        client.search(new SearchRequest(settings.getStaticSettings().getIndexNames().getWatches()).source(getReferencingWatchesQuery())
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN), new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {
                        if (response.getHits().getTotalHits().value() > 0) {
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

    public final String toJson() {
        return Strings.toString(this);
    }

    public static Account parse(String accountType, String id, String string) throws ConfigValidationException {
        return create(accountType, id, DocNode.parse(Format.JSON).from(string));
    }

    public static Account create(String accountType, String id, DocNode jsonNode) throws ConfigValidationException {

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

        public final A create(String id, DocNode docNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vJsonNode = new ValidatingDocNode(docNode, validationErrors);

            vJsonNode.used("type", "_name");

            A result = create(id, vJsonNode, validationErrors);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        public final A create(String id, ValidatingDocNode vJsonNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            vJsonNode = new ValidatingDocNode(vJsonNode, validationErrors);

            vJsonNode.used("type", "_name");

            A result = create(id, vJsonNode, validationErrors);

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        protected abstract A create(String id, ValidatingDocNode vJsonNode, ValidationErrors validationErrors) throws ConfigValidationException;

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
