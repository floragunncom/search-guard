/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.transport;

import java.util.List;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.NoSuchComponentException;
import com.floragunn.searchguard.TypedComponentRegistry;
import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.UserInformationBackend;
import com.floragunn.searchguard.authc.base.AcceptanceRules;
import com.floragunn.searchguard.authc.base.StandardAuthenticationDomain;
import com.floragunn.searchguard.authc.base.UserMapping;
import com.floragunn.searchguard.authc.transport.TransportAuthenticationDomain.TransportAuthenticationFrontend;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

public class TransportAuthenticationDomain extends StandardAuthenticationDomain<TransportAuthenticationFrontend> {

    public TransportAuthenticationDomain(DocNode source, String type, String id, String description, boolean enabled, int order,
            AcceptanceRules acceptanceRules, TransportAuthenticationFrontend authenticationFrontend, AuthenticationBackend authenticationBackend,
            ImmutableList<UserInformationBackend> additionalUserInformationBackends, UserMapping userMapping, MetricsLevel metricsLevel) {
        super(source, type, id, description, enabled, order, acceptanceRules, authenticationFrontend, authenticationBackend,
                additionalUserInformationBackends, userMapping, metricsLevel);
    }

    public static AuthenticationDomain<TransportAuthenticationFrontend> parse(DocNode documentNode, ConfigurationRepository.Context context,
            MetricsLevel metricsLevel) throws ConfigValidationException {
        TypedComponentRegistry typedComponentRegistry = context.modulesRegistry().getTypedComponentRegistry();

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(documentNode, validationErrors);

        String id = vNode.get("id").asString();
        String description = vNode.get("description").asString();
        boolean enabled = vNode.get("enabled").withDefault(true).asBoolean();
        int order = vNode.get("order").withDefault(0).asInt();
        AcceptanceRules acceptanceRules = new AcceptanceRules(vNode.get("accept").by(AcceptanceRules.Criteria::parse),
                vNode.get("skip").by(AcceptanceRules.Criteria::parse));
        UserMapping userMapping = vNode.get("user_mapping").by(UserMapping::parse);

        String type = vNode.get("type").required().asString();

        AuthenticationBackend authenticationBackend = null;

        if (type != null) {
            String authenticatorType;
            String backendType;

            int slash = type.indexOf('/');

            if (slash == -1) {
                authenticatorType = type;
                backendType = null;
            } else {
                authenticatorType = type.substring(0, slash);
                backendType = type.substring(slash + 1);
            }

            if (!authenticatorType.equals("basic")) {
                validationErrors.add(new InvalidAttributeValue("type", type, "basic").message("Invalid transport authenticator"));
            }

            if (backendType != null) {
                try {
                    authenticationBackend = typedComponentRegistry.create(AuthenticationBackend.class, type,
                            vNode.getDocumentNode().getAsNode(backendType), context);
                } catch (ConfigValidationException e) {
                    validationErrors.add(backendType, e);
                } catch (NoSuchComponentException e) {
                    validationErrors.add(new InvalidAttributeValue("type", type, e.getAvailableTypesAsInfoString())
                            .message("Unknown authentication backend").cause(e));
                }
            } else {
                validationErrors.add(
                        new InvalidAttributeValue("type", type, typedComponentRegistry.getAvailableSubTypesAsShortString(AuthenticationBackend.class))
                                .message("The authentication backend type must be specified behind a slash in the type attribute"));
            }
        }

        ImmutableList<UserInformationBackend> additionalUserInformationBackends = ImmutableList.empty();

        if (vNode.hasNonNull("additional_user_information")) {
            try {
                additionalUserInformationBackends = parseAdditionalUserInformationBackends(
                        vNode.getDocumentNode().getAsListOfNodes("additional_user_information"), context);
            } catch (ConfigValidationException e) {
                validationErrors.add("additional_user_information", e);
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return new TransportAuthenticationDomain(documentNode, type, id, description, enabled, order, acceptanceRules,
                new TransportAuthenticationFrontend(), authenticationBackend, additionalUserInformationBackends, userMapping, metricsLevel);
    }

    private static ImmutableList<UserInformationBackend> parseAdditionalUserInformationBackends(List<DocNode> list,
            ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ImmutableList.Builder<UserInformationBackend> result = new ImmutableList.Builder<>(list.size());

        for (int i = 0; i < list.size(); i++) {
            try {
                result.with(parseAdditionalUserInformationBackend(list.get(i), context));
            } catch (ConfigValidationException e) {
                validationErrors.add(String.valueOf(i), e);
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return result.build();
    }

    private static UserInformationBackend parseAdditionalUserInformationBackend(DocNode docNode, ConfigurationRepository.Context context)
            throws ConfigValidationException {
        TypedComponentRegistry typedComponentRegistry = context.modulesRegistry().getTypedComponentRegistry();

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        String type = vNode.get("type").required().expected(typedComponentRegistry.getAvailableSubTypesAsShortString(UserInformationBackend.class))
                .asString();

        validationErrors.throwExceptionForPresentErrors();

        // type is not null

        try {
            return typedComponentRegistry.create(UserInformationBackend.class, type, docNode.getAsNode(type), context);
        } catch (ConfigValidationException e) {
            validationErrors.add(type, e);
        } catch (NoSuchComponentException e) {
            validationErrors.add(
                    new InvalidAttributeValue("type", type, e.getAvailableTypesAsInfoString()).message("Unknown authentication backend").cause(e));
        }

        throw new ConfigValidationException(validationErrors);
    }

    /**
     * For now, just a dummy class to keep the interface happy. There is no real authentication frontend for transport requests.
     */
    public static class TransportAuthenticationFrontend implements AuthenticationFrontend {

        private final ComponentState componentState = new ComponentState(0, "authentication_frontend", "transport",
                TransportAuthenticationFrontend.class).initialized();

        @Override
        public String getType() {
            return "transport";
        }

        @Override
        public ComponentState getComponentState() {
            return componentState;
        }

    }

}
