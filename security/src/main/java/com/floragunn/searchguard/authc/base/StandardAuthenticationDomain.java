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

package com.floragunn.searchguard.authc.base;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.searchguard.NoSuchComponentException;
import com.floragunn.searchguard.TypedComponentRegistry;
import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.UserInformationBackend;
import com.floragunn.searchguard.authc.AuthenticationBackend.UserMapper;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.AuthDomainInfo;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableList;
import com.google.common.hash.Hashing;

public class StandardAuthenticationDomain<AuthenticatorType extends AuthenticationFrontend> implements AuthenticationDomain<AuthenticatorType>,
        Comparable<StandardAuthenticationDomain<AuthenticatorType>>, Document<StandardAuthenticationDomain<AuthenticatorType>> {

    private final DocNode source;
    private final String type;
    private final String id;
    private final AuthenticationBackend authenticationBackend;
    private final AuthenticatorType authenticationFrontend;

    private final boolean enabled;
    private final AcceptanceRules acceptanceRules;
    private final UserMapping userMapping;
    private final String description;
    private final String infoString;
    private final ImmutableList<UserInformationBackend> additionalUserInformationBackends;

    /**
     * Only for supporting the legacy config format
     */
    private final int order;

    public StandardAuthenticationDomain(DocNode source, String type, String id, String description, boolean enabled, int order,
            AcceptanceRules acceptanceRules, AuthenticatorType authenticationFrontend, AuthenticationBackend authenticationBackend,
            ImmutableList<UserInformationBackend> additionalUserInformationBackends, UserMapping userMapping) {
        this.source = source;
        this.type = type;
        this.id = id;
        this.description = description;
        this.enabled = enabled;
        this.authenticationFrontend = authenticationFrontend;
        this.authenticationBackend = authenticationBackend;
        this.order = order;
        this.additionalUserInformationBackends = additionalUserInformationBackends;
        this.userMapping = userMapping;
        this.infoString = buildInfoString();
        this.acceptanceRules = acceptanceRules;
    }

    public AuthenticationBackend getBackend() {
        return authenticationBackend;
    }

    public AuthenticatorType getFrontend() {
        return authenticationFrontend;
    }

    public int getOrder() {
        return order;
    }

    @Override
    public int compareTo(final StandardAuthenticationDomain<AuthenticatorType> o) {
        return Integer.compare(this.order, o.order);
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean accept(RequestMetaData<?> requestMetaData) {
        return acceptanceRules.accept(requestMetaData);
    }

    @Override
    public boolean accept(AuthCredentials authCredentials) {
        return acceptanceRules.accept(authCredentials);
    }

    @Override
    public String toString() {
        return infoString;
    }

    private String buildInfoString() {
        StringBuilder result = new StringBuilder(type);

        if (id != null) {
            result.append("[").append(id).append("]");
        }

        return result.toString();
    }

    public static <AuthenticatorType extends AuthenticationFrontend> AuthenticationDomain<AuthenticatorType> parse(DocNode documentNode,
            Class<AuthenticatorType> authenticatorType, ConfigurationRepository.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(documentNode, validationErrors);

        return parse(vNode, validationErrors, authenticatorType, context);
    }

    public static <AuthenticatorType extends AuthenticationFrontend> AuthenticationDomain<AuthenticatorType> parse(ValidatingDocNode vNode,
            ValidationErrors validationErrors, Class<AuthenticatorType> authenticatorType, ConfigurationRepository.Context context)
            throws ConfigValidationException {
        TypedComponentRegistry typedComponentRegistry = context.modulesRegistry().getTypedComponentRegistry();

        String id = vNode.get("id").asString();
        String description = vNode.get("description").asString();
        boolean enabled = vNode.get("enabled").withDefault(true).asBoolean();
        int order = vNode.get("order").withDefault(0).asInt();
        AcceptanceRules acceptanceRules = new AcceptanceRules(vNode.get("accept").by(AcceptanceRules.Criteria::parse),
                vNode.get("skip").by(AcceptanceRules.Criteria::parse));
        UserMapping userMapping = vNode.get("user_mapping").by(UserMapping::parse);

        String type = vNode.get("type").required().asString();

        AuthenticatorType authenticator = null;
        AuthenticationBackend authenticationBackend = AuthenticationBackend.NOOP;
        ImmutableList<UserInformationBackend> additionalUserInformationBackends = ImmutableList.empty();

        if (type != null) {
            String authenticatorSubType;
            String backendType;

            int slash = type.indexOf('/');

            if (slash == -1) {
                authenticatorSubType = type;
                backendType = null;
            } else {
                authenticatorSubType = type.substring(0, slash);
                backendType = type.substring(slash + 1);
            }

            try {
                authenticator = typedComponentRegistry.create(authenticatorType, authenticatorSubType,
                        vNode.getDocumentNode().getAsNode(authenticatorSubType), context);
            } catch (ConfigValidationException e) {
                validationErrors.add(authenticatorSubType, e);
            } catch (NoSuchComponentException e) {
                validationErrors.add(new InvalidAttributeValue("type", type, e.getAvailableTypesAsInfoString())
                        .message("Unknown authentication frontend").cause(e));
            }

            if (backendType != null) {
                try {
                    authenticationBackend = typedComponentRegistry.create(AuthenticationBackend.class, backendType,
                            vNode.getDocumentNode().getAsNode(backendType), context);
                } catch (ConfigValidationException e) {
                    validationErrors.add(backendType, e);
                } catch (NoSuchComponentException e) {
                    validationErrors.add(new InvalidAttributeValue("type", type, e.getAvailableTypesAsInfoString())
                            .message("Unknown authentication backend").cause(e));
                }
            }
        }

        if (id == null) {
            id = Hashing.sha256().hashString(vNode.getDocumentNode().toJsonString(), StandardCharsets.UTF_8).toString().substring(0, 8);
        }

        if (vNode.hasNonNull("additional_user_information")) {
            try {
                additionalUserInformationBackends = parseAdditionalUserInformationBackends(
                        vNode.getDocumentNode().getAsListOfNodes("additional_user_information"), context);
            } catch (ConfigValidationException e) {
                validationErrors.add("additional_user_information", e);
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return new StandardAuthenticationDomain<AuthenticatorType>(vNode.getDocumentNode(), type, id, description, enabled, order, acceptanceRules,
                authenticator, authenticationBackend, additionalUserInformationBackends, userMapping);
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
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

        String type = vNode.get("type").required()
                .expected(context.modulesRegistry().getTypedComponentRegistry().getAvailableSubTypesAsShortString(UserInformationBackend.class))
                .asString();

        validationErrors.throwExceptionForPresentErrors();

        // type is not null

        try {
            return context.modulesRegistry().getTypedComponentRegistry().create(UserInformationBackend.class, type, docNode.getAsNode(type), context);
        } catch (ConfigValidationException e) {
            validationErrors.add(type, e);
        } catch (NoSuchComponentException e) {
            validationErrors.add(new InvalidAttributeValue("type", type, e.getAvailableTypes()).message("Unknown authentication backend").cause(e));
        }

        throw new ConfigValidationException(validationErrors);
    }

    public UserMapping getUserMapping() {
        return userMapping;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public UserMapper getUserMapper() {
        if (userMapping != null) {
            return userMapping;
        } else {
            return UserMapper.DIRECT;
        }
    }
    
    public CredentialsMapper getCredentialsMapper() {
        if (userMapping != null) {
            return userMapping;
        } else {
            return CredentialsMapper.DIRECT;
        }
    }
 
    public ImmutableList<UserInformationBackend> getAdditionalUserInformationBackends() {
        return additionalUserInformationBackends;
    }

    @Override
    public CompletableFuture<User> authenticate(AuthCredentials authCredentials) throws AuthenticatorUnavailableException, CredentialsException {
        try {
            authCredentials = authenticationBackend.authenticate(authCredentials).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AuthenticatorUnavailableException) {
                throw (AuthenticatorUnavailableException) e.getCause();
            } else if (e.getCause() instanceof CredentialsException) {
                throw (CredentialsException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }

        if (authCredentials == null) {
            return null;
        }
        
        authCredentials = authCredentials.with(AuthDomainInfo.forAuthenticatorType(authenticationFrontend.getType())
                .authBackendType(authenticationBackend != null ? authenticationBackend.getType() : null));

        if (additionalUserInformationBackends.size() != 0) {
            for (UserInformationBackend backend : additionalUserInformationBackends) {
                try {
                    authCredentials = authCredentials.with(backend.getUserInformation(authCredentials).get());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof AuthenticatorUnavailableException) {
                        throw (AuthenticatorUnavailableException) e.getCause();
                    } else if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        throw new RuntimeException(e.getCause());
                    }
                }
            }
        }

        User authenticatedUser;

        if (userMapping != null) {
            authenticatedUser = userMapping.map(authCredentials);
        } else {
            authenticatedUser = UserMapper.DIRECT.map(authCredentials);
        }

        return CompletableFuture.completedFuture(authenticatedUser);
    }

    @Override
    public CompletableFuture<User> impersonate(User originalUser, AuthCredentials authCredentials)
            throws AuthenticatorUnavailableException, CredentialsException {
        if (!(authenticationBackend instanceof UserInformationBackend)) {
            return CompletableFuture.completedFuture(null);
        }

        UserInformationBackend primaryBackend = (UserInformationBackend) authenticationBackend;

        try {
            authCredentials = primaryBackend.getUserInformation(authCredentials).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AuthenticatorUnavailableException) {
                throw (AuthenticatorUnavailableException) e.getCause();
            } else if (e.getCause() instanceof CredentialsException) {
                throw (CredentialsException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }

        if (authCredentials == null) {
            return null;
        }

        authCredentials = authCredentials.copy()
                .authDomainInfo(AuthDomainInfo.from(originalUser).addAuthBackend(authenticationBackend.getType() + "+impersonation")).build();

        if (additionalUserInformationBackends.size() != 0) {
            for (UserInformationBackend backend : additionalUserInformationBackends) {
                try {
                    authCredentials = authCredentials.with(backend.getUserInformation(authCredentials).get());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof AuthenticatorUnavailableException) {
                        throw (AuthenticatorUnavailableException) e.getCause();
                    } else if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        throw new RuntimeException(e.getCause());
                    }
                }
            }
        }

        return CompletableFuture.completedFuture(userMapping.map(authCredentials));
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public boolean cacheUser() {
        switch (authenticationBackend.userCachingPolicy()) {
        case ALWAYS:
            return true;
        case ONLY_IF_AUTHZ_SEPARATE:
            return !additionalUserInformationBackends.isEmpty();
        case NEVER:
        default:
            return false;
        }
    }

    public String getDescription() {
        return description;
    }
}