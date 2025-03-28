/*
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.enterprise.auth.ldap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.config.templates.AttributeSource;
import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchguard.TypedComponent;
import com.floragunn.searchguard.authc.AuthenticationBackend;
import com.floragunn.searchguard.authc.AuthenticationDebugLogger;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.UserInformationBackend;
import com.floragunn.searchguard.authc.base.AuthcResult;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.PrivilegedCode;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.DereferencePolicy;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.ldap.sdk.SimpleBindRequest;

public class LDAPAuthenticationBackend implements AuthenticationBackend, UserInformationBackend, AutoCloseable {

    public static final String TYPE = "ldap";

    protected static final Logger log = LogManager.getLogger(LDAPAuthenticationBackend.class);

    private final LDAPConnectionManager connectionManager;
    private final String userSearchBaseDn;
    private final SearchScope userSearchScope;
    private final SearchFilter userSearchFilter;
    private final String [] userSearchAttributes;
    private final GroupSearch groupSearch;

    private final boolean fakeLoginEnabled;
    private final String fakeLoginDn;
    private final byte[] fakeLoginPassword;
    
    private final DocNode configSource;

    private final ComponentState componentState = new ComponentState(0, "authentication_backend", TYPE, LDAPAuthenticationBackend.class).initialized()
            .requiresEnterpriseLicense();

    public LDAPAuthenticationBackend(Map<String, Object> config, ConfigurationRepository.Context context) throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors, context);

        this.connectionManager = vNode.get("idp").required().by(LDAPConnectionManager::new);
        this.userSearchBaseDn = vNode.get("user_search.base_dn").withDefault("").asString();
        this.userSearchScope = vNode.get("user_search.scope").withDefault(SearchScope.SUB).byString(LDAP::getSearchScope);
        this.userSearchFilter = vNode.get("user_search.filter").withDefault(SearchFilter.DEFAULT).by(SearchFilter::parseForUserSearch);
        this.userSearchAttributes = vNode.get("user_search.retrieve_attributes")
                .withListDefault(SearchRequest.ALL_OPERATIONAL_ATTRIBUTES, SearchRequest.ALL_USER_ATTRIBUTES).ofStrings().toArray(new String[0]);
        this.groupSearch = vNode.get("group_search").by(GroupSearch::new);
        this.fakeLoginEnabled = vNode.get("fake_login.enabled").withDefault(false).asBoolean();
        this.fakeLoginDn = vNode.get("fake_login.password").asString();
        this.fakeLoginPassword = vNode.get("fake_login.password").withDefault("fakeLoginPwd123").asString().getBytes();
        
        this.configSource = DocNode.wrap(config);

        validationErrors.throwExceptionForPresentErrors();
        
        this.componentState.addPart(this.connectionManager.getComponentState());
    }

    @Override
    public CompletableFuture<AuthCredentials> authenticate(AuthCredentials credentials, Meter meter)
            throws AuthenticatorUnavailableException, CredentialsException {
        SearchResultEntry entry = PrivilegedCode.execute(() -> search(credentials, meter), AuthenticatorUnavailableException.class);

        // fake a user that does not exist
        // makes guessing if a user exists or not harder when looking on the
        // authentication delay time
        if (entry == null) {
            if (fakeLoginEnabled) {
                String fakeLoginDn = this.fakeLoginDn != null ? this.fakeLoginDn : "CN=faketomakebindfail,DC=" + UUID.randomUUID().toString();
                try (Meter subMeter = meter.detail("invalid_login_delay")) {
                    checkPassword(fakeLoginDn, fakeLoginPassword);
                } catch (LDAPException e) {
                    // This is expected
                }
            }

            throw new CredentialsException(new AuthcResult.DebugInfo("ldap", false, "User could not be found by query",
                    ImmutableMap.of("user_name", credentials.getName())));
        }

        final String dn = entry.getDN();

        if (log.isTraceEnabled()) {
            log.trace("Try to authenticate dn {}", dn);
        }

        try (Meter subMeter = meter.detail("check_password")) {
            checkPassword(entry.getDN(), credentials.getPassword());
        } catch (LDAPException e) {
            throw new CredentialsException(new AuthcResult.DebugInfo("ldap", false, "User could not be authenticated by password",
                    OrderedImmutableMap.<String, Object>of("user_name", credentials.getName(), "dn", entry.getDN(), "ldap_error", e.getMessage())
                            .with(LDAP.getDetailsFrom(e))),
                    e);
        }

        AuthCredentials updatedCredentials = credentials.userMappingAttribute("ldap_user_entry", entryToMap(entry));

        AuthCredentials.Builder resultBuilder = updatedCredentials.copy();

        if (groupSearch != null) {
            Set<Entry> groupEntries = searchGroups(entry, updatedCredentials, meter);

            resultBuilder.userMappingAttribute("ldap_group_entries", ImmutableList.map(groupEntries, (e) -> entryToMap(e)));
            resultBuilder.backendRoles(extractRoles(groupEntries));
        }

        return CompletableFuture.completedFuture(resultBuilder.authDomainInfo(credentials.getAuthDomainInfo().authBackendType(getType())).build());
    }

    @Override
    public CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials userInformation, Meter meter, AuthenticationDebugLogger debug)
            throws AuthenticatorUnavailableException {
        SearchResultEntry entry = PrivilegedCode.execute(() -> search(userInformation, meter), AuthenticatorUnavailableException.class);

        if (entry == null) {
            debug.failure("additional_user_information: ldap", "User search failed", "user.name", userInformation.getName(), "user_search", this.configSource.get("user_search"));
            return CompletableFuture.completedFuture(null);
        }

        AuthCredentials updatedCredentials = userInformation.userMappingAttribute("ldap_user_entry", entryToMap(entry));

        debug.success("additional_user_information: ldap", "User search successful", ImmutableMap.of("user.name", userInformation.getName(), "ldap_entry",  entryToMap(entry)));        
        
        AuthCredentials.Builder resultBuilder = updatedCredentials.copy();

        if (groupSearch != null) {
            Set<Entry> groupEntries = searchGroups(entry, updatedCredentials, meter);

            debug.success("additional_user_information: ldap", "Group search successful", ImmutableMap.of("group_entries", groupEntries.stream().map((e) -> entryToMap(e)).collect(Collectors.toSet())));        
            
            resultBuilder.userMappingAttribute("ldap_group_entries", ImmutableList.map(groupEntries, (e) -> entryToMap(e)));
            resultBuilder.backendRoles(extractRoles(groupEntries));
        }

        return CompletableFuture.completedFuture(resultBuilder.build());
    }
    
    @Override
    public CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials userInformation, Meter meter)
            throws AuthenticatorUnavailableException {
        return this.getUserInformation(userInformation, meter, AuthenticationDebugLogger.DISABLED);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void close() {
        if (this.connectionManager != null) {
            try {
                this.connectionManager.close();
            } catch (IOException e) {
                log.warn("Error while closing " + connectionManager, e);
            }
        }
    }

    private SearchResultEntry search(AuthCredentials userName, Meter meter) throws AuthenticatorUnavailableException {

        try (Meter subMeter = meter.detail("user_search"); LDAPConnection connection = connectionManager.getConnection()) {
            Filter filter;
            try {
                filter = userSearchFilter.toFilter(AttributeSource.of("user.name", userName.getName()));
            } catch (LDAPException | ExpressionEvaluationException e) {
                throw new AuthenticatorUnavailableException("Could not create query for LDAP user search", e.getMessage(), e);
            }

            log.trace("Performing LDAP user search: {}, {} {}", filter, userSearchBaseDn, userSearchAttributes);

            SearchRequest searchRequest = new SearchRequest(userSearchBaseDn, userSearchScope, filter, this.userSearchAttributes);
            searchRequest.setDerefPolicy(DereferencePolicy.ALWAYS);

            try (Meter subSubMeter = subMeter.detail("ldap_search_operation")) {
                SearchResult searchResult = connection.search(searchRequest);

                log.trace("User search {} yielded {} results", filter, searchResult.getEntryCount());

                if (searchResult != null && searchResult.getEntryCount() > 0) {
                    return searchResult.getSearchEntries().get(0);
                } else {
                    return null;
                }

            } catch (LDAPException e) {
                throw new AuthenticatorUnavailableException("LDAP user search failed", LDAP.getBetterErrorMessage(e), e)
                        .details(LDAP.getDetailsFrom(e).with("ldap_base_dn", userSearchBaseDn).with("ldap_filter", filter.toString()));
            }
        }
    }

    private Set<Entry> searchGroups(SearchResultEntry entry, AuthCredentials credentials, Meter meter) throws AuthenticatorUnavailableException {
        try (Meter subMeter = meter.detail("group_search"); LDAPConnection connection = connectionManager.getConnection()) {
            return groupSearch.search(connection, entry.getDN(), AttributeSource.from(credentials.getAttributesForUserMapping()), subMeter);
        }
    }

    private void checkPassword(String dn, byte[] password) throws LDAPException {
        PrivilegedCode.execute(() -> connectionManager.getPool().bindAndRevertAuthentication(new SimpleBindRequest(dn, password)),
                LDAPException.class);
    }

    private ImmutableMap<String, Object> entryToMap(Entry entry) {
        ImmutableMap.Builder<String, Object> result = new ImmutableMap.Builder<>();

        result.with("dn", entry.getDN());

        for (Attribute attribute : entry.getAttributes()) {
            result.with(attribute.getName(), Arrays.asList(attribute.getValues()));
        }

        return result.build();
    }

    private ImmutableSet<String> extractRoles(Set<Entry> groupEntries) {
        String roleNameAttribute = groupSearch.getRoleNameAttribute();

        if (roleNameAttribute == null || groupEntries.isEmpty()) {
            return ImmutableSet.empty();
        }

        ImmutableSet.Builder<String> result = new ImmutableSet.Builder<>();

        for (Entry entry : groupEntries) {
            if ("dn".equalsIgnoreCase(roleNameAttribute)) {
                result.add(entry.getDN());
            } else {
                String[] values = entry.getAttributeValues(roleNameAttribute);

                if (values != null && values.length > 0) {
                    result.addAll(values);
                }
            }
        }

        return result.build();
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    public static Collection<TypedComponent.Info<?>> INFOS = ImmutableList.of(new TypedComponent.Info<AuthenticationBackend>() {

        @Override
        public Class<AuthenticationBackend> getType() {
            return AuthenticationBackend.class;
        }

        @Override
        public String getName() {
            return TYPE;
        }

        @Override
        public TypedComponent.Factory<AuthenticationBackend> getFactory() {
            return LDAPAuthenticationBackend::new;
        }
    }, new TypedComponent.Info<UserInformationBackend>() {

        @Override
        public Class<UserInformationBackend> getType() {
            return UserInformationBackend.class;
        }

        @Override
        public String getName() {
            return TYPE;
        }

        @Override
        public TypedComponent.Factory<UserInformationBackend> getFactory() {
            return LDAPAuthenticationBackend::new;
        }
    });
}
