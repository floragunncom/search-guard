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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.floragunn.codova.config.net.CacheConfig;
import com.floragunn.codova.config.templates.AttributeSource;
import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.google.common.cache.Cache;
import com.unboundid.ldap.sdk.DereferencePolicy;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;

public class GroupSearch {

    private final String searchBaseDn;
    private final SearchScope searchScope;
    private final SearchFilter searchFilter;
    private final boolean recursive;
    private final Pattern recursivePattern;
    private final SearchFilter recursiveSearchFilter;
    private final int maxRecusionDepth;
    private final String roleNameAttribute;
    private final Cache<Filter, Set<Entry>> searchCache;

    GroupSearch(DocNode docNode, Parser.Context context) throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        this.searchBaseDn = vNode.get("base_dn").required().asString();
        this.searchScope = vNode.get("scope").withDefault(SearchScope.SUB).byString(LDAP::getSearchScope);
        this.searchFilter = vNode.get("filter").withDefault(SearchFilter.DEFAULT_GROUP_SEARCH).by(SearchFilter::parseForGroupSearch);
        this.roleNameAttribute = vNode.get("role_name_attribute").withDefault("dn").asString();
        this.recursive = vNode.get("recursive.enabled").withDefault(false).asBoolean();
        this.recursiveSearchFilter = vNode.get("recursive.filter").withDefault(this.searchFilter).by(SearchFilter::parseForGroupSearch);
        this.recursivePattern = vNode.get("recursive.enabled_for").by(Pattern::parse);
        this.maxRecusionDepth = vNode.get("recursive.max_depth").withDefault(30).asInt();
        this.searchCache = vNode.get("cache").withDefault(CacheConfig.DEFAULT).by(CacheConfig::new).build();

        validationErrors.throwExceptionForPresentErrors();
    }

    Set<Entry> search(LDAPConnection connection, String dn, AttributeSource attributeSource, Meter meter) throws AuthenticatorUnavailableException {
        return new SearchState(connection, attributeSource, meter).search(dn);
    }

    class SearchState {

        private final LDAPConnection connection;
        private final AttributeSource attributeSource;
        private final Meter meter;

        private Map<String, Entry> foundEntries = new HashMap<>();

        SearchState(LDAPConnection connection, AttributeSource attributeSource, Meter meter) {
            this.connection = connection;
            this.attributeSource = attributeSource;
            this.meter = meter;
        }

        Set<Entry> search(String dn) throws AuthenticatorUnavailableException {
            AttributeSource attributeSource;

            if (dn != null) {
                attributeSource = AttributeSource.joined(AttributeSource.of("dn", dn), this.attributeSource);
            } else {
                attributeSource = this.attributeSource;
            }

            Filter filter;

            try {
                filter = searchFilter.toFilter(attributeSource);
            } catch (LDAPException | ExpressionEvaluationException e) {
                throw new AuthenticatorUnavailableException("Could not create query for LDAP group search", e.getMessage(), e);
            }

            if (searchCache != null) {
                Set<Entry> cachedResult = searchCache.getIfPresent(filter);

                if (cachedResult != null) {
                    return cachedResult;
                }
            }

            SearchRequest searchRequest = new SearchRequest(searchBaseDn, searchScope, filter, SearchRequest.ALL_OPERATIONAL_ATTRIBUTES,
                    SearchRequest.ALL_USER_ATTRIBUTES);
            searchRequest.setDerefPolicy(DereferencePolicy.ALWAYS);

            try {

                Set<String> newEntryDns = new HashSet<>();

                for (SearchResultEntry entry : connection.search(searchRequest).getSearchEntries()) {
                    foundEntries.put(entry.getDN(), entry);

                    if (recursivePattern == null || recursivePattern.matches(entry.getDN())) {
                        newEntryDns.add(entry.getDN());
                    }
                }

                if (recursive && newEntryDns.size() != 0) {
                    searchNested(newEntryDns, 0);
                }

                Set<Entry> result = ImmutableSet.of(foundEntries.values());

                if (searchCache != null) {
                    searchCache.put(filter, result);
                }

                return result;
            } catch (LDAPException e) {
                throw new AuthenticatorUnavailableException("LDAP group search failed", LDAP.getBetterErrorMessage(e), e)
                        .details(LDAP.getDetailsFrom(e).with("ldap_group_base_dn", searchBaseDn).with("ldap_filter", filter.toString()));
            }
        }

        void searchNested(Set<String> dnSet, int currentDepth) throws AuthenticatorUnavailableException {
            Set<String> newEntryDns = new HashSet<>();

            try (Meter subMeter = this.meter.detail("recursive_search")) {
                List<Filter> filters = new ArrayList<>(dnSet.size());

                for (String dn : dnSet) {
                    AttributeSource attributeSource = AttributeSource.joined(AttributeSource.of("dn", dn), this.attributeSource);
                    try {
                        filters.add(recursiveSearchFilter.toFilter(attributeSource));
                    } catch (LDAPException | ExpressionEvaluationException e) {
                        throw new AuthenticatorUnavailableException("Could not create query for LDAP group search", e.getMessage(), e);
                    }
                }

                Filter filter = Filter.createORFilter(filters);

                try {
                    SearchRequest searchRequest = new SearchRequest(searchBaseDn, searchScope, filter, SearchRequest.ALL_OPERATIONAL_ATTRIBUTES,
                            SearchRequest.ALL_USER_ATTRIBUTES);
                    searchRequest.setDerefPolicy(DereferencePolicy.ALWAYS);

                    for (SearchResultEntry entry : connection.search(searchRequest).getSearchEntries()) {
                        if (!foundEntries.containsKey(entry.getDN())) {
                            foundEntries.put(entry.getDN(), entry);

                            if (recursivePattern == null || recursivePattern.matches(entry.getDN())) {
                                newEntryDns.add(entry.getDN());
                            }
                        }
                    }
                } catch (LDAPException e) {
                    throw new AuthenticatorUnavailableException("LDAP group search failed", LDAP.getBetterErrorMessage(e), e)
                            .details(LDAP.getDetailsFrom(e).with("ldap_group_base_dn", searchBaseDn).with("ldap_filter", filter.toString()));
                }
            }

            if (newEntryDns.size() != 0 && currentDepth < maxRecusionDepth) {
                searchNested(newEntryDns, currentDepth + 1);
            }
        }
    }

    public String getRoleNameAttribute() {
        return roleNameAttribute;
    }
}
