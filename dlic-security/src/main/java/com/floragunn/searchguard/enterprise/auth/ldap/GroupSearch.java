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
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.support.Pattern;
import com.floragunn.searchguard.user.AttributeSource;
import com.floragunn.searchguard.user.StringInterpolationException;
import com.floragunn.searchsupport.util.ImmutableSet;
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
        this.searchFilter = vNode.get("filter").withDefault(SearchFilter.DEFAULT_GROUP_SEARCH).by(SearchFilter::parse);
        this.roleNameAttribute = vNode.get("role_name_attribute").withDefault("dn").asString();
        this.recursive = vNode.get("recursive.enabled").withDefault(false).asBoolean();
        this.recursiveSearchFilter = vNode.get("recursive.filter").withDefault(this.searchFilter).by(SearchFilter::parse);
        this.recursivePattern = vNode.get("recursive.enabled_for").by(Pattern::parse);
        this.maxRecusionDepth = vNode.get("recursive.max_depth").withDefault(30).asInt();
        this.searchCache = vNode.get("cache").withDefault(CacheConfig.DEFAULT).by(CacheConfig::new).build();

        validationErrors.throwExceptionForPresentErrors();
    }

    Set<Entry> search(LDAPConnection connection, String dn, AttributeSource attributeSource) throws LDAPException, StringInterpolationException {
        return new SearchState(connection, attributeSource).search(dn);
    }

    class SearchState {

        private final LDAPConnection connection;

        private Map<String, Entry> foundEntries = new HashMap<>();
        private AttributeSource attributeSource;

        SearchState(LDAPConnection connection, AttributeSource attributeSource) {
            this.connection = connection;
            this.attributeSource = attributeSource;
        }

        Set<Entry> search(String dn) throws LDAPException, StringInterpolationException {
            AttributeSource attributeSource;

            if (dn != null) {
                attributeSource = AttributeSource.joined(AttributeSource.of("dn", dn), this.attributeSource);
            } else {
                attributeSource = this.attributeSource;
            }

            Filter filter = searchFilter.toFilter(attributeSource);
            
            if (searchCache != null) {
                Set<Entry> cachedResult = searchCache.getIfPresent(filter);
                
                if (cachedResult != null) {
                    return cachedResult;
                }
            }

            SearchRequest searchRequest = new SearchRequest(searchBaseDn, searchScope, filter, SearchRequest.ALL_OPERATIONAL_ATTRIBUTES,
                    SearchRequest.ALL_USER_ATTRIBUTES);
            searchRequest.setDerefPolicy(DereferencePolicy.ALWAYS);

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
        }

        void searchNested(Set<String> dnSet, int currentDepth) throws LDAPException, StringInterpolationException {
            List<Filter> filters = new ArrayList<>(dnSet.size());

            for (String dn : dnSet) {
                AttributeSource attributeSource = AttributeSource.joined(AttributeSource.of("dn", dn), this.attributeSource);
                filters.add(recursiveSearchFilter.toFilter(attributeSource));
            }

            Filter filter = Filter.createORFilter(filters);

            SearchRequest searchRequest = new SearchRequest(searchBaseDn, searchScope, filter, SearchRequest.ALL_OPERATIONAL_ATTRIBUTES,
                    SearchRequest.ALL_USER_ATTRIBUTES);
            searchRequest.setDerefPolicy(DereferencePolicy.ALWAYS);

            Set<String> newEntryDns = new HashSet<>();

            for (SearchResultEntry entry : connection.search(searchRequest).getSearchEntries()) {
                if (!foundEntries.containsKey(entry.getDN())) {
                    foundEntries.put(entry.getDN(), entry);

                    if (recursivePattern == null || recursivePattern.matches(entry.getDN())) {
                        newEntryDns.add(entry.getDN());
                    }
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
