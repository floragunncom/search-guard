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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.user.User;

public class DlsFlsSearchOperationListener implements SearchOperationListener {

    private final AuthInfoService authInfoService;
    private final AuthorizationService authorizationService;
    private final AtomicReference<DlsFlsProcessedConfig> config;

    DlsFlsSearchOperationListener(AuthInfoService authInfoService, AuthorizationService authorizationService,
            AtomicReference<DlsFlsProcessedConfig> config) {
        this.authInfoService = authInfoService;
        this.authorizationService = authorizationService;
        this.config = config;
    }

    @Override
    public void onPreQueryPhase(SearchContext context) {
        try {
            DlsFlsProcessedConfig config = this.config.get();

            if (!config.isEnabled()) {
                return;
            }

            PrivilegesEvaluationContext privilegesEvaluationContext = getPrivilegesEvaluationContext();

            if (privilegesEvaluationContext == null) {
                return;
            }

            RoleBasedDocumentAuthorization documentAuthorization = config.getDocumentAuthorization();

            if (documentAuthorization == null) {
                throw new IllegalStateException("Authorization configuration is not yet initialized");
            }

            String index = context.indexShard().indexSettings().getIndex().getName();

            if (privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext() != null
                    && privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig() != null) {
                SgDynamicConfiguration<Role> roles = privilegesEvaluationContext.getSpecialPrivilegesEvaluationContext().getRolesConfig();
                documentAuthorization = new RoleBasedDocumentAuthorization(roles, ImmutableSet.of(index));
            }

            DlsRestriction dlsRestriction = documentAuthorization.getDlsRestriction(getPrivilegesEvaluationContext(), index);

            if (!dlsRestriction.isUnrestricted()) {
                BooleanQuery.Builder queryBuilder = dlsRestriction.toQueryBuilder(context.getQueryShardContext(), (q) -> new ConstantScoreQuery(q));

                queryBuilder.add(context.parsedQuery().query(), Occur.MUST);

                context.parsedQuery(new ParsedQuery(queryBuilder.build()));
                context.preProcess(true);            }
        } catch (Exception e) {
            throw new RuntimeException("Error evaluating dls for a search query: " + e, e);
        }
    }

    private PrivilegesEvaluationContext getPrivilegesEvaluationContext() {
        User user = authInfoService.peekCurrentUser();

        if (user == null) {
            return null;
        }

        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = authInfoService.getSpecialPrivilegesEvaluationContext();
        ImmutableSet<String> mappedRoles = this.authorizationService.getMappedRoles(user, specialPrivilegesEvaluationContext);

        return new PrivilegesEvaluationContext(user, mappedRoles, null, null, false, null, specialPrivilegesEvaluationContext);
    }
}
