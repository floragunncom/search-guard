/*
 * Copyright 2022-2024 by floragunn GmbH - All rights reserved
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

import java.util.function.Supplier;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.AuthInfoService;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContext;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.meta.Meta;

public class DlsFlsBaseContext {
    private final AuthInfoService authInfoService;
    private final AuthorizationService authorizationService;
    private final ThreadContext threadContext;
    private final Supplier<Meta> indexMetaDataSupplier;

    DlsFlsBaseContext(AuthInfoService authInfoService, AuthorizationService authorizationService, ThreadContext threadContext,
            Supplier<Meta> indexMetaDataSupplier) {
        this.authInfoService = authInfoService;
        this.authorizationService = authorizationService;
        this.threadContext = threadContext;
        this.indexMetaDataSupplier = indexMetaDataSupplier;
    }

    public PrivilegesEvaluationContext getPrivilegesEvaluationContext() {
        User user = authInfoService.peekCurrentUser();

        if (user == null) {
            return null;
        }

        SpecialPrivilegesEvaluationContext specialPrivilegesEvaluationContext = authInfoService.getSpecialPrivilegesEvaluationContext();
        ImmutableSet<String> mappedRoles = this.authorizationService.getMappedRoles(user, specialPrivilegesEvaluationContext);

        return new PrivilegesEvaluationContext(user, this.authInfoService.isAdmin(user), mappedRoles, null, null, false, null,
                specialPrivilegesEvaluationContext);
    }

    public boolean isDlsDoneOnFilterLevel() {
        if (threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE) != null) {
            return true;
        } else {
            return false;
        }
    }

    String getDoneDlsFilterLevelQuery() {
        return threadContext.getHeader(ConfigConstants.SG_FILTER_LEVEL_DLS_DONE);
    }

    public Meta getIndexMetaData() {
        return this.indexMetaDataSupplier.get();
    }
    

    public boolean isInteralRequest() {
        return "true".equals(HeaderHelper.getSafeFromHeader(threadContext, ConfigConstants.SG_CONF_REQUEST_HEADER));
    }

}