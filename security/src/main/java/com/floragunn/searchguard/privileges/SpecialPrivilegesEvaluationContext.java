package com.floragunn.searchguard.privileges;

import org.elasticsearch.common.transport.TransportAddress;

import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.ActionAuthorization;
import com.floragunn.searchguard.authz.DocumentAuthorization;
import com.floragunn.searchguard.user.User;

public interface SpecialPrivilegesEvaluationContext {
    User getUser();
    
    ImmutableSet<String> getMappedRoles();

    ActionAuthorization getActionAuthorization();
    
    DocumentAuthorization getDocumentAuthorization();
    
    default TransportAddress getCaller() {
        return null;
    }
    
    default boolean requiresPrivilegeEvaluationForLocalRequests() {
        return false;
    }
    
    default boolean isSgConfigRestApiAllowed() {
        return false;
    }
}
