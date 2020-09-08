package com.floragunn.searchguard.privileges;

import java.util.Set;

import org.elasticsearch.common.transport.TransportAddress;

import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.user.User;

public interface SpecialPrivilegesEvaluationContext {
    User getUser();
    
    Set<String> getMappedRoles();

    SgRoles getSgRoles();
    
    TransportAddress getCaller();
    
    default boolean requiresPrivilegeEvaluationForLocalRequests() {
        return false;
    }
}
