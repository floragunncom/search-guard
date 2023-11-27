package com.floragunn.searchguard.authz;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.user.User;

import java.util.Map;
import java.util.Set;

public interface TenantAccessMapper {

    Map<String, Boolean> mapTenantsAccess(User user, Set<String> roles);

    TenantAccessMapper NO_OP = (user, roles) -> ImmutableMap.empty();

}
