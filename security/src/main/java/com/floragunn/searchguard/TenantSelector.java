package com.floragunn.searchguard;

import com.floragunn.searchguard.user.User;

import java.util.Optional;

public interface TenantSelector {

    TenantSelector NO_OP = user -> Optional.empty();

    Optional<String> selectTenant(User user);

}
