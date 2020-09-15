package com.floragunn.searchguard.sgconf.internal_users_db;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.floragunn.searchguard.sgconf.InternalUsersModel;

public class EmptyInternalUsersModel extends InternalUsersModel {

    @Override
    public boolean exists(String user) {
        return false;
    }

    @Override
    public List<String> getBackenRoles(String user) {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> getAttributes(String user) {
        return Collections.emptyMap();
    }

    @Override
    public String getDescription(String user) {
        return null;
    }

    @Override
    public String getHash(String user) {
        return null;
    }

    @Override
    public List<String> getSearchGuardRoles(String user) {
        return Collections.emptyList();
    }

}
