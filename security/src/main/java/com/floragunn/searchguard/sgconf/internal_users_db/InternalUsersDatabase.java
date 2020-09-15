package com.floragunn.searchguard.sgconf.internal_users_db;

import java.util.List;
import java.util.Map;

import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;

public class InternalUsersDatabase extends InternalUsersModel {
    private volatile InternalUsersModel delegate = new EmptyInternalUsersModel();

    public InternalUsersDatabase(DynamicConfigFactory dynamicConfigFactory) {
        dynamicConfigFactory.registerDCFListener(new DCFListener() {

            @Override
            public void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium) {
                delegate = ium;
            }
        });
    }

    public boolean exists(String user) {
        return delegate.exists(user);
    }

    public List<String> getBackenRoles(String user) {
        return delegate.getBackenRoles(user);
    }

    public Map<String, Object> getAttributes(String user) {
        return delegate.getAttributes(user);
    }

    public String getDescription(String user) {
        return delegate.getDescription(user);
    }

    public String getHash(String user) {
        return delegate.getHash(user);
    }

    public List<String> getSearchGuardRoles(String user) {
        return delegate.getSearchGuardRoles(user);
    }

}
