package com.floragunn.searchguard.authz.config;

import com.floragunn.searchguard.authz.TenantAccessMapper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public interface MultiTenancyConfigurationProvider {

    boolean isMultiTenancyEnabled();

    @Nullable
    String getKibanaServerUser();

    @Nullable
    String getKibanaIndex();

    TenantAccessMapper getTenantAccessMapper();

    boolean isGlobalTenantEnabled();

    boolean isPrivateTenantEnabled();

    List<String> getPreferredTenants();

    MultiTenancyConfigurationProvider DEFAULT = new MultiTenancyConfigurationProvider() {
        @Override
        public boolean isMultiTenancyEnabled() {
            return false;
        }

        @Nullable
        @Override
        public String getKibanaServerUser() {
            return null;
        }

        @Nullable
        @Override
        public String getKibanaIndex() {
            return null;
        }

        @Override
        public TenantAccessMapper getTenantAccessMapper() {
            return TenantAccessMapper.NO_OP;
        }

        @Override
        public boolean isGlobalTenantEnabled() {
            return true;
        }

        @Override
        public boolean isPrivateTenantEnabled() {
            return false;
        }

        @Override
        public List<String> getPreferredTenants() {
            return Collections.emptyList();
        }
    };
}
