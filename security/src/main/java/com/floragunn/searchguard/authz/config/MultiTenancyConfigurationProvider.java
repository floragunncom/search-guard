package com.floragunn.searchguard.authz.config;

import com.floragunn.searchguard.authz.TenantAccessMapper;

import javax.annotation.Nullable;

public interface MultiTenancyConfigurationProvider {

    boolean isMultiTenancyEnabled();

    @Nullable
    String getKibanaServerUser();

    @Nullable
    String getKibanaIndex();

    TenantAccessMapper getTenantAccessMapper();

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
    };
}
