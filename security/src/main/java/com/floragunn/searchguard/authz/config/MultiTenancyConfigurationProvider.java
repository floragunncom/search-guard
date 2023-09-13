package com.floragunn.searchguard.authz.config;

public interface MultiTenancyConfigurationProvider {

    boolean isMultiTenancyEnabled();

    String getKibanaServerUser();

    String getKibanaIndex();

}
