package com.floragunn.searchguard;

import com.floragunn.searchguard.authc.RequestMetaData;
import com.floragunn.searchguard.authc.session.BaseRequestMetaData;

import java.util.Optional;

public class SignalsTenantParamResolver {

    //this is quick and dirty hack to get the tenant from the uri path

    private SignalsTenantParamResolver() {

    }

    public static String getRequestedTenant(BaseRequestMetaData request) {
        //TODO ES8 RestHandler refactoring: check precedence
        //not sure if precedence is correct here
        final Optional<String> tenantFromUri = getSignalsTenantFrom(request);

        if (tenantFromUri.isPresent()) {
            String tenantParamValue = tenantFromUri.get();

            if ("_main".equals(tenantParamValue)) {
                return null;
            } else {
                return tenantParamValue;
            }
        } else {
            return request.getHeader("sgtenant") != null ? request.getHeader("sgtenant") : request.getHeader("sg_tenant");
        }
    }


    public static Optional<String> getSignalsTenantFrom(BaseRequestMetaData request) {
        if(request.getUri().startsWith("/_signals/watch/") || request.getUri().startsWith("/_signals/tenant/")) {
            return Optional.of(request.getUri().split("/")[3]);
        }

        return Optional.empty();
    }
}
