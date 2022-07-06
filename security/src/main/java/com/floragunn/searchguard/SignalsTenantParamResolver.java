package com.floragunn.searchguard;

import org.elasticsearch.rest.RestRequest;

import java.util.Optional;

public class SignalsTenantParamResolver {

    //this is quick and dirty hack to get the tenant from the uri path

    private SignalsTenantParamResolver() {

    }

    public static String getRequestedTenant(RestRequest request) {
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
            return request.header("sgtenant") != null ? request.header("sgtenant") : request.header("sg_tenant");
        }
    }


    public static Optional<String> getSignalsTenantFrom(RestRequest request) {
        if(request.uri().startsWith("/_signals/watch/")) {
            return Optional.of(request.uri().split("/")[3]);
        }

        return Optional.empty();
    }
}
