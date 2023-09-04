package com.floragunn.searchguard;

import com.floragunn.searchguard.authc.RequestMetaData;
import org.elasticsearch.rest.RestRequest;

import java.util.Optional;

public class SignalsTenantParamResolver {

    private SignalsTenantParamResolver() {

    }

    public static String getRequestedTenant(RequestMetaData<RestRequest> request) {
        String result = request.getParam("tenant");
        if ("_main".equals(result)) {
            return null;
        }

        if (result != null) {
            return result;
        }
        RestRequest restReq = request.getRequest();
        final Optional<String> tenantFromUri = getSignalsTenantFrom(restReq);

        if (tenantFromUri.isPresent()) {
            String tenantParamValue = tenantFromUri.get();

            if ("_main".equals(tenantParamValue)) {
                return null;
            } else {
                return tenantParamValue;
            }
        } else {
            return restReq.header("sgtenant") != null ? restReq.header("sgtenant") : restReq.header("sg_tenant");
        }
    }

    public static Optional<String> getSignalsTenantFrom(RestRequest request) {
        if(request.uri().startsWith("/_signals/watch/")) {
            return Optional.of(request.uri().split("/")[3]);
        }

        return Optional.empty();
    }
}
