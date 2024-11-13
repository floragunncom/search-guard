package com.floragunn.searchguard.enterprise.femt.tenants;

import com.floragunn.searchsupport.action.StandardRequests.EmptyRequest;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.searchsupport.action.Action;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.injection.guice.Inject;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;

public class GetAvailableTenantsAction extends Action<EmptyRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(GetAvailableTenantsAction.class);

    public final static String NAME = "cluster:admin:searchguard:femt:user/available_tenants/get";
    public static final GetAvailableTenantsAction INSTANCE = new GetAvailableTenantsAction();

    public GetAvailableTenantsAction() {
        super(NAME, EmptyRequest::new, StandardResponse::new);
    }

    public static class GetAvailableTenantsHandler extends Handler<EmptyRequest, StandardResponse> {

        private final AvailableTenantService availableTenantService;

        @Inject
        public GetAvailableTenantsHandler(HandlerDependencies handlerDependencies, AvailableTenantService availableTenantService) {
            super(INSTANCE, handlerDependencies);
            this.availableTenantService = requireNonNull(availableTenantService, "Available tenant service is required");
        }

        @Override
        protected CompletableFuture<StandardResponse> doExecute(EmptyRequest request) {
            return supplyAsync(() -> {
                try {
                    return availableTenantService.findTenantAvailableForCurrentUser() //
                        .map(availableTenantData -> new StandardResponse(SC_OK).data(availableTenantData)) //
                        .orElseGet(() -> new StandardResponse(SC_NOT_FOUND, "User not found"));
                } catch (DefaultTenantNotFoundException ex) {
                    String message = "Cannot determine default tenant for current user";
                    log.error(message, ex);
                    return new StandardResponse(ex.status().getStatus(), message);
                } catch (Exception ex) {
                    String message = "Cannot retrieve information about tenants available for current user.";
                    log.error(message, ex);
                    return new StandardResponse(SC_INTERNAL_SERVER_ERROR, message);
                }
            });
        }
    }
}