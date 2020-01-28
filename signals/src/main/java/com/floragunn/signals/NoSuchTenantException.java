package com.floragunn.signals;

public class NoSuchTenantException extends Exception {

    private static final long serialVersionUID = 1148458641133860111L;

    private final String tenant;

    public NoSuchTenantException(String tenant) {
        super("No such tenant: " + tenant);
        this.tenant = tenant;
    }

    public String getTenant() {
        return tenant;
    }
}
