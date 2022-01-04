package com.floragunn.searchguard.ssl.util.config;

public class GenericSSLConfigException extends Exception {

    private static final long serialVersionUID = 3774103067927533078L;

    public GenericSSLConfigException() {
        super();
    }

    public GenericSSLConfigException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GenericSSLConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public GenericSSLConfigException(String message) {
        super(message);
    }

    public GenericSSLConfigException(Throwable cause) {
        super(cause);
    }

}
