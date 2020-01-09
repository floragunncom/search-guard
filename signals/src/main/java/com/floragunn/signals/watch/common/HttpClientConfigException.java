package com.floragunn.signals.watch.common;

public class HttpClientConfigException extends Exception {

    private static final long serialVersionUID = 7810177146375811444L;

    public HttpClientConfigException() {
        super();
    }

    public HttpClientConfigException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public HttpClientConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpClientConfigException(String message) {
        super(message);
    }

    public HttpClientConfigException(Throwable cause) {
        super(cause);
    }

}
