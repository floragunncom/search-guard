package com.floragunn.signals.accounts;

public class NoSuchAccountException extends Exception {

    private static final long serialVersionUID = -5002349536254314758L;

    public NoSuchAccountException() {
        super();
    }

    public NoSuchAccountException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NoSuchAccountException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchAccountException(String message) {
        super(message);
    }

    public NoSuchAccountException(Throwable cause) {
        super(cause);
    }

}
